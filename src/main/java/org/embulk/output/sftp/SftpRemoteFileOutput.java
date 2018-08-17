package org.embulk.output.sftp;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.Service;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.embulk.output.sftp.utils.TimedCallable;
import org.embulk.spi.Buffer;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.embulk.output.sftp.SftpFileOutputPlugin.PluginTask;

public class SftpRemoteFileOutput extends SftpLocalFileOutput
{
    private static final int TIMEOUT = 60; // 1min
    private Service watcher;

    SftpRemoteFileOutput(PluginTask task, int taskIndex)
    {
        super(task, taskIndex);
    }

    @Override
    public void add(final Buffer buffer)
    {
        try {
            final int len = buffer.limit();
            // time-out write
            new TimedCallable<Void>()
            {
                @Override
                public Void call() throws Exception
                {
                    localOutput.write(buffer.array(), buffer.offset(), len);
                    return null;
                }
            }.call(TIMEOUT, TimeUnit.SECONDS);
            bufLen += len;
        }
        catch (InterruptedException | ExecutionException | TimeoutException ex) {
            logger.error("Failed to write buffer", ex);
            stopWatcher();
            throw Throwables.propagate(ex);
        }
        finally {
            buffer.release();
        }
    }

    void closeCurrentFile()
    {
        if (localOutput != null) {
            new TimedCallable<Void>()
            {
                @Override
                public Void call() throws IOException
                {
                    localOutput.close();
                    return null;
                }
            }.callNonInterruptible(TIMEOUT, TimeUnit.SECONDS);
            localOutput = null;
        }
        stopWatcher();
    }

    void stopWatcher()
    {
        if (watcher != null) {
            watcher.stopAsync();
        }
    }

    @Override
    public void nextFile()
    {
        closeCurrentFile();

        try {
            String fileName = getOutputFilePath();
            // always write to remote .tmp first
            String temporaryFileName = fileName + TMP_SUFFIX;
            // resolve remote file & open output stream
            FileObject tempFile = sftpUtils.newSftpFile(sftpUtils.getSftpFileUri(temporaryFileName));
            // this is where it's different from |SftpLocalFileOutput|
            // localOutput is now an OutputStream of remote file
            localOutput = new BufferedOutputStream(tempFile.getContent().getOutputStream());
            watcher = newProgressWatcher().startAsync();
        }
        catch (FileSystemException e) {
            stopWatcher();
            logger.error(e.getMessage());
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void finish()
    {
        closeCurrentFile();
        String fileName = getOutputFilePath();
        String temporaryFileName = fileName + TMP_SUFFIX;
        sftpUtils.renameFile(temporaryFileName, fileName);
        fileList.add(fileReport(temporaryFileName, fileName));
        fileIndex++;
        stopWatcher();
    }

    private Service newProgressWatcher()
    {
        return new AbstractScheduledService()
        {
            private static final int PERIOD = 10; // seconds
            private long prevLen = 0L;

            @Override
            protected void runOneIteration()
            {
                logger.info("Upload progress: {} KB - {} KB/s", bufLen / 1024, (bufLen - prevLen) / 1024 / PERIOD);
                prevLen = bufLen;
            }

            @Override
            protected Scheduler scheduler()
            {
                return Scheduler.newFixedRateSchedule(PERIOD, PERIOD, TimeUnit.SECONDS);
            }
        };
    }
}
