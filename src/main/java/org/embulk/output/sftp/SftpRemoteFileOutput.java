package org.embulk.output.sftp;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.Service;
import org.apache.commons.vfs2.FileSystemException;
import org.embulk.output.sftp.utils.TimedCallable;
import org.embulk.spi.Buffer;

import java.io.BufferedOutputStream;
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
        appending = true;
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
                    remoteOutput.write(buffer.array(), buffer.offset(), len);
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

    @Override
    void closeCurrentFile()
    {
        super.closeCurrentFile();
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
            curFilename = getOutputFilePath();
            tempFilename = curFilename + TMP_SUFFIX;
            // resolve remote file & open output stream
            remoteFile = sftpUtils.newSftpFile(sftpUtils.getSftpFileUri(tempFilename));
            // this is where it's different from |SftpLocalFileOutput|
            remoteOutput = new BufferedOutputStream(remoteFile.getContent().getOutputStream());
            watcher = newProgressWatcher().startAsync();
        }
        catch (FileSystemException e) {
            stopWatcher();
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void finish()
    {
        closeCurrentFile();
        closeRemoteFile();
        fileList.add(fileReport());
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
