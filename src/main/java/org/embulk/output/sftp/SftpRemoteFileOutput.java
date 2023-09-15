/*
 * Copyright 2015 Civitaspo, and the Embulk project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.embulk.output.sftp;

import org.apache.commons.vfs2.FileSystemException;
import org.embulk.output.sftp.utils.TimedCallable;
import org.embulk.spi.Buffer;
import org.embulk.spi.TempFileSpace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.embulk.output.sftp.SftpFileOutputPlugin.PluginTask;

public class SftpRemoteFileOutput extends SftpLocalFileOutput
{
    private static final Logger logger = LoggerFactory.getLogger(SftpRemoteFileOutput.class);
    private static final int TIMEOUT = 60; // 1min
    private ScheduledExecutorService watcher;

    SftpRemoteFileOutput(final PluginTask task, final int taskIndex, final TempFileSpace tempFileSpace)
    {
        super(task, taskIndex, tempFileSpace);
        appending = true;
        this.watcher = null;
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
            throw new RuntimeException(ex);
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
            try {
                this.watcher.shutdown();
                if (!this.watcher.awaitTermination(100, TimeUnit.MILLISECONDS)) {
                    logger.info("The progress watcher thread did not terminate properly.");
                    this.watcher.shutdownNow();
                }
            }
            catch (final InterruptedException ex) {
                logger.info("The progress watcher thread termination was interrupted.", ex);
                this.watcher.shutdownNow();
            }
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
            if (remoteFile == null) {
                logger.warn("Failed to create new SFTP file for {}. Skipping further processing.", tempFilename);
                return;
            }
            // this is where it's different from |SftpLocalFileOutput|
            remoteOutput = new BufferedOutputStream(remoteFile.getContent().getOutputStream());
            // watcher = newProgressWatcher().startAsync();
            this.watcher = Executors.newSingleThreadScheduledExecutor();
            this.watcher.scheduleAtFixedRate(newProgressWatcher(), WATCHER_PERIOD, WATCHER_PERIOD, TimeUnit.SECONDS);
        }
        catch (FileSystemException e) {
            stopWatcher();
            throw new RuntimeException(e);
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

    private Runnable newProgressWatcher()
    {
        return new Runnable()
        {
            private long prevLen = 0L;

            @Override
            public void run()
            {
                logger.info("Upload progress: {} KB - {} KB/s", bufLen / 1024, (bufLen - prevLen) / 1024 / WATCHER_PERIOD);
                prevLen = bufLen;
            }
        };
    }

    private static final int WATCHER_PERIOD = 10; // seconds
}
