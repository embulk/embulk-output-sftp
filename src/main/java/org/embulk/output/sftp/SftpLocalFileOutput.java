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

import org.apache.commons.vfs2.FileObject;
import org.embulk.config.TaskReport;
import org.embulk.output.sftp.utils.TimeoutCloser;
import org.embulk.spi.Buffer;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutput;
import org.embulk.spi.TempFileSpace;
import org.embulk.spi.TransactionalFileOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.embulk.output.sftp.SftpFileOutputPlugin.PluginTask;

/**
 * Created by takahiro.nakayama on 10/20/15.
 */
public class SftpLocalFileOutput
        implements FileOutput, TransactionalFileOutput
{
    // to make it clear that it is a constant
    static final String TMP_SUFFIX = ".tmp";

    private static final Logger logger = LoggerFactory.getLogger(SftpLocalFileOutput.class);
    private final String pathPrefix;
    private final String sequenceFormat;
    private final String fileNameExtension;
    private final TempFileSpace tempFileSpace;
    private boolean renameFileAfterUpload;

    private final int taskIndex;
    final SftpUtils sftpUtils;
    int fileIndex = 0;
    private File tempFile;
    private BufferedOutputStream localOutput = null;
    List<Map<String, String>> fileList = new ArrayList<>();
    String curFilename;
    String tempFilename;

    /* for file splitting purpose */
    private final long threshold; // local file size to flush (upload to server)
    boolean appending = false; // when local file exceeds threshold, go to append mode
    FileObject remoteFile;
    BufferedOutputStream remoteOutput; // to keep output stream open during append mode
    long bufLen = 0L; // local temp file size

    SftpLocalFileOutput(final PluginTask task, final int taskIndex, final TempFileSpace tempFileSpace)
    {
        this.pathPrefix = task.getPathPrefix();
        this.sequenceFormat = task.getSequenceFormat();
        this.fileNameExtension = task.getFileNameExtension();
        this.renameFileAfterUpload = task.getRenameFileAfterUpload();
        this.taskIndex = taskIndex;
        this.tempFileSpace = tempFileSpace;
        this.sftpUtils = new SftpUtils(task);
        this.threshold = task.getTempFileThreshold();
    }

    @Override
    public void nextFile()
    {
        closeCurrentFile();

        try {
            tempFile = this.tempFileSpace.createTempFile();
            localOutput = new BufferedOutputStream(new FileOutputStream(tempFile));
            appending = false;
            curFilename = getOutputFilePath();
            tempFilename = curFilename + TMP_SUFFIX;
        }
        catch (FileNotFoundException e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Override
    public void add(final Buffer buffer)
    {
        try {
            final int len = buffer.limit();
            if (bufLen + len > threshold) {
                localOutput.close();
                // into 'append' mode
                appending = true;
                flush();

                // reset output stream (overwrite local temp file)
                localOutput = new BufferedOutputStream(new FileOutputStream(tempFile));
                bufLen = 0L;
            }
            localOutput.write(buffer.array(), buffer.offset(), len);
            bufLen += len;
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        finally {
            buffer.release();
        }
    }

    @Override
    public void finish()
    {
        closeCurrentFile();
        try {
            flush();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        closeRemoteFile();
        // if input config is not `renameFileAfterUpload`
        // and file is being split, we have to rename it here
        // otherwise, when it exits, it won't rename
        if (!renameFileAfterUpload && appending) {
            sftpUtils.renameFile(tempFilename, curFilename);
        }
        fileList.add(fileReport());
        fileIndex++;
    }

    @Override
    public void close()
    {
        closeCurrentFile();
        // TODO
        sftpUtils.close();
    }

    @Override
    public void abort()
    {
        // delete incomplete files
        if (renameFileAfterUpload) {
            sftpUtils.deleteFile(tempFilename);
        }
        else {
            sftpUtils.deleteFile(curFilename);
        }
    }

    @Override
    public TaskReport commit()
    {
        TaskReport report = SftpFileOutputPlugin.CONFIG_MAPPER_FACTORY.newTaskReport();
        report.set("file_list", fileList);
        return report;
    }

    void closeCurrentFile()
    {
        try {
            if (localOutput != null) {
                localOutput.close();
                localOutput = null;
            }
        }
        catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    void closeRemoteFile()
    {
        if (remoteOutput != null) {
            new TimeoutCloser(remoteOutput).close();
            remoteOutput = null;
            remoteFile = null;
        }
    }

    String getOutputFilePath()
    {
        return pathPrefix + String.format(sequenceFormat, taskIndex, fileIndex) + fileNameExtension;
    }

    Map<String, String> fileReport()
    {
        final LinkedHashMap<String, String> report = new LinkedHashMap<>();
        report.put("temporary_filename", tempFilename);
        report.put("real_filename", curFilename);
        return Collections.unmodifiableMap(report);
    }

    private void flush() throws IOException
    {
        if (appending) {
            // open and keep stream open
            if (remoteOutput == null) {
                remoteFile = sftpUtils.resolve(tempFilename);
                remoteOutput = sftpUtils.openStream(remoteFile);
            }
            sftpUtils.appendFile(tempFile, remoteFile, remoteOutput);
        }
        else {
            sftpUtils.uploadFile(tempFile, renameFileAfterUpload ? tempFilename : curFilename);
        }
    }

    OutputStream getLocalOutput()
    {
        return localOutput;
    }

    OutputStream getRemoteOutput()
    {
        return remoteOutput;
    }
}
