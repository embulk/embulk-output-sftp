package org.embulk.output.sftp;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.vfs2.FileObject;
import org.embulk.config.TaskReport;
import org.embulk.output.sftp.utils.TimeoutCloser;
import org.embulk.spi.Buffer;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutput;
import org.embulk.spi.TransactionalFileOutput;
import org.slf4j.Logger;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
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

    final Logger logger = Exec.getLogger(getClass());
    private final String pathPrefix;
    private final String sequenceFormat;
    private final String fileNameExtension;
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
    OutputStream remoteOutput; // to keep output stream open during append mode
    long bufLen = 0L; // local temp file size

    SftpLocalFileOutput(PluginTask task, int taskIndex)
    {
        this.pathPrefix = task.getPathPrefix();
        this.sequenceFormat = task.getSequenceFormat();
        this.fileNameExtension = task.getFileNameExtension();
        this.renameFileAfterUpload = task.getRenameFileAfterUpload();
        this.taskIndex = taskIndex;
        this.sftpUtils = new SftpUtils(task);
        this.threshold = task.getTempFileThreshold();
    }

    @Override
    public void nextFile()
    {
        closeCurrentFile();

        try {
            tempFile = Exec.getTempFileSpace().createTempFile();
            localOutput = new BufferedOutputStream(new FileOutputStream(tempFile));
            curFilename = getOutputFilePath();
            tempFilename = curFilename + TMP_SUFFIX;
        }
        catch (FileNotFoundException e) {
            logger.error(e.getMessage());
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void add(final Buffer buffer)
    {
        try {
            final int len = buffer.limit();
            if (bufLen + len > threshold) {
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
            throw Throwables.propagate(ex);
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
            throw Throwables.propagate(e);
        }
        closeRemoteFile();
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
        TaskReport report = Exec.newTaskReport();
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
            throw Throwables.propagate(ex);
        }
    }

    void closeRemoteFile()
    {
        if (remoteFile != null) {
            new TimeoutCloser(remoteFile).close();
            remoteFile = null;
            remoteOutput = null;
        }
        // if input config is not `renameFileAfterUpload`
        // and file is being split, we have to rename it here
        // otherwise, when it exits, it won't rename
        if (!renameFileAfterUpload && appending) {
            sftpUtils.renameFile(tempFilename, curFilename);
        }
    }

    String getOutputFilePath()
    {
        return pathPrefix + String.format(sequenceFormat, taskIndex, fileIndex) + fileNameExtension;
    }

    Map<String, String> fileReport()
    {
        return ImmutableMap.of(
                "temporary_filename", tempFilename,
                "real_filename", curFilename
        );
    }

    private void flush() throws IOException
    {
        if (appending) {
            // open and keep stream open
            if (remoteFile == null && remoteOutput == null) {
                remoteFile = sftpUtils.resolve(tempFilename);
                remoteOutput = sftpUtils.openStream(remoteFile);
            }
            sftpUtils.appendFile(tempFile, remoteFile, remoteOutput);
        }
        else {
            if (renameFileAfterUpload) {
                sftpUtils.uploadFile(tempFile, tempFilename);
            }
            else {
                sftpUtils.uploadFile(tempFile, curFilename);
            }
        }
    }
}
