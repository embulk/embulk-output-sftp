package org.embulk.output.sftp;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.embulk.config.TaskReport;
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
    // flush will be triggered when local temp file reaches this threshold
    private final long threshold;
    private boolean appending = false;
    private long bufLen = 0L;

    private final int taskIndex;
    final SftpUtils sftpUtils;
    int fileIndex = 0;
    private File tempFile;
    BufferedOutputStream localOutput = null;
    List<Map<String, String>> fileList = new ArrayList<>();

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
                // if we have to split into multiple uploads (append mode)
                // we have to use `.tmp` filename (ie. turn on `renameFileAfterUpload`)
                renameFileAfterUpload = true;
                // ignore returned value, as we don't need the report here
                flush();
                // toggle `appending`
                appending = true;
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
        // collect report of last flush
        try {
            fileList.add(flush());
        }
        catch (IOException e) {
            logger.error("Failed to (final) flush");
            throw Throwables.propagate(e);
        }
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
        String fileName = getOutputFilePath();
        String temporaryFileName = fileName + TMP_SUFFIX;
        if (renameFileAfterUpload) {
            sftpUtils.deleteFile(temporaryFileName);
        }
        else {
            sftpUtils.deleteFile(fileName);
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
        if (localOutput != null) {
            try {
                localOutput.close();
            }
            catch (IOException ex) {
                throw Throwables.propagate(ex);
            }
        }
    }

    String getOutputFilePath()
    {
        return pathPrefix + String.format(sequenceFormat, taskIndex, fileIndex) + fileNameExtension;
    }

    private Map<String, String> flush() throws IOException
    {
        closeCurrentFile();
        String fileName = getOutputFilePath();
        String temporaryFileName = fileName + TMP_SUFFIX;
        if (renameFileAfterUpload) {
            sftpUtils.uploadFile(tempFile, temporaryFileName, appending);
        }
        else {
            sftpUtils.uploadFile(tempFile, fileName, appending);
        }
        return fileReport(temporaryFileName, fileName);
    }

    static Map<String, String> fileReport(final String tempFile, final String realFile)
    {
        return ImmutableMap.of(
                "temporary_filename", tempFile,
                "real_filename", realFile
        );
    }
}
