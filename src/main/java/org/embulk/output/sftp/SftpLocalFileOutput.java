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
    final Logger logger = Exec.getLogger(getClass());
    private final String pathPrefix;
    private final String sequenceFormat;
    private final String fileNameExtension;
    private boolean renameFileAfterUpload;
    // flush will be triggered when local temp file reaches this threshold
    private final long localBufferSize;

    private final int taskIndex;
    final SftpUtils sftpUtils;
    int fileIndex = 0;
    private File tempFile;
    BufferedOutputStream localOutput = null;
    List<Map<String, String>> fileList = new ArrayList<>();

    final String temporaryFileSuffix = ".tmp";

    SftpLocalFileOutput(PluginTask task, int taskIndex)
    {
        this.pathPrefix = task.getPathPrefix();
        this.sequenceFormat = task.getSequenceFormat();
        this.fileNameExtension = task.getFileNameExtension();
        this.renameFileAfterUpload = task.getRenameFileAfterUpload();
        this.taskIndex = taskIndex;
        this.sftpUtils = new SftpUtils(task);
        this.localBufferSize = task.getLocalBufferSize();
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
            if (tempFile.length() + buffer.limit() > localBufferSize) {
                // if we have to split into multiple uploads (append mode)
                // have to use `.tmp` filename and switch on `renameFileAfterUpload`
                renameFileAfterUpload = true;
                // ignore returned value, as we don't need the report here
                flush();
                // reset output stream (overwrite local temp file)
                localOutput = new BufferedOutputStream(new FileOutputStream(tempFile));
            }
            localOutput.write(buffer.array(), buffer.offset(), buffer.limit());
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
        fileList.add(flush());
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

    private Map<String, String> flush()
    {
        closeCurrentFile();
        String fileName = getOutputFilePath();
        String temporaryFileName = fileName + temporaryFileSuffix;
        if (renameFileAfterUpload) {
            sftpUtils.uploadFile(tempFile, temporaryFileName, true);
        }
        else {
            sftpUtils.uploadFile(tempFile, fileName, false);
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
