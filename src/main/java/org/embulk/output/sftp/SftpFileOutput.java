package org.embulk.output.sftp;

import com.google.common.base.Throwables;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.embulk.output.sftp.SftpFileOutputPlugin.PluginTask;

/**
 * Created by takahiro.nakayama on 10/20/15.
 */
public class SftpFileOutput
    implements FileOutput, TransactionalFileOutput
{
    private final Logger logger = Exec.getLogger(SftpFileOutput.class);
    private final String pathPrefix;
    private final String sequenceFormat;
    private final String fileNameExtension;
    private final boolean renameFileAfterUpload;

    private final int taskIndex;
    private final SftpUtils sftpUtils;
    private int fileIndex = 0;
    private File tempFile;
    private BufferedOutputStream localOutput = null;
    private List<Map<String, String>> fileList = new ArrayList<>();

    private final String temporaryFileSuffix = ".tmp";

    SftpFileOutput(PluginTask task, int taskIndex)
    {
        this.pathPrefix = task.getPathPrefix();
        this.sequenceFormat = task.getSequenceFormat();
        this.fileNameExtension = task.getFileNameExtension();
        this.renameFileAfterUpload = task.getRenameFileAfterUpload();
        this.taskIndex = taskIndex;
        this.sftpUtils = new SftpUtils(task);
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
            Throwables.propagate(e);
        }
    }

    @Override
    public void add(final Buffer buffer)
    {
        try {
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
        closeCurrentFile();
        String fileName = getOutputFilePath();
        String temporaryFileName = fileName + temporaryFileSuffix;
        if (renameFileAfterUpload) {
            sftpUtils.uploadFile(tempFile, temporaryFileName);
        }
        else {
            sftpUtils.uploadFile(tempFile, fileName);
        }

        Map<String, String> executedFiles = new HashMap<>();
        executedFiles.put("temporary_filename", temporaryFileName);
        executedFiles.put("real_filename", fileName);
        fileList.add(executedFiles);
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

    private void closeCurrentFile()
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

    private String getOutputFilePath()
    {
        return pathPrefix + String.format(sequenceFormat, taskIndex, fileIndex) + fileNameExtension;
    }
}
