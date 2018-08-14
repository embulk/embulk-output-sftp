package org.embulk.output.sftp;

import com.google.common.base.Throwables;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;

import java.io.BufferedOutputStream;

import static org.embulk.output.sftp.SftpFileOutputPlugin.PluginTask;

public class SftpRemoteFileOutput extends SftpLocalFileOutput
{
    SftpRemoteFileOutput(PluginTask task, int taskIndex)
    {
        super(task, taskIndex);
    }

    @Override
    public void nextFile()
    {
        closeCurrentFile();

        try {
            String fileName = getOutputFilePath();
            // always write to remote .tmp first
            String temporaryFileName = fileName + temporaryFileSuffix;
            // resolve remote file & open output stream
            FileObject tempFile = sftpUtils.newSftpFile(sftpUtils.getSftpFileUri(temporaryFileName));
            // this is where it's different from {@code SftpLocalFileOutput}
            // localOutput is now an OutputStream of remote file
            localOutput = new BufferedOutputStream(tempFile.getContent().getOutputStream());
        }
        catch (FileSystemException e) {
            logger.error(e.getMessage());
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void finish()
    {
        // closing remote output stream is enough
        closeCurrentFile();
        String fileName = getOutputFilePath();
        String temporaryFileName = fileName + temporaryFileSuffix;

        fileList.add(fileReport(temporaryFileName, fileName));
        fileIndex++;
    }
}
