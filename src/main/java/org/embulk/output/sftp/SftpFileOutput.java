package org.embulk.output.sftp;

import com.google.common.base.Throwables;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.apache.commons.vfs2.provider.sftp.IdentityInfo;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;
import org.embulk.config.TaskReport;
import org.embulk.spi.Buffer;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutput;
import org.embulk.spi.TransactionalFileOutput;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import static org.embulk.output.sftp.SftpFileOutputPlugin.PluginTask;

/**
 * Created by takahiro.nakayama on 10/20/15.
 */
public class SftpFileOutput
    implements FileOutput, TransactionalFileOutput
{
    private final Logger logger = Exec.getLogger(SftpFileOutput.class);
    private final StandardFileSystemManager manager;
    private final FileSystemOptions fsOptions;
    private final String userInfo;
    private final String host;
    private final int port;
    private final String pathPrefix;
    private final String sequenceFormat;
    private final String fileNameExtension;

    private final int taskIndex;
    private int fileIndex = 0;
    private FileObject currentFile;
    private OutputStream currentFileOutputStream;

    private StandardFileSystemManager initializeStandardFileSystemManager()
    {
        if (!logger.isDebugEnabled()) {
            // TODO: change logging format: org.apache.commons.logging.Log
            System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog");
        }
        StandardFileSystemManager manager = new StandardFileSystemManager();
        try {
            manager.init();
        }
        catch (FileSystemException e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }

        return manager;
    }

    private String initializeUserInfo(PluginTask task)
    {
        String userInfo = task.getUser();
        if (task.getPassword().isPresent()) {
            userInfo += ":" + task.getPassword().get();
        }
        return userInfo;
    }

    private FileSystemOptions initializeFsOptions(PluginTask task)
    {
        FileSystemOptions fsOptions = new FileSystemOptions();

        try {
            SftpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(fsOptions, task.getUserDirIsRoot());
            SftpFileSystemConfigBuilder.getInstance().setTimeout(fsOptions, task.getSftpConnectionTimeout());
            SftpFileSystemConfigBuilder.getInstance().setStrictHostKeyChecking(fsOptions, "no");
            if (task.getSecretKeyFilePath().isPresent()) {
                IdentityInfo identityInfo = new IdentityInfo(new File((task.getSecretKeyFilePath().get())), task.getSecretKeyPassphrase().getBytes());
                SftpFileSystemConfigBuilder.getInstance().setIdentityInfo(fsOptions, identityInfo);
            }
        }
        catch (FileSystemException e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }

        return fsOptions;
    }

    SftpFileOutput(PluginTask task, int taskIndex)
    {
        this.manager = initializeStandardFileSystemManager();
        this.userInfo = initializeUserInfo(task);
        this.fsOptions = initializeFsOptions(task);
        this.host = task.getHost();
        this.port = task.getPort();
        this.pathPrefix = task.getPathPrefix();
        this.sequenceFormat = task.getSequenceFormat();
        this.fileNameExtension = task.getFileNameExtension();
        this.taskIndex = taskIndex;
    }

    @Override
    public void nextFile()
    {
        closeCurrentFile();

        try {
            currentFile = newSftpFile(getSftpFileUri(getOutputFilePath()));
            currentFileOutputStream = currentFile.getContent().getOutputStream();
            logger.info("new sftp file: {}", currentFile.getPublicURIString());
        }
        catch (FileSystemException e) {
            logger.error(e.getMessage());
            Throwables.propagate(e);
        }
    }

    @Override
    public void add(Buffer buffer)
    {
        if (currentFile == null) {
            throw new IllegalStateException("nextFile() must be called before poll()");
        }

        try {
            currentFileOutputStream.write(buffer.array(), buffer.offset(), buffer.limit());
        }
        catch (IOException e) {
            logger.error(e.getMessage());
            Throwables.propagate(e);
        }
        buffer.release();
    }

    @Override
    public void finish()
    {
        closeCurrentFile();
    }

    @Override
    public void close()
    {
        closeCurrentFile();
        manager.close();
    }

    @Override
    public void abort()
    {
    }

    @Override
    public TaskReport commit()
    {
        return null;
    }


    private void closeCurrentFile()
    {
        if (currentFile == null) {
            return;
        }

        try {
            currentFileOutputStream.close();
            currentFile.getContent().close();
            currentFile.close();
        }
        catch (IOException e) {
            logger.error(e.getMessage());
            Throwables.propagate(e);
        }
        finally {
            fileIndex++;
            currentFile = null;
            currentFileOutputStream = null;
        }
    }

    private URI getSftpFileUri(String remoteFilePath)
    {
        try {
            return new URI("sftp", userInfo, host, port, remoteFilePath, null, null);
        }
        catch (URISyntaxException e) {
            logger.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private String getOutputFilePath()
    {
        return pathPrefix + String.format(sequenceFormat, taskIndex, fileIndex) + fileNameExtension;
    }

    private FileObject newSftpFile(URI sftpUri)
            throws FileSystemException
    {
        return manager.resolveFile(sftpUri.toString(), fsOptions);
    }
}
