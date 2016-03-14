package org.embulk.output.sftp;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.apache.commons.vfs2.provider.sftp.IdentityInfo;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;
import org.embulk.config.ConfigException;
import org.embulk.config.TaskReport;
import org.embulk.output.sftp.util.Retriable;
import org.embulk.spi.Buffer;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutput;
import org.embulk.spi.TransactionalFileOutput;
import org.embulk.spi.unit.LocalFile;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import static org.embulk.output.sftp.SftpFileOutputPlugin.PluginTask;
import static org.embulk.output.sftp.util.Retriable.*;

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
    private final int maxConnectionRetry;
    private final String pathPrefix;
    private final String sequenceFormat;
    private final String fileNameExtension;
    private final Retriable retriable;

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
            throw new ConfigException(e);
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
            SftpFileSystemConfigBuilder builder = SftpFileSystemConfigBuilder.getInstance();
            builder.setUserDirIsRoot(fsOptions, task.getUserDirIsRoot());
            builder.setTimeout(fsOptions, task.getSftpConnectionTimeout());
            builder.setStrictHostKeyChecking(fsOptions, "no");
            if (task.getSecretKeyFilePath().isPresent()) {
                IdentityInfo identityInfo = new IdentityInfo(
                    new File((task.getSecretKeyFilePath().transform(localFileToPathString()).get())),
                    task.getSecretKeyPassphrase().getBytes()
                );
                builder.setIdentityInfo(fsOptions, identityInfo);
                logger.info("set identity: {}", task.getSecretKeyFilePath().get());
            }

            if (task.getProxy().isPresent()) {
                ProxyTask proxy = task.getProxy().get();

                ProxyTask.ProxyType.setProxyType(builder, fsOptions, proxy.getType());

                if (proxy.getHost().isPresent()) {
                    builder.setProxyHost(fsOptions, proxy.getHost().get());
                    builder.setProxyPort(fsOptions, proxy.getPort());
                }

                if (proxy.getUser().isPresent()) {
                    builder.setProxyUser(fsOptions, proxy.getUser().get());
                }

                if (proxy.getPassword().isPresent()) {
                    builder.setProxyPassword(fsOptions, proxy.getPassword().get());
                }

                if (proxy.getCommand().isPresent()) {
                    builder.setProxyCommand(fsOptions, proxy.getCommand().get());
                }
            }
        }
        catch (FileSystemException e) {
            logger.error(e.getMessage());
            throw new ConfigException(e);
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
        this.maxConnectionRetry = task.getMaxConnectionRetry();
        this.pathPrefix = task.getPathPrefix();
        this.sequenceFormat = task.getSequenceFormat();
        this.fileNameExtension = task.getFileNameExtension();
        this.taskIndex = taskIndex;
        this.retriable = new Builder(task.getMaxConnectionRetry()).build();
    }

    @Override
    public void nextFile()
    {
        closeCurrentFile();

        try {
            currentFile = newSftpFile(getSftpFileUri(getOutputFilePath()));
            currentFileOutputStream = createSftpFileOutputStream(currentFile);
            logger.info("new sftp file: {}", currentFile.getPublicURIString());
        }
        catch (MaxRetriesExceededException e) {
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
        finally {
            buffer.release();
        }
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
        return Exec.newTaskReport();
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
            throw new ConfigException(e);
        }
    }

    private String getOutputFilePath()
    {
        return pathPrefix + String.format(sequenceFormat, taskIndex, fileIndex) + fileNameExtension;
    }

    private FileObject newSftpFile(final URI sftpUri)
            throws MaxRetriesExceededException
    {
        return retriable.callWithExponentialBackoff(new Retriable.Callable<FileObject>() {
            @Override
            public FileObject call()
                    throws RetriableException
            {
                try {
                    FileObject file = manager.resolveFile(sftpUri.toString(), fsOptions);
                    if (file.getParent().exists()) {
                        logger.info("parent directory {} exists there", file.getParent());
                        return file;
                    }
                    else {
                        logger.info("trying to create parent directory {}", file.getParent());
                        file.getParent().createFolder();
                        throw new RetriableException("parent directory does not exist,");
                    }
                }
                catch (FileSystemException e) {
                    throw new RetriableException(e);
                }
            }
        });
    }

    private OutputStream createSftpFileOutputStream(final FileObject fileObject)
            throws MaxRetriesExceededException
    {
        return retriable.callWithExponentialBackoff(new Retriable.Callable<OutputStream>() {
            @Override
            public OutputStream call()
                    throws RetriableException
            {
                try {
                    return fileObject.getContent().getOutputStream();
                }
                catch (FileSystemException e) {
                    throw new RetriableException(e);
                }
            }
        });
    }

    private Function<LocalFile, String> localFileToPathString()
    {
        return new Function<LocalFile, String>()
        {
            public String apply(LocalFile file)
            {
                return file.getPath().toString();
            }
        };
    }
}
