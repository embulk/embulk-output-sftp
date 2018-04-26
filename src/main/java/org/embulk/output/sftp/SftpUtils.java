package org.embulk.output.sftp;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.provider.sftp.IdentityInfo;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;
import org.embulk.config.ConfigException;
import org.embulk.spi.Exec;
import org.embulk.spi.unit.LocalFile;
import org.embulk.spi.util.RetryExecutor.RetryGiveupException;
import org.embulk.spi.util.RetryExecutor.Retryable;
import org.slf4j.Logger;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import static org.embulk.output.sftp.SftpFileOutputPlugin.PluginTask;
import static org.embulk.spi.util.RetryExecutor.retryExecutor;

/**
 * Created by takahiro.nakayama on 10/20/15.
 */
public class SftpUtils
{
    private final Logger logger = Exec.getLogger(SftpUtils.class);
    private final DefaultFileSystemManager manager;
    private final FileSystemOptions fsOptions;
    private final String userInfo;
    private final String user;
    private final String host;
    private final int port;
    private final int maxConnectionRetry;

    private DefaultFileSystemManager initializeStandardFileSystemManager()
    {
        if (!logger.isDebugEnabled()) {
            // TODO: change logging format: org.apache.commons.logging.Log
            System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog");
        }
        /*
         * We can use StandardFileSystemManager instead of DefaultFileSystemManager
         * when Apache Commons VFS2 removes permission check logic when remote file renaming.
         * https://github.com/embulk/embulk-output-sftp/issues/40
         * https://github.com/embulk/embulk-output-sftp/pull/44
         * https://issues.apache.org/jira/browse/VFS-590
        */
        DefaultFileSystemManager manager = new DefaultFileSystemManager();
        try {
            manager.addProvider("sftp", new org.embulk.output.sftp.provider.sftp.SftpFileProvider());
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
            builder.setTimeout(fsOptions, task.getSftpConnectionTimeout() * 1000);
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

    SftpUtils(PluginTask task)
    {
        this.manager = initializeStandardFileSystemManager();
        this.userInfo = initializeUserInfo(task);
        this.user = task.getUser();
        this.fsOptions = initializeFsOptions(task);
        this.host = task.getHost();
        this.port = task.getPort();
        this.maxConnectionRetry = task.getMaxConnectionRetry();
    }

    public void close()
    {
        manager.close();
    }

    public Void uploadFile(final File localTempFile, final String remotePath)
    {
        try {
            return retryExecutor()
                    .withRetryLimit(maxConnectionRetry)
                    .withInitialRetryWait(500)
                    .withMaxRetryWait(30 * 1000)
                    .runInterruptible(new Retryable<Void>() {
                        @Override
                        public Void call() throws IOException
                        {
                            long size = localTempFile.length();
                            int step = 10; // 10% each step
                            long bytesPerStep = size / step;
                            long startTime = System.nanoTime();

                            try (FileObject remoteFile = newSftpFile(getSftpFileUri(remotePath));
                                    InputStream inputStream = new FileInputStream(localTempFile);
                                    BufferedOutputStream outputStream = new BufferedOutputStream(remoteFile.getContent().getOutputStream());
                            ) {
                                logger.info("Uploading to remote sftp file ({} KB): {}", size / 1024, remoteFile.getPublicURIString());
                                byte[] buffer = new byte[32 * 1024 * 1024]; // 32MB buffer size
                                int len = inputStream.read(buffer);
                                long total = 0;
                                int progress = 0;
                                while (len != -1) {
                                    outputStream.write(buffer, 0, len);
                                    len = inputStream.read(buffer);
                                    total += len;
                                    if (total / bytesPerStep > progress) {
                                        progress = (int) (total / bytesPerStep);
                                        long transferRate = (long) (total / ((System.nanoTime() - startTime) / 1e9));
                                        logger.info("Upload progress: {}% - {} KB - {} KB/s",
                                                progress * step, total / 1024, transferRate / 1024);
                                    }
                                }
                                logger.info("Upload completed.");
                            }
                            return null;
                        }

                        @Override
                        public boolean isRetryableException(Exception exception)
                        {
                            if (exception instanceof ConfigException) {
                                return false;
                            }
                            return true;
                        }

                        @Override
                        public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait) throws RetryGiveupException
                        {
                            String message = String.format("SFTP output failed. Retrying %d/%d after %d seconds. Message: %s",
                                    retryCount, retryLimit, retryWait / 1000, exception.getMessage());
                            if (retryCount % 3 == 0) {
                                logger.warn(message, exception);
                            }
                            else {
                                logger.warn(message);
                            }
                        }

                        @Override
                        public void onGiveup(Exception firstException, Exception lastException) throws RetryGiveupException
                        {
                        }
                    });
        }
        catch (RetryGiveupException ex) {
            throw Throwables.propagate(ex.getCause());
        }
        catch (InterruptedException ex) {
            throw Throwables.propagate(ex);
        }
    }

    public Void renameFile(final String before, final String after)
    {
        try {
            return retryExecutor()
                    .withRetryLimit(maxConnectionRetry)
                    .withInitialRetryWait(500)
                    .withMaxRetryWait(30 * 1000)
                    .runInterruptible(new Retryable<Void>() {
                        @Override
                        public Void call() throws IOException
                        {
                            FileObject previousFile = manager.resolveFile(getSftpFileUri(before).toString(), fsOptions);
                            FileObject afterFile = manager.resolveFile(getSftpFileUri(after).toString(), fsOptions);
                            previousFile.moveTo(afterFile);
                            logger.info("renamed remote file: {} to {}", previousFile.getPublicURIString(), afterFile.getPublicURIString());

                            return null;
                        }

                        @Override
                        public boolean isRetryableException(Exception exception)
                        {
                            return true;
                        }

                        @Override
                        public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait) throws RetryGiveupException
                        {
                            String message = String.format("SFTP rename remote file failed. Retrying %d/%d after %d seconds. Message: %s",
                                    retryCount, retryLimit, retryWait / 1000, exception.getMessage());
                            if (retryCount % 3 == 0) {
                                logger.warn(message, exception);
                            }
                            else {
                                logger.warn(message);
                            }
                        }

                        @Override
                        public void onGiveup(Exception firstException, Exception lastException) throws RetryGiveupException
                        {
                        }
                    });
        }
        catch (RetryGiveupException ex) {
            throw Throwables.propagate(ex.getCause());
        }
        catch (InterruptedException ex) {
            throw Throwables.propagate(ex);
        }
    }

    public void validateHost(PluginTask task)
    {
        if (task.getHost().contains("%s")) {
            throw new ConfigException("'host' can't contain spaces");
        }
        getSftpFileUri("/");

        if (task.getProxy().isPresent() && task.getProxy().get().getHost().isPresent()) {
            if (task.getProxy().get().getHost().get().contains("%s")) {
                throw new ConfigException("'proxy.host' can't contains spaces");
            }
        }
    }

    private URI getSftpFileUri(String remoteFilePath)
    {
        try {
            return new URI("sftp", userInfo, host, port, remoteFilePath, null, null);
        }
        catch (URISyntaxException e) {
            String message = String.format("URISyntaxException was thrown: Illegal character in sftp://%s:******@%s:%s%s", user, host, port, remoteFilePath);
            throw new ConfigException(message);
        }
    }

    private FileObject newSftpFile(final URI sftpUri) throws FileSystemException
    {
        FileObject file = manager.resolveFile(sftpUri.toString(), fsOptions);
        if (file.exists()) {
            file.delete();
        }
        if (file.getParent().exists()) {
            logger.info("parent directory {} exists there", file.getParent().getPublicURIString());
        }
        else {
            logger.info("trying to create parent directory {}", file.getParent().getPublicURIString());
            file.getParent().createFolder();
        }
        return file;
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
