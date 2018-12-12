package org.embulk.output.sftp;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.provider.sftp.IdentityInfo;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;
import org.embulk.config.ConfigException;
import org.embulk.output.sftp.utils.DefaultRetry;
import org.embulk.output.sftp.utils.TimedCallable;
import org.embulk.output.sftp.utils.TimeoutCloser;
import org.embulk.spi.Exec;
import org.embulk.spi.unit.LocalFile;
import org.embulk.spi.util.RetryExecutor.RetryGiveupException;
import org.embulk.spi.util.RetryExecutor.Retryable;
import org.slf4j.Logger;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.regex.Pattern;

import static org.embulk.output.sftp.SftpFileOutputPlugin.PluginTask;
import static org.embulk.spi.util.RetryExecutor.retryExecutor;

/**
 * Created by takahiro.nakayama on 10/20/15.
 */
public class SftpUtils
{
    private final Logger logger = Exec.getLogger(SftpUtils.class);
    private DefaultFileSystemManager manager;
    private final FileSystemOptions fsOptions;
    private final String userInfo;
    private final String user;
    private final String host;
    private final int port;
    private final int maxConnectionRetry;
    @VisibleForTesting
    int writeTimeout = 300; // 5 minutes

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
                        new File((task.getSecretKeyFilePath().map(localFileToPathString()).get())),
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

    void uploadFile(final File localTempFile, final String remotePath)
    {
        withRetry(new DefaultRetry<Void>(String.format("SFTP upload file '%s'", remotePath))
        {
            @Override
            public Void call() throws Exception
            {
                final FileObject remoteFile = newSftpFile(getSftpFileUri(remotePath));
                final BufferedOutputStream outputStream = openStream(remoteFile);
                // When channel is broken, closing resource may hang, hence the time-out wrapper
                try (final TimeoutCloser ignored = new TimeoutCloser(outputStream)) {
                    appendFile(localTempFile, remoteFile, outputStream);
                    return null;
                }
                finally {
                    // closing sequentially
                    new TimeoutCloser(remoteFile).close();
                }
            }

            @Override
            public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait)
            {
                super.onRetry(exception, retryCount, retryLimit, retryWait);
                // re-connect
                manager.close();
                manager = initializeStandardFileSystemManager();
            }
        });
    }

    /**
     * This method won't close outputStream, outputStream is intended to keep open for next write
     *
     * @param localTempFile
     * @param remoteFile
     * @param outputStream
     * @throws IOException
     */
    void appendFile(final File localTempFile, final FileObject remoteFile, final BufferedOutputStream outputStream) throws IOException
    {
        long size = localTempFile.length();
        int step = 10; // 10% each step
        long bytesPerStep = Math.max(size / step, 1); // to prevent / 0 if file size < 10 bytes
        long startTime = System.nanoTime();

        // start uploading
        try (InputStream inputStream = new FileInputStream(localTempFile)) {
            logger.info("Uploading to remote sftp file ({} KB): {}", size / 1024, remoteFile.getPublicURIString());
            final byte[] buffer = new byte[32 * 1024 * 1024]; // 32MB buffer size
            int len = inputStream.read(buffer);
            long total = 0;
            int progress = 0;
            while (len != -1) {
                timedWrite(outputStream, buffer, len);
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
    }

    public Void renameFile(final String before, final String after)
    {
        return withRetry(new DefaultRetry<Void>("SFTP rename remote file")
        {
            @Override
            public Void call() throws IOException
            {
                FileObject previousFile = resolve(before);
                FileObject afterFile = resolve(after);
                previousFile.moveTo(afterFile);
                logger.info("renamed remote file: {} to {}", previousFile.getPublicURIString(), afterFile.getPublicURIString());

                return null;
            }
        });
    }

    public void deleteFile(final String remotePath)
    {
        try {
            FileObject file = manager.resolveFile(getSftpFileUri(remotePath).toString(), fsOptions);
            if (file.exists()) {
                file.delete();
            }
        }
        catch (FileSystemException e) {
            logger.warn("Failed to delete remote file '{}': {}", remotePath, e.getMessage());
        }
    }

    public void validateHost(PluginTask task)
    {
        Pattern pattern = Pattern.compile("\\s");
        if (pattern.matcher(task.getHost()).find()) {
            throw new ConfigException("'host' can't contain spaces");
        }
        getSftpFileUri("/");

        if (task.getProxy().isPresent() && task.getProxy().get().getHost().isPresent()) {
            if (pattern.matcher(task.getProxy().get().getHost().get()).find()) {
                throw new ConfigException("'proxy.host' can't contains spaces");
            }
        }
    }

    FileObject resolve(final String remoteFilePath) throws FileSystemException
    {
        return manager.resolveFile(getSftpFileUri(remoteFilePath).toString(), fsOptions);
    }

    BufferedOutputStream openStream(final FileObject remoteFile) throws FileSystemException
    {
        return new BufferedOutputStream(remoteFile.getContent().getOutputStream());
    }

    URI getSftpFileUri(String remoteFilePath)
    {
        try {
            return new URI("sftp", userInfo, host, port, remoteFilePath, null, null);
        }
        catch (URISyntaxException e) {
            String message = String.format("URISyntaxException was thrown: Illegal character in sftp://%s:******@%s:%s%s", user, host, port, remoteFilePath);
            throw new ConfigException(message);
        }
    }

    FileObject newSftpFile(final URI sftpUri) throws FileSystemException
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

    private <T> T withRetry(Retryable<T> call)
    {
        try {
            return retryExecutor()
                    .withRetryLimit(maxConnectionRetry)
                    .withInitialRetryWait(500)
                    .withMaxRetryWait(30 * 1000)
                    .runInterruptible(call);
        }
        catch (RetryGiveupException ex) {
            throw Throwables.propagate(ex.getCause());
        }
        catch (InterruptedException ex) {
            throw Throwables.propagate(ex);
        }
    }

    private void timedWrite(final OutputStream stream, final byte[] buf, final int len) throws IOException
    {
        try {
            new TimedCallable<Void>()
            {
                @Override
                public Void call() throws Exception
                {
                    stream.write(buf, 0, len);
                    return null;
                }
            }.call(writeTimeout, TimeUnit.SECONDS);
        }
        catch (Exception e) {
            logger.warn("Failed to write buffer, aborting ... ");
            throw new IOException(e);
        }
    }
}
