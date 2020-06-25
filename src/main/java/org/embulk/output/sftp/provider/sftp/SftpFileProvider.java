/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.embulk.output.sftp.provider.sftp;

import com.jcraft.jsch.Session;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.UserAuthenticationData;
import org.apache.commons.vfs2.provider.GenericFileName;
import org.apache.commons.vfs2.provider.sftp.SftpClientFactory;
import org.apache.commons.vfs2.util.UserAuthenticatorUtils;
import org.embulk.spi.Exec;
import org.embulk.spi.util.RetryExecutor;
import org.slf4j.Logger;

/*
 * We can remove this file when Apache Commons VFS2 removes permission check logic when remote file renaming.
 * https://github.com/embulk/embulk-output-sftp/issues/40
 * https://github.com/embulk/embulk-output-sftp/pull/44
 * https://issues.apache.org/jira/browse/VFS-590
 */
/**
 * A provider for accessing files over SFTP.
 */
public class SftpFileProvider extends org.apache.commons.vfs2.provider.sftp.SftpFileProvider
{
    private final Logger logger = Exec.getLogger(SftpFileProvider.class);
    /**
     * Constructs a new provider.
     */
    public SftpFileProvider()
    {
        super();
    }

    /**
     * Creates a {@link FileSystem}.
     */
    @Override
    protected FileSystem doCreateFileSystem(final FileName name, final FileSystemOptions fileSystemOptions)
            throws FileSystemException
    {
        // JSch jsch = createJSch(fileSystemOptions);
        // Create the file system
        final GenericFileName rootName = (GenericFileName) name;
        Session session;
        UserAuthenticationData authData = null;
        try {
            authData = UserAuthenticatorUtils.authenticate(fileSystemOptions, AUTHENTICATOR_TYPES);
            RetryExecutor retryExec = RetryExecutor.retryExecutor()
                    .withRetryLimit(7)
                    .withInitialRetryWait(1000) //1 seconds
                    .withMaxRetryWait(300000); //5 minutes
            final UserAuthenticationData finalAuthData = authData;
            session = retryExec.runInterruptible(new RetryExecutor.Retryable<Session>() {
                public Session call() throws FileSystemException {
                    try {
                        return SftpClientFactory.createConnection(rootName.getHostName(), rootName.getPort(),
                                UserAuthenticatorUtils.getData(finalAuthData, UserAuthenticationData.USERNAME,
                                        UserAuthenticatorUtils.toChar(rootName.getUserName())),
                                UserAuthenticatorUtils.getData(finalAuthData, UserAuthenticationData.PASSWORD,
                                        UserAuthenticatorUtils.toChar(rootName.getPassword())),
                                fileSystemOptions);
                    }
                    catch (Exception e) {
                        logger.error("Create SFTP connection was failed: {}", e.getMessage());
                        throw e;
                    }
                }

                public boolean isRetryableException(Exception e)
                {
                    return true;
                }

                public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait)
                {
                    String message = String.format("%s failed. Retrying %d/%d after %d seconds. Message: %s",
                            "Create SFTP connection ", retryCount, retryLimit, retryWait / 1000, exception.getMessage());
                    if (retryCount % retryLimit == 0) {
                        logger.warn(message, exception);
                    }
                    else {
                        logger.warn(message);
                    }
                }

                public void onGiveup(Exception firstException, Exception lastException) throws RetryExecutor.RetryGiveupException
                {
                    logger.error("Giving up on retrying for {}, first exception is [{}], last exception is [{}]",
                            "Create SFTP connection ", firstException.getMessage(), lastException.getMessage());
                    throw new RetryExecutor.RetryGiveupException(lastException);
                }
            });

        }
        catch (final Exception e) {
            throw new FileSystemException("vfs.provider.sftp/connect.error", name, e);
        }
        finally {
            UserAuthenticatorUtils.cleanup(authData);
        }
        return new SftpFileSystem(rootName, session, fileSystemOptions);
    }
}
