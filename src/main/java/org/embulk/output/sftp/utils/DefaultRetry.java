/*
 * Copyright 2018 The Embulk project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.embulk.output.sftp.utils;

import com.jcraft.jsch.JSchException;
import org.embulk.config.ConfigException;
import org.embulk.util.retryhelper.Retryable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DefaultRetry<T> implements Retryable<T>
{
    private Logger logger = LoggerFactory.getLogger(getClass());

    private final String task;

    protected DefaultRetry(String task)
    {
        this.task = task;
    }

    @Override
    public boolean isRetryableException(Exception exception)
    {
        return !hasRootCauseUserProblem(exception);
    }

    @Override
    public void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait)
    {
        String message = String.format("%s failed. Retrying %d/%d after %d seconds. Message: %s",
                task, retryCount, retryLimit, retryWait / 1000, exception.getMessage());
        if (retryCount % 3 == 0) {
            logger.warn(message, exception);
        }
        else {
            logger.warn(message);
        }
    }

    @Override
    public void onGiveup(Exception firstException, Exception lastException)
    {
        if (hasRootCauseUserProblem(lastException)) {
            throw new ConfigException(lastException);
        }
    }

    private static boolean isAuthFail(Throwable e)
    {
        return e instanceof JSchException && (e.getMessage().contains("Auth fail") || e.getMessage().contains("USERAUTH fail"));
    }

    private static boolean isConnectionProblem(Throwable e)
    {
        return e instanceof JSchException && (e.getMessage().contains("Connection refused"));
    }

    private static boolean hasRootCauseUserProblem(Throwable e)
    {
        while (e != null && !isAuthFail(e) && !isConnectionProblem(e)) {
            e = e.getCause();
        }
        return e != null;
    }
}
