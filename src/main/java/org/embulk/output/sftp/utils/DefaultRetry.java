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
