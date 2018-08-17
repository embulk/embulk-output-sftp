package org.embulk.output.sftp.utils;

import com.jcraft.jsch.JSchException;
import org.embulk.spi.Exec;
import org.embulk.spi.util.RetryExecutor;
import org.slf4j.Logger;

public abstract class DefaultRetry<T> implements RetryExecutor.Retryable<T>
{
    private Logger logger = Exec.getLogger(getClass());

    private final String task;

    protected DefaultRetry(String task)
    {
        this.task = task;
    }

    @Override
    public boolean isRetryableException(Exception exception)
    {
        return !hasRootCauseAuthFail(exception);
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
    }

    private static boolean isAuthFail(Throwable e)
    {
        return e instanceof JSchException && "USERAUTH fail".equals(e.getMessage());
    }

    private static boolean hasRootCauseAuthFail(Throwable e)
    {
        while (e != null && !isAuthFail(e)) {
            e = e.getCause();
        }
        return e != null;
    }
}
