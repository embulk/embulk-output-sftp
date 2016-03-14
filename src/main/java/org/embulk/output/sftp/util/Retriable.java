package org.embulk.output.sftp.util;

import org.embulk.spi.Exec;
import org.slf4j.Logger;

/**
 * Created by takahiro.nakayama on 3/15/16.
 */
public class Retriable
{
    private static final Logger logger = Exec.getLogger(Retriable.class);
    private final int baseIntervalSeconds;
    private final int maxRetry;

    public static class RetriableException
            extends Exception
    {
        public RetriableException(String message)
        {
            super(message);
        }

        public RetriableException(Throwable cause)
        {
            super(cause);
        }
    }

    public static class MaxRetriesExceededException
            extends Exception
    {
        public MaxRetriesExceededException(Throwable cause)
        {
            super(cause);
        }
    }

    public static class Builder
    {
        private int maxRetry = 7;    // default value
        private int baseIntervalSeconds = 2; // default value

        public Builder(int maxRetry)
        {
            this();
            this.maxRetry(maxRetry);
        }

        public Builder()
        {
        }

        public Builder maxRetry(int maxRetry)
        {
            this.maxRetry = maxRetry;
            return this;
        }

        public Builder baseSeconds(int baseSeconds)
        {
            this.baseIntervalSeconds = baseSeconds;
            return this;
        }

        public Retriable build()
        {
            return new Retriable(baseIntervalSeconds, maxRetry);
        }
    }

    private Retriable(int baseIntervalSeconds, int maxRetry)
    {
        this.baseIntervalSeconds = baseIntervalSeconds;
        this.maxRetry = maxRetry;
    }

    public interface Callable<T> {
        T call() throws RetriableException;
    }

    public  <T> T callWithExponentialBackoff(Callable<T> callable)
            throws MaxRetriesExceededException
    {
        int count = 0;
        while (true) {
            try {
                return callable.call();
            }
            catch (RetriableException e) {
                if (count++ >= maxRetry) {
                    throw new MaxRetriesExceededException(e);
                }
                logger.warn("catch retriable failure: " + e.getMessage(), e);
            }
            sleep((long) Math.pow(baseIntervalSeconds, count) * 1000);
            logger.warn("retry to connect sftp server: " + count + " times");
        }
    }

    private void sleep(long milliseconds)
    {
        try {
            logger.warn("sleep in next connection retry: {} milliseconds", milliseconds);
            Thread.sleep(milliseconds); // milliseconds
        }
        catch (InterruptedException e) {
            // Ignore this exception because this exception is just about `sleep`.
            logger.warn(e.getMessage(), e);
        }
    }
}
