package org.embulk.output.sftp.util;

import org.embulk.spi.Exec;
import org.slf4j.Logger;

/**
 * Created by takahiro.nakayama on 3/15/16.
 */
public class Retriable
{
    private static final Logger logger = Exec.getLogger(Retriable.class);
    private final int baseSeconds;
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
        private int maxRetry = 5;    // default value
        private int baseSeconds = 2; // default value

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
            this.baseSeconds = baseSeconds;
            return this;
        }

        public Retriable build()
        {
            return new Retriable(baseSeconds, maxRetry);
        }
    }

    private Retriable(int baseSeconds, int maxRetry)
    {
        this.baseSeconds = baseSeconds;
        this.maxRetry = maxRetry;
    }

    public interface Callable<T> {
        T call() throws RetriableException;
    }

    public  <T> T callWithExponentialBackoff(Callable<T> i)
            throws MaxRetriesExceededException
    {
        int count = 0;
        while (true) {
            try {
                return i.call();
            }
            catch (RetriableException e) {
                if (count++ >= maxRetry) {
                    throw new MaxRetriesExceededException(e);
                }
                logger.warn("catch retriable failure: " + e.getMessage(), e);
            }
            sleep(getSleepTime(count));
            logger.warn("retry to connect sftp server: " + count + " times");
        }
    }

    private long getSleepTime(int count)
    {
        return  ((long) Math.pow(baseSeconds, count) * 1000);
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
