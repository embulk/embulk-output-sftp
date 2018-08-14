package org.embulk.output.sftp.utils;

import com.google.common.base.Optional;
import org.embulk.spi.Exec;
import org.slf4j.Logger;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class TimedCallable<V> implements Callable<V>
{
    private static final ExecutorService THREAD_POOL = Executors.newCachedThreadPool();
    private Logger logger = Exec.getLogger(getClass());

    public V callNonInterruptible(long timeout, TimeUnit timeUnit)
    {
        try {
            return call(timeout, timeUnit);
        }
        catch (Exception e) {
            logger.warn("Failed with exception {}: {}", e.getClass(), Optional.fromNullable(e.getMessage()).or(""));
            return null;
        }
    }

    public V call(long timeout, TimeUnit timeUnit)
            throws InterruptedException, ExecutionException, TimeoutException
    {
        FutureTask<V> task = new FutureTask<>(this);
        try {
            THREAD_POOL.execute(task);
            return task.get(timeout, timeUnit);
        }
        catch (TimeoutException e) {
            logger.warn("Execution timed out, re-throwing ...");
            task.cancel(true);
            throw e;
        }
    }
}
