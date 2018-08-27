package org.embulk.output.sftp.utils;

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

    public V call(long timeout, TimeUnit timeUnit)
            throws InterruptedException, ExecutionException, TimeoutException
    {
        FutureTask<V> task = new FutureTask<>(this);
        try {
            THREAD_POOL.execute(task);
            return task.get(timeout, timeUnit);
        }
        finally {
            task.cancel(true);
        }
    }
}
