package org.embulk.output.sftp.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;

import java.io.Closeable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class TimeoutCloser implements Closeable
{
    @VisibleForTesting
    int timeout = 300; // 5 minutes
    private Closeable wrapped;

    public TimeoutCloser(Closeable wrapped)
    {
        this.wrapped = wrapped;
    }

    @Override
    public void close()
    {
        try {
            new TimedCallable<Void>()
            {
                @Override
                public Void call() throws Exception
                {
                    if (wrapped != null) {
                        wrapped.close();
                    }
                    return null;
                }
            }.call(timeout, TimeUnit.SECONDS);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw Throwables.propagate(e);
        }
    }
}
