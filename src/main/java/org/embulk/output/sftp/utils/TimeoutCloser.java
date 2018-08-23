package org.embulk.output.sftp.utils;

import com.google.common.annotations.VisibleForTesting;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

public class TimeoutCloser implements Closeable
{
    @VisibleForTesting
    int timeout = 120;
    private Closeable wrapped;

    public TimeoutCloser(Closeable wrapped)
    {
        this.wrapped = wrapped;
    }

    @Override
    public void close()
    {
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
        }.callNonInterruptible(timeout, TimeUnit.SECONDS);
    }
}
