package org.embulk.output.sftp.utils;

import org.embulk.EmbulkTestRuntime;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

public class TestTimedCallable
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Test
    public void testCallNonInterruptible()
    {
        try {
            new TimedCallable<Void>()
            {
                @Override
                public Void call() throws Exception
                {
                    throw new Exception("Fake error");
                }
            }.callNonInterruptible(1, TimeUnit.SECONDS);
        }
        catch (Exception e) {
            fail("Should not throw exception");
        }
    }

    @Test
    public void testCallTimeout()
    {
        try {
            new TimedCallable<Void>()
            {
                @Override
                public Void call() throws Exception
                {
                    Thread.sleep(200);
                    return null;
                }
            }.call(100, TimeUnit.MILLISECONDS);
        }
        catch (Exception e) {
            assertThat(e, instanceOf(TimeoutException.class));
        }
    }
}
