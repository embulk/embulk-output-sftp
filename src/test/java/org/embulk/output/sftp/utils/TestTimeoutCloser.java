package org.embulk.output.sftp.utils;

import org.embulk.EmbulkTestRuntime;
import org.junit.Rule;
import org.junit.Test;

import java.io.Closeable;
import java.util.concurrent.TimeoutException;

import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

public class TestTimeoutCloser
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Test
    public void testTimeoutAndAbort()
    {
        TimeoutCloser closer = new TimeoutCloser(new Closeable()
        {
            @Override
            public void close()
            {
                try {
                    Thread.sleep(5000);
                }
                catch (InterruptedException e) {
                    fail("Failed to sleep");
                }
            }
        });
        closer.timeout = 1;
        try {
            closer.close();
            fail("Should not finish");
        }
        catch (Exception e) {
            assertThat(e, instanceOf(RuntimeException.class));
            assertThat(e.getCause(), instanceOf(TimeoutException.class));
        }
    }
}
