package org.embulk.output.sftp.utils;

import org.embulk.EmbulkTestRuntime;
import org.junit.Rule;
import org.junit.Test;

import java.io.Closeable;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertTrue;

public class TestTimeoutCloser
{
    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

    @Test
    public void testTimeoutAndResume()
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
        long start = System.currentTimeMillis();
        closer.close();
        long duration = System.currentTimeMillis() - start;
        assertTrue("longer than 1s", duration > 1000);
        assertTrue("shorter than 5s", duration < 5000);
    }
}
