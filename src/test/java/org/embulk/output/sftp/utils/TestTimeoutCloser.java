/*
 * Copyright 2018 The Embulk project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
        TimeoutCloser.setTimeout(1);
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
