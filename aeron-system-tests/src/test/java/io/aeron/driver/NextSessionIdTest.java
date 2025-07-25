/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.driver;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static io.aeron.CommonContext.generateRandomDirName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class NextSessionIdTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    private TestMediaDriver driver;
    private Aeron aeron;

    @BeforeEach
    void before()
    {
        driver = TestMediaDriver.launch(new MediaDriver.Context()
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
            .threadingMode(ThreadingMode.SHARED)
            .aeronDirectoryName(generateRandomDirName()), systemTestWatcher);

        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));

        systemTestWatcher.dataCollector().add(driver.context().aeronDirectory());
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(aeron, driver);
    }

    @Test
    void shouldFetchNextAvailableSessionId()
    {
        final ExclusivePublication publication =
            aeron.addExclusivePublication("aeron:ipc?term-length=64k", 555);

        final int nextSessionId = aeron.nextSessionId(777);
        assertEquals(publication.sessionId() + 1, nextSessionId);

        final int nextSessionId2 = aeron.nextSessionId(42);
        assertEquals(nextSessionId + 1, nextSessionId2);
    }

    @Test
    void shouldSkipActiveSessionId()
    {
        final int nextSessionId = aeron.nextSessionId(777);

        final ExclusivePublication pub1 =
            aeron.addExclusivePublication("aeron:ipc?term-length=64k|session-id=" + (nextSessionId + 1), 555);
        final ExclusivePublication pub2 =
            aeron.addExclusivePublication("aeron:ipc?term-length=64k|session-id=" + (nextSessionId + 2), 333);

        final int result = aeron.nextSessionId(pub1.streamId());
        assertNotEquals(pub1.sessionId(), result);
        assertEquals(pub2.sessionId(), result);
    }
}
