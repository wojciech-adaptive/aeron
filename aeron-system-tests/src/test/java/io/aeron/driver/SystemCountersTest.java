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
import io.aeron.CommonContext;
import io.aeron.command.ControlProtocolEvents;
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.status.CountersReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SystemCountersTest
{
    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    private TestMediaDriver driver;
    private Aeron aeron;

    @BeforeEach
    void before()
    {
        final MediaDriver.Context context = new MediaDriver.Context()
            .aeronDirectoryName(CommonContext.generateRandomDirName())
            .dirDeleteOnStart(true)
            .dirDeleteOnShutdown(true)
            .threadingMode(ThreadingMode.SHARED);
        driver = TestMediaDriver.launch(context, systemTestWatcher);
        systemTestWatcher.dataCollector().add(driver.context().aeronDirectory());

        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(context.aeronDirectoryName()));
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(aeron, driver);
    }

    @Test
    void verifySystemCounters()
    {
        final CountersReader countersReader = aeron.countersReader();
        final Int2ObjectHashMap<String> idToLabel = new Int2ObjectHashMap<>();
        countersReader.forEach((counterId, typeId, keyBuffer, label) ->
        {
            if (SystemCounterDescriptor.SYSTEM_COUNTER_TYPE_ID == typeId)
            {
                assertEquals(counterId, keyBuffer.getInt(0));
                idToLabel.put(counterId, label);
            }
        });

        for (final SystemCounterDescriptor counter : SystemCounterDescriptor.values())
        {
            final String counterLabel = idToLabel.get(counter.id());
            assertThat(counterLabel, startsWith(counter.label()));
            assertEquals(counter.id(), countersReader.getCounterRegistrationId(counter.id()));
            assertEquals(Aeron.NULL_VALUE, countersReader.getCounterOwnerId(counter.id()));
        }
    }

    @Test
    void controlProtocolVersion()
    {
        assertEquals(
            ControlProtocolEvents.CONTROL_PROTOCOL_SEMANTIC_VERSION,
            aeron.countersReader().getCounterValue(SystemCounterDescriptor.CONTROL_PROTOCOL_VERSION.id()));
    }
}
