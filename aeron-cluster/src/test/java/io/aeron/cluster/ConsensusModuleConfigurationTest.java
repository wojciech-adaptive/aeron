/*
 * Copyright 2014-2024 Real Logic Limited.
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
package io.aeron.cluster;

import io.aeron.cluster.codecs.BackupQueryDecoder;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.SessionConnectRequestDecoder;
import io.aeron.security.AuthorisationService;
import org.junit.jupiter.api.Test;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConsensusModuleConfigurationTest
{
    @Test
    void shouldUseDenyAllAuthorisationSupplierWhenPropertySet()
    {
        final String denyAll = "DENY_ALL";
        System.setProperty(ConsensusModule.Configuration.AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME, denyAll);
        try
        {
            final AuthorisationService authorisationService =
                requireNonNull(ConsensusModule.Configuration.authorisationServiceSupplier()).get();

            assertFalse(authorisationService.isAuthorised(
                MessageHeaderDecoder.SCHEMA_ID, BackupQueryDecoder.TEMPLATE_ID, null, new byte[0]));
            assertFalse(authorisationService.isAuthorised(
                MessageHeaderDecoder.SCHEMA_ID, SessionConnectRequestDecoder.TEMPLATE_ID, null, new byte[0]));
        }
        finally
        {
            System.clearProperty(ConsensusModule.Configuration.AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME);
        }

        final AuthorisationService authorisationService =
            requireNonNull(ConsensusModule.Configuration.authorisationServiceSupplier()).get();

        assertTrue(authorisationService.isAuthorised(
            MessageHeaderDecoder.SCHEMA_ID, BackupQueryDecoder.TEMPLATE_ID, null, new byte[0]));
        assertFalse(authorisationService.isAuthorised(
            MessageHeaderDecoder.SCHEMA_ID, SessionConnectRequestDecoder.TEMPLATE_ID, null, new byte[0]));
    }

    @Test
    void shouldUseAllowAllAuthorisationSupplierWhenPropertySet()
    {
        final String denyAll = "ALLOW_ALL";
        System.setProperty(ConsensusModule.Configuration.AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME, denyAll);
        try
        {
            final AuthorisationService authorisationService =
                requireNonNull(ConsensusModule.Configuration.authorisationServiceSupplier()).get();

            assertTrue(authorisationService.isAuthorised(
                MessageHeaderDecoder.SCHEMA_ID, BackupQueryDecoder.TEMPLATE_ID, null, new byte[0]));
            assertTrue(authorisationService.isAuthorised(
                MessageHeaderDecoder.SCHEMA_ID, SessionConnectRequestDecoder.TEMPLATE_ID, null, new byte[0]));
        }
        finally
        {
            System.clearProperty(ConsensusModule.Configuration.AUTHORISATION_SERVICE_SUPPLIER_PROP_NAME);
        }

        final AuthorisationService authorisationService =
            requireNonNull(ConsensusModule.Configuration.authorisationServiceSupplier()).get();

        assertTrue(authorisationService.isAuthorised(
            MessageHeaderDecoder.SCHEMA_ID, BackupQueryDecoder.TEMPLATE_ID, null, new byte[0]));
        assertFalse(authorisationService.isAuthorised(
            MessageHeaderDecoder.SCHEMA_ID, SessionConnectRequestDecoder.TEMPLATE_ID, null, new byte[0]));
    }
}
