/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
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

package au.csiro.pathling.terminology.store;

import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.net.ServerSocket;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies the registry checksum lookup against a stubbed registry: a matching checksum verifies, a
 * differing one fails the import, and every way the registry can fail to answer with a usable
 * checksum leaves the package unverified with a reason rather than failing.
 *
 * @author John Grimes
 */
class PackageRegistryVerifierTest {

  private static final String NAME = "fixtures";
  private static final String VERSION = "1.0.0";
  private static final String SHA1 = "8a7a096866b9b6e96e288ce8d72bf99aa31714a3";
  private static final String OTHER_SHA1 = "0000000000000000000000000000000000000001";

  private WireMockServer server;

  @BeforeEach
  void startServer() {
    server = new WireMockServer(WireMockConfiguration.options().dynamicPort());
    server.start();
  }

  @AfterEach
  void stopServer() {
    server.stop();
  }

  @Test
  void verifiedWhenShasumMatches() {
    RegistryStub.stubListing(server, NAME, VERSION, SHA1);

    final PackageVerificationResult result = verifier(server.baseUrl()).verify(NAME, VERSION, SHA1);

    assertEquals(PackageVerification.VERIFIED, result.getStatus());
    // The registry that vouched for the bytes is recorded alongside the outcome.
    assertEquals(server.baseUrl(), result.getRegistry());
    assertNull(result.getReason());
    server.verify(getRequestedFor(urlEqualTo(RegistryStub.path(NAME))));
  }

  @Test
  void verifiedIgnoresShasumCase() {
    RegistryStub.stubListing(server, NAME, VERSION, SHA1.toUpperCase());

    final PackageVerificationResult result = verifier(server.baseUrl()).verify(NAME, VERSION, SHA1);

    assertEquals(PackageVerification.VERIFIED, result.getStatus());
  }

  @Test
  void mismatchThrowsNamingBothHashes() {
    RegistryStub.stubListing(server, NAME, VERSION, OTHER_SHA1);
    final PackageRegistryVerifier verifier = verifier(server.baseUrl());

    final TerminologyImportException e =
        assertThrows(TerminologyImportException.class, () -> verifier.verify(NAME, VERSION, SHA1));

    final String message = e.getMessage();
    assertTrue(message.contains("does not match the registry checksum"), message);
    assertTrue(message.contains(NAME), message);
    assertTrue(message.contains(VERSION), message);
    assertTrue(message.contains(server.baseUrl()), message);
    assertTrue(message.contains(OTHER_SHA1), message);
    assertTrue(message.contains(SHA1), message);
    assertTrue(message.contains("verifyPackage"), message);
  }

  @Test
  void unverifiedWhenNoShasum() {
    RegistryStub.stubListing(server, NAME, VERSION, null);

    assertUnverified(verifier(server.baseUrl()).verify(NAME, VERSION, SHA1));
  }

  @Test
  void unverifiedWhenNoDist() {
    RegistryStub.stubListingWithoutDist(server, NAME, VERSION);

    assertUnverified(verifier(server.baseUrl()).verify(NAME, VERSION, SHA1));
  }

  @Test
  void unverifiedWhenVersionNotListed() {
    RegistryStub.stubListing(server, NAME, "2.0.0", SHA1);

    assertUnverified(verifier(server.baseUrl()).verify(NAME, VERSION, SHA1));
  }

  @Test
  void unverifiedOn404() {
    RegistryStub.stubNotFound(server, NAME);

    assertUnverified(verifier(server.baseUrl()).verify(NAME, VERSION, SHA1));
  }

  @Test
  void unverifiedOn500() {
    RegistryStub.stubServerError(server, NAME);

    assertUnverified(verifier(server.baseUrl()).verify(NAME, VERSION, SHA1));
  }

  @Test
  void unverifiedOnMalformedBody() {
    RegistryStub.stubMalformed(server, NAME);

    assertUnverified(verifier(server.baseUrl()).verify(NAME, VERSION, SHA1));
  }

  @Test
  void unverifiedWhenConnectionRefused() throws IOException {
    final int closedPort;
    try (ServerSocket socket = new ServerSocket(0)) {
      closedPort = socket.getLocalPort();
    }

    assertUnverified(verifier("http://127.0.0.1:" + closedPort).verify(NAME, VERSION, SHA1));
  }

  @Test
  void followsRedirect() {
    RegistryStub.stubRedirect(server, NAME, VERSION, SHA1);

    final PackageVerificationResult result =
        verifier(server.baseUrl() + RegistryStub.REDIRECT_PREFIX).verify(NAME, VERSION, SHA1);

    assertEquals(PackageVerification.VERIFIED, result.getStatus());
    server.verify(getRequestedFor(urlEqualTo(RegistryStub.path(NAME))));
  }

  @Test
  void trailingSlashOnRegistryIsIgnored() {
    RegistryStub.stubListing(server, NAME, VERSION, SHA1);
    final PackageRegistryVerifier verifier = verifier(server.baseUrl() + "//");

    assertEquals(server.baseUrl(), verifier.getRegistryUrl());
    assertEquals(PackageVerification.VERIFIED, verifier.verify(NAME, VERSION, SHA1).getStatus());
    server.verify(getRequestedFor(urlEqualTo(RegistryStub.path(NAME))));
  }

  @Test
  void packageNameIsPathEncoded() {
    final String spaced = "spaced name";
    RegistryStub.stubListing(server, spaced, VERSION, SHA1);

    final PackageVerificationResult result =
        verifier(server.baseUrl()).verify(spaced, VERSION, SHA1);

    assertEquals(PackageVerification.VERIFIED, result.getStatus());
    server.verify(getRequestedFor(urlEqualTo("/spaced%20name")));
  }

  @Test
  void nullRegistrySelectsTheDefault() {
    // No lookup is made, so the default is exercised without contacting the real registry.
    assertEquals(
        PackageRegistryVerifier.DEFAULT_REGISTRY_URL,
        new PackageRegistryVerifier(null).getRegistryUrl());
    assertEquals("https://packages.fhir.org", PackageRegistryVerifier.DEFAULT_REGISTRY_URL);
    assertEquals(0, server.getAllServeEvents().size());
  }

  private static void assertUnverified(@Nonnull final PackageVerificationResult result) {
    assertEquals(PackageVerification.UNVERIFIED, result.getStatus());
    // No registry vouched for the bytes, but the reason is available for the warning.
    assertNull(result.getRegistry());
    assertNotNull(result.getReason());
  }

  @Nonnull
  private static PackageRegistryVerifier verifier(@Nonnull final String registryUrl) {
    return new PackageRegistryVerifier(registryUrl);
  }
}
