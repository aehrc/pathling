/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.terminology.caching;

import static au.csiro.pathling.test.helpers.TerminologyHelpers.LOINC_URI;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.SNOMED_URI;
import static com.github.tomakehurst.wiremock.client.WireMock.anyRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.proxyAllTo;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static java.util.Objects.requireNonNull;
import static org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome.NOTSUBSUMED;
import static org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome.SUBSUMEDBY;
import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.config.HttpClientCachingConfiguration;
import au.csiro.pathling.config.HttpClientCachingStorageType;
import au.csiro.pathling.config.HttpClientConfiguration;
import au.csiro.pathling.config.TerminologyAuthConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.terminology.DefaultTerminologyServiceFactory;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyService.Property;
import au.csiro.pathling.terminology.TerminologyService.Translation;
import ca.uhn.fhir.context.FhirVersionEnum;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.recording.RecordSpecBuilder;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@Slf4j
@TestInstance(Lifecycle.PER_CLASS)
@TestMethodOrder(OrderAnnotation.class)
class CachingTerminologyServiceTest {

  static final int WIREMOCK_PORT = 4072;
  static final Coding T2D_CODING = new Coding(SNOMED_URI, "44054006",
      "Type 2 diabetes mellitus");
  static final Coding CHLOROQUINE_POISONING_CODING = new Coding(SNOMED_URI, "45110008",
      "Chloroquine poisoning");
  static final Coding DIABETES_CODING = new Coding(SNOMED_URI, "73211009",
      "Diabetes mellitus");
  static final Coding INACTIVE_CHLOROQUINE_POISONING_CODING = new Coding(SNOMED_URI,
      "291446002", null);
  static final String SAME_AS_CONCEPT_MAP_URL = SNOMED_URI + "?fhir_cm=900000000000527005";
  static final String ALL_CODES_VALUE_SET_URL = SNOMED_URI + "?fhir_vs";
  static final String NON_EXISTENT_VALUE_SET = "urn:test:non-existent-value-set";
  static final Coding LOINC_CODING = new Coding(LOINC_URI, "2939-7", null);

  static WireMockServer wireMockServer;
  static DefaultTerminologyServiceFactory terminologyServiceFactory;

  TerminologyService terminologyService;

  /**
   * @param name A display name for the test.
   * @param supplier A function to execute the terminology service operation.
   * @param expectedResult The expected result of the operation.
   * @param badRequest Whether the request was intended to be invalid.
   */
  record TestParameters(
      @Nonnull String name,
      @Nonnull Supplier<Object> supplier,
      @Nonnull Object expectedResult,
      boolean badRequest,
      boolean invalidParameters
  ) {

    @Nonnull
    @Override
    public String toString() {
      return name;
    }

  }

  Stream<TestParameters> parameters() {
    return Stream.of(
        // Validate code - check that "Type 2 diabetes mellitus" is in the set of all SNOMED codes.
        new TestParameters("validate-code", () -> terminologyService.validateCode(
            ALL_CODES_VALUE_SET_URL, T2D_CODING), true, false, false),

        // Translate - translate "Chloroquine poisoning of undetermined intent" to the code that 
        // replaces it (within the SAME AS historical association reference set).
        new TestParameters("translate",
            () -> terminologyService.translate(INACTIVE_CHLOROQUINE_POISONING_CODING,
                SAME_AS_CONCEPT_MAP_URL, false, null),
            List.of(Translation.of(ConceptMapEquivalence.EQUAL, CHLOROQUINE_POISONING_CODING)),
            false, false),

        // Subsumes - check that "Type 2 diabetes mellitus" is subsumed by "Diabetes mellitus".
        new TestParameters("subsumes",
            () -> terminologyService.subsumes(T2D_CODING, DIABETES_CODING), SUBSUMEDBY, false,
            false),

        // Lookup - get the "inactive" property for "Chloroquine poisoning of undetermined intent".
        new TestParameters("lookup",
            () -> terminologyService.lookup(INACTIVE_CHLOROQUINE_POISONING_CODING, "inactive"),
            List.of(Property.of("inactive", new BooleanType(true))), false, false),

        // Invalid validate code request (not found from the server).
        new TestParameters("invalid validate-code", () -> terminologyService.validateCode(
            NON_EXISTENT_VALUE_SET, CHLOROQUINE_POISONING_CODING), false, true, false),

        // Invalid subsumes request (parameters that do not pass validation).
        new TestParameters("invalid subsumes",
            () -> terminologyService.subsumes(LOINC_CODING, DIABETES_CODING), NOTSUBSUMED, false,
            true)
    );
  }

  @BeforeAll
  static void beforeAll() {
    wireMockServer = new WireMockServer(
        new WireMockConfiguration().port(WIREMOCK_PORT)
            .usingFilesUnderDirectory("src/test/resources/wiremock/CachingTerminologyServiceTest"));
    WireMock.configureFor("localhost", WIREMOCK_PORT);
    wireMockServer.start();

    final HttpClientConfiguration clientConfig = HttpClientConfiguration.builder()
        .socketTimeout(5_000)
        .maxConnectionsTotal(1)
        .maxConnectionsPerRoute(1)
        .build();
    final HttpClientCachingConfiguration cacheConfig = HttpClientCachingConfiguration.builder()
        .enabled(true)
        .maxEntries(1_000)
        .storageType(HttpClientCachingStorageType.MEMORY)
        .build();
    final TerminologyAuthConfiguration authConfig = TerminologyAuthConfiguration.builder()
        .enabled(false)
        .build();
    final TerminologyConfiguration config = TerminologyConfiguration.builder()
        .serverUrl("http://localhost:" + WIREMOCK_PORT + "/fhir")
        .client(clientConfig)
        .cache(cacheConfig)
        .authentication(authConfig)
        .build();

    terminologyServiceFactory = new DefaultTerminologyServiceFactory(FhirVersionEnum.R4, config);
  }

  @BeforeEach
  void setUp() {
    terminologyService = terminologyServiceFactory.build();
    if (isRecordMode()) {
      log.warn("Proxying all request to: {}", recordingTxServerUrl());
      stubFor(proxyAllTo(recordingTxServerUrl()));
    }
  }

  @ParameterizedTest
  @MethodSource("parameters")
  @Order(1)
  void coldRequest(@Nonnull final TestParameters parameters) {
    // Execute the request 2 times.
    for (int i = 0; i < 2; i++) {
      // Execute the request.
      final Object result = requireNonNull(parameters.supplier().get());
      // Assert that the result equals the expected result.
      assertEquals(parameters.expectedResult(), result);
    }

    // Verify that the request was only made once.
    verify(parameters.invalidParameters()
           // If the request was invalid, then the request should not have been made.
           ? 0
           : 1, anyRequestedFor(anyUrl()));
  }

  @ParameterizedTest
  @MethodSource("parameters")
  @Order(2)
  void revalidationOfExpiredResult(@Nonnull final TestParameters parameters)
      throws InterruptedException {
    if (isRecordMode()) {
      stubFor(proxyAllTo(recordingTxServerUrl()).inScenario("revalidationOfExpiredResult"));
    }

    // Execute the request 2 times.
    for (int i = 0; i < 2; i++) {
      // Execute the request.
      final Object result = requireNonNull(parameters.supplier().get());
      // Assert that the result equals the expected result.
      assertEquals(parameters.expectedResult(), result);

      if (!parameters.badRequest()) {
        // Wait until the 1-second max age has passed.
        Thread.sleep(2_000);
      }
    }

    if (parameters.invalidParameters()) {
      // If the parameters were invalid, no request should have been made.
      verify(0, anyRequestedFor(anyUrl()));
    } else if (parameters.badRequest()) {
      // If the request was invalid, verify that the request was only made once (i.e. it was 
      // cached as a bad request, which has an expiry longer than 1 second).
      verify(1, anyRequestedFor(anyUrl()));
    } else {
      // If the request was not invalid, verify that two requests were made. The second operation 
      // should have been run past the max age, triggering a revalidation request.
      verify(2, anyRequestedFor(anyUrl()));
      verify(1, anyRequestedFor(anyUrl())
          .withHeader("If-None-Match", matching(".*")));
    }
  }

  @AfterEach
  void tearDown() {
    if (isRecordMode()) {
      log.warn("Recording snapshots to: {}", wireMockServer.getOptions().filesRoot());
      final RecordSpecBuilder specBuilder = new RecordSpecBuilder()
          .matchRequestBodyWithEqualToJson(true, false)
          .captureHeader("If-None-Match");
      wireMockServer.snapshotRecord(specBuilder);
    }
    wireMockServer.resetRequests();
    DefaultTerminologyServiceFactory.reset();
  }

  @AfterAll
  static void afterAll() {
    wireMockServer.stop();
  }

  static boolean isRecordMode() {
    return Boolean.parseBoolean(System.getProperty("pathling.test.recording.enabled", "false"));
  }

  static String recordingTxServerUrl() {
    return requireNonNull(System.getProperty("pathling.test.recording.txServerUrl"));
  }

}
