/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.test.integration;

import static au.csiro.pathling.test.helpers.TerminologyHelpers.LC_29463_7;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.LC_55915_3;
import static com.github.tomakehurst.wiremock.client.WireMock.proxyAllTo;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.io.CacheableDatabase;
import au.csiro.pathling.terminology.DefaultTerminologyServiceFactory;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyService.Property;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import com.github.tomakehurst.wiremock.recording.RecordSpecBuilder;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

/**
 * @author Piotr Szul
 */

@Tag("Tranche2")
@Slf4j
@TestPropertySource(properties = {"pathling.terminology.acceptLanguage=fr-FR"})
@ActiveProfiles({"core", "server", "integration-test"})
class TerminologyServiceWithLanguageIntegrationTest extends WireMockTest {

  @Autowired
  private TerminologyServiceFactory terminologyServiceFactory;


  // Mocking the Database bean to avoid lengthy initialization
  @SuppressWarnings("unused")
  @MockBean
  private CacheableDatabase database;

  @Value("${pathling.test.recording.terminologyServerUrl}")
  String recordingTxServerUrl;

  private TerminologyService terminologyService;

  
  @BeforeAll
  public static void beforeAll() {
    DefaultTerminologyServiceFactory.reset();
  }

  @AfterAll
  public static void afterAll() {
    DefaultTerminologyServiceFactory.reset();
  }
  
  @BeforeEach
  @Override
  void setUp() {
    super.setUp();
    terminologyService = terminologyServiceFactory.build();
    if (isRecordMode()) {
      wireMockServer.resetAll();
      log.warn("Proxying all request to: {}", recordingTxServerUrl);
      stubFor(proxyAllTo(recordingTxServerUrl));
    }
  }
  
  @AfterEach
  @Override
  void tearDown() {
    if (isRecordMode()) {
      log.warn("Recording snapshots to: {}", wireMockServer.getOptions().filesRoot());
      wireMockServer
          .snapshotRecord(new RecordSpecBuilder().matchRequestBodyWithEqualToJson(true, false));
    }
    super.tearDown();
  }

  @Test
  void testLookupDisplayPropertyWithLanguage() {
    
    assertEquals(
        List.of(Property.of("display", new StringType(
            "Beta-2-Globulin [Masse/Volumen] in Zerebrospinalflüssigkeit mit Elektrophorese"))),
        terminologyService.lookup(LC_55915_3, "display", "de-DE;q=0.9,fr-FR;q=0.8"));

    assertEquals(
        List.of(Property.of("display", new StringType(
            "Poids corporel [Masse] Patient ; Numérique"))),
        terminologyService.lookup(LC_29463_7, "display"));
  }
}
