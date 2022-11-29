/*
 * Copyright 2022 Commonwealth Scientific and Industrial Research
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

import au.csiro.pathling.io.Database;
import au.csiro.pathling.terminology.TerminologyService2;
import au.csiro.pathling.terminology.TerminologyService2.Property;
import au.csiro.pathling.terminology.TerminologyService2.PropertyOrDesignation;
import au.csiro.pathling.terminology.TerminologyService2.Translation;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import com.github.tomakehurst.wiremock.recording.RecordSpecBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence;
import org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.mock.mockito.MockBean;
import java.util.*;

import static au.csiro.pathling.test.assertions.Assertions.assertMatches;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.*;
import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Piotr Szul
 */

@Tag("Tranche2")
@Slf4j
class TerminologyService2IntegrationTest extends WireMockTest {

  private static final String SNOMED_VERSION_UNKN = "http://snomed.info/sct/32506021000036107/version/19000101";
  private final static Coding CD_SNOMED_403190006_VERSION_UNKN = newVersionedCoding(
      SNOMED_URI, "403190006",
      SNOMED_VERSION_UNKN, "Epidermal burn of skin");


  private final static Coding UNKNOWN_SYSTEM_CODING = new Coding("uuid:unknown", "unknown",
      "Unknown");

  @Autowired
  SparkSession spark;

  @Autowired
  private TerminologyServiceFactory terminologyServiceFactory;

  @Value("${pathling.test.recording.terminologyServerUrl}")
  String recordingTxServerUrl;

  // we do not need to access the resources here
  @SuppressWarnings("unused")
  @MockBean
  private Database database;

  private TerminologyService2 terminologyService;

  @BeforeEach
  @Override
  void setUp() {
    super.setUp();
    terminologyService = terminologyServiceFactory.buildService2();
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
  void testCorrectlyTranslatesKnownAndUnknownSystems() {

    List<Translation> result = terminologyService.translate(
        CD_SNOMED_72940011000036107, CM_HIST_ASSOCIATIONS, false, null);

    assertEquals(1, result.size());
    assertEquals(ConceptMapEquivalence.EQUIVALENT, result.get(0).getEquivalence());
    assertTrue(codingEquals(CD_SNOMED_720471000168102, result.get(0).getConcept()));

    assertEquals(Collections.emptyList(), terminologyService.translate(
        CD_SNOMED_444814009, CM_HIST_ASSOCIATIONS, false, null));

    assertEquals(Collections.emptyList(), terminologyService.translate(
        CD_AST_VIC, CM_HIST_ASSOCIATIONS, false, null));

    assertEquals(Collections.emptyList(), terminologyService.translate(
        UNKNOWN_SYSTEM_CODING, CM_HIST_ASSOCIATIONS, false, null));

    assertEquals(Collections.emptyList(), terminologyService.translate(
        CD_SNOMED_403190006_VERSION_UNKN, CM_HIST_ASSOCIATIONS, false, null));
  }

  @Test
  void testCorrectlyTranslatesInReverse() {

    List<Translation> result = terminologyService.translate(
        CD_SNOMED_720471000168102_VER2021, CM_HIST_ASSOCIATIONS, true, null);

    assertEquals(1, result.size());
    assertEquals(ConceptMapEquivalence.EQUIVALENT, result.get(0).getEquivalence());
    assertTrue(codingEquals(CD_SNOMED_72940011000036107, result.get(0).getConcept()));
  }

  @Test
  void testTranslatesWithTargetAndMulitpleResults() {

    final Coding input = new Coding(AUTOMAP_INPUT_URI, "shortness of breath", null);
    final String target = "http://snomed.info/sct?fhir_vs=ecl/(%3C%3C%2064572001%20%7CDisease%7C%20OR%20%3C%3C%20404684003%20%7CClinical%20finding%7C)";

    List<Translation> result = terminologyService.translate(
        input, CM_AUTOMAP_DEFAULT, false, target);
    // TODO: Why this one has xsct? But this version cannot be used in input codings?
    final String version = "http://snomed.info/xsct/32506021000036107/version/20220930";
    final Coding result1 = snomedCoding("267036007", "Dyspnea (finding)", version);
    final Coding result2 = snomedCoding("390870001",
        "Short of breath dressing/undressing (finding)", version);
    // final Coding result3 = snomedCoding("1217110005",
    //     "Dyspnea when bending forward (finding)", version);
    // final Coding result4 = snomedCoding("161941007", "Dyspnea at rest (finding)", version);
    // final Coding result5 = snomedCoding("60845006", "Dyspnea on exertion (finding)", version);

    assertEquals(5, result.size());
    assertEquals(ConceptMapEquivalence.INEXACT, result.get(0).getEquivalence());
    assertTrue(codingEquals(result1, result.get(0).getConcept()));
    assertEquals(ConceptMapEquivalence.INEXACT, result.get(1).getEquivalence());
    assertTrue(codingEquals(result2, result.get(1).getConcept()));
  }

  @Test
  void testFailsForUnknownConceptMap() {

    final ResourceNotFoundException error = assertThrows(ResourceNotFoundException.class,
        () -> terminologyService.translate(CD_SNOMED_72940011000036107,
            "http://snomed.info/sct?fhir_cm=xxxx", false,
            null));

    assertMatches(
        "HTTP 404 Not Found: "
            + "\\[.+\\]: "
            + "Unable to find ConceptMap with URI http://snomed\\.info/sct\\?fhir_cm=xxxx",
        error.getMessage());
  }

  @Test
  void testCorrectlyValidatesKnownAndUnknownSystems() {

    assertTrue(
        terminologyService.validate("http://snomed.info/sct?fhir_vs=refset/32570521000036109",
            CD_SNOMED_284551006)
    );

    assertTrue(
        terminologyService.validate("http://snomed.info/sct?fhir_vs=refset/32570521000036109",
            CD_SNOMED_VER_403190006));

    assertFalse(
        terminologyService.validate("http://snomed.info/sct?fhir_vs=refset/32570521000036109",
            CD_SNOMED_72940011000036107)
    );

    assertFalse(
        terminologyService.validate("http://snomed.info/sct?fhir_vs=refset/32570521000036109",
            CD_AST_VIC)
    );

    assertFalse(
        terminologyService.validate("http://snomed.info/sct?fhir_vs=refset/32570521000036109",
            UNKNOWN_SYSTEM_CODING)
    );

    // TODO: This throws an 404 exception because the specific version of a known system cannot be found. 
    // Why it's not treated the same as an unknown system (like in translate?)
    // assertFalse(
    //     terminologyService.validate("http://snomed.info/sct?fhir_vs=refset/32570521000036109",
    //         CD_SNOMED_403190006_VERSION_UNKN)
    // );
  }

  @Test
  void testCorrectlySubsumesKnownAndUnknownSystems() {

    assertEquals(ConceptSubsumptionOutcome.SUBSUMES,
        terminologyService.subsumes(CD_SNOMED_107963000, CD_SNOMED_63816008)
    );

    assertEquals(ConceptSubsumptionOutcome.SUBSUMES,
        terminologyService.subsumes(CD_SNOMED_107963000, CD_SNOMED_VER_63816008)
    );

    assertEquals(ConceptSubsumptionOutcome.NOTSUBSUMED,
        terminologyService.subsumes(CD_SNOMED_107963000, UNKNOWN_SYSTEM_CODING)
    );

    // TODO: This throws an 404 exception because the specific version of a known system cannot be found. 
    // Whey it's not treated the same as an unknown system (like with translate)
    // assertEquals(ConceptSubsumptionOutcome.NOTSUBSUMED,
    //     terminologyService.subsumes(CD_SNOMED_107963000, CD_SNOMED_403190006_VERSION_UNKN)
    // );

    // TODO: This is the same coding but with different version and we cannot test for it
    // assertEquals(ConceptSubsumptionOutcome.EQUIVALENT,
    //     terminologyService.subsumes(CD_SNOMED_VER_63816008, CD_SNOMED_63816008_VER2022)
    // );
  }

  @Test
  void testUserAgentHeader() {
    terminologyService.validate(SNOMED_URI + "?fhir_vs", CD_SNOMED_284551006);
    verify(anyRequestedFor(urlPathMatching("/fhir/(.*)"))
        .withHeader("User-Agent", matching("pathling/(.*)")));
  }


  @Test
  void testLookupStandardPropertiesForKnownAndUnknownSystems() {
    assertEquals(
        List.of(Property.of("display", new StringType("Left hepatectomy"))),
        terminologyService.lookup(CD_SNOMED_VER_63816008, "display", null));

    assertEquals(
        List.of(Property.of("code", new CodeType("55915-3"))),
        terminologyService.lookup(LC_55915_3, "code", "en"));

    // TODO: Unexpected: ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException: HTTP 404 Not Found: [0dbaeea4-1bcc-40c0-b7b1-61fe4b4e188a]: A usable code system with URL uuid:unknown could not be resolved.

    // assertEquals(
    //     Collections.emptyList(),
    //     terminologyService.lookup(UNKNOWN_SYSTEM_CODING, "display", null));

    // TODO: ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException: HTTP 404 Not Found: [410fa0b5-50c4-42ba-a4f4-6e952d43a46f]: A usable code system with URL http://snomed.info/sct|http://snomed.info/sct/32506021000036107/version/19000101 could not be resolved.
    // assertEquals(
    //     Collections.emptyList(),
    //     terminologyService.lookup(CD_SNOMED_403190006_VERSION_UNKN, "display", null));
  }

}
