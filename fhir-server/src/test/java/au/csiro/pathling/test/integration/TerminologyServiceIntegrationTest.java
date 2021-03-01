/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.integration;

import static au.csiro.pathling.test.helpers.TerminologyHelpers.*;
import static com.github.tomakehurst.wiremock.client.WireMock.proxyAllTo;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.errors.MalformedResponseException;
import au.csiro.pathling.fhir.DefaultTerminologyClientFactory;
import au.csiro.pathling.fhir.TerminologyClientFactory;
import au.csiro.pathling.terminology.ConceptTranslator;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.test.fixtures.ConceptTranslatorBuilder;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.TestPropertySource;

/**
 * @author Piotr Szul
 */
@TestPropertySource(properties = {
    "live.terminology.serveBaseUrl=https://r4.ontoserver.csiro.au/",
    "pathling.terminology.serverUrl=http://localhost:" + 4072 + "/fhir"
})
@Slf4j
class TerminologyServiceIntegrationTest extends WireMockTest {

  public static boolean isRecordMode() {
    return Boolean.parseBoolean(System.getProperty("WireMockTest.recordMappings", "false"));
  }

  @Autowired
  private FhirContext fhirContext;

  @Autowired
  private IParser jsonParser;

  @Value("${live.terminology.serveBaseUrl}")
  private String liveTerminologServerBaseUrl;

  @Value("${pathling.terminology.serverUrl}")
  private String terminologServerUrl;

  private TerminologyService terminologyService;

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    if (isRecordMode()) {
      wireMockServer.resetAll();
      log.warn("Proxying all request to: {}", liveTerminologServerBaseUrl);
      stubFor(proxyAllTo(liveTerminologServerBaseUrl));
    }

    // TODO: refactor to use actual dependency injection
    final TerminologyClientFactory tcf = new DefaultTerminologyClientFactory(fhirContext,
        terminologServerUrl, 0, false);
    terminologyService = tcf.buildService(log);
  }

  @AfterEach
  @Override
  public void tearDown() {
    if (isRecordMode()) {
      log.warn("Recording snapshots to: {}", wireMockServer.getOptions().filesRoot());
      wireMockServer.snapshotRecord();
    }
    super.tearDown();
  }

  @Test
  public void testCorrectlyTranslatesKnownAndUnknownCodes() {

    final ConceptTranslator actualTranslation = terminologyService.translate(
        Arrays.asList(simpleOf(CD_SNOMED_72940011000036107), snomedSimple("444814009")),
        CM_HIST_ASSOCIATIONS, false, ALL_EQUIVALENCES);

    final ConceptTranslator expectedTranslation = ConceptTranslatorBuilder.empty()
        .put(CD_SNOMED_72940011000036107, CD_SNOMED_720471000168102)
        .build();
    assertEquals(expectedTranslation, actualTranslation);
  }

  @Test
  public void testCorrectlyTranslatesInReverse() {

    final ConceptTranslator actualTranslation = terminologyService.translate(
        Arrays.asList(simpleOf(CD_SNOMED_720471000168102), snomedSimple("444814009")),
        CM_HIST_ASSOCIATIONS, true, ALL_EQUIVALENCES);

    final ConceptTranslator expectedTranslation = ConceptTranslatorBuilder.empty()
        .put(CD_SNOMED_720471000168102, CD_SNOMED_72940011000036107)
        .build();
    assertEquals(expectedTranslation, actualTranslation);
  }


  // TODO: enable when fixed in terminology server,
  // that is it does not accept ignore systems in codings.
  @Test
  @Disabled
  public void testIgnoresUnknownSystems() {

    final ConceptTranslator actualTranslation = terminologyService.translate(
        Arrays.asList(testSimple("72940011000036107"), testSimple("444814009")),
        CM_HIST_ASSOCIATIONS, false,
        ALL_EQUIVALENCES);

    final ConceptTranslator expectedTranslation = ConceptTranslatorBuilder.empty().build();
    assertEquals(expectedTranslation, actualTranslation);
  }

  @Test
  public void testFailsForUnknowConteptMap() {

    final MalformedResponseException error = assertThrows(MalformedResponseException.class,
        () -> terminologyService.translate(
            Arrays.asList(simpleOf(CD_SNOMED_72940011000036107), snomedSimple("444814009")),
            "http://snomed.info/sct?fhir_cm=xxxx", false,
            ALL_EQUIVALENCES));

    assertEquals(
        "Failed entry in response bundle with status: 404",
        error.getMessage());
  }
}