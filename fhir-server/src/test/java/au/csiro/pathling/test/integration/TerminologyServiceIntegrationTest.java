/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.integration;

import static au.csiro.pathling.test.assertions.Assertions.assertMatches;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.*;
import static com.github.tomakehurst.wiremock.client.WireMock.proxyAllTo;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import au.csiro.pathling.fhir.DefaultTerminologyServiceFactory;
import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import au.csiro.pathling.terminology.ConceptTranslator;
import au.csiro.pathling.terminology.Relation;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.UUIDFactory;
import au.csiro.pathling.test.fixtures.ConceptTranslatorBuilder;
import au.csiro.pathling.test.fixtures.RelationBuilder;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import com.github.tomakehurst.wiremock.recording.RecordSpecBuilder;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Coding;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.TestPropertySource;

/**
 * @author Piotr Szul
 */
@TestPropertySource(properties = {
    "live.terminology.serverBaseUrl=https://r4.ontoserver.csiro.au/",
    "pathling.terminology.serverUrl=http://localhost:" + 4072 + "/fhir"
})
@Slf4j
class TerminologyServiceIntegrationTest extends WireMockTest {

  private static boolean isRecordMode() {
    return Boolean.parseBoolean(System.getProperty("WireMockTest.recordMappings", "false"));
  }

  @Autowired
  private FhirContext fhirContext;

  @Value("${live.terminology.serverBaseUrl}")
  private String liveTerminologyServerBaseUrl;

  @Value("${pathling.terminology.serverUrl}")
  private String terminologyServerUrl;

  private TerminologyService terminologyService;

  private UUIDFactory mockUUIDFactory;

  @BeforeEach
  @Override
  public void setUp() {
    super.setUp();
    if (isRecordMode()) {
      wireMockServer.resetAll();
      log.warn("Proxying all request to: {}", liveTerminologyServerBaseUrl);
      stubFor(proxyAllTo(liveTerminologyServerBaseUrl));
    }

    mockUUIDFactory = Mockito.mock(UUIDFactory.class);
    // TODO: refactor to use actual dependency injection
    // requires possible refactoring of test contexts.
    final DefaultTerminologyServiceFactory tcf = new DefaultTerminologyServiceFactory(fhirContext,
        terminologyServerUrl, 0, false);
    terminologyService = tcf.buildService(log, mockUUIDFactory);
  }

  @AfterEach
  @Override
  public void tearDown() {
    if (isRecordMode()) {
      log.warn("Recording snapshots to: {}", wireMockServer.getOptions().filesRoot());
      wireMockServer
          .snapshotRecord(new RecordSpecBuilder().matchRequestBodyWithEqualToJson(true, false));
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
        CM_HIST_ASSOCIATIONS, false, ALL_EQUIVALENCES);

    final ConceptTranslator expectedTranslation = ConceptTranslatorBuilder.empty().build();
    assertEquals(expectedTranslation, actualTranslation);
  }

  @Test
  public void testFailsForUnknownConceptMap() {

    final ResourceNotFoundException error = assertThrows(ResourceNotFoundException.class,
        () -> terminologyService.translate(
            Arrays.asList(simpleOf(CD_SNOMED_72940011000036107), snomedSimple("444814009")),
            "http://snomed.info/sct?fhir_cm=xxxx", false,
            ALL_EQUIVALENCES));

    assertMatches(
        "Error in response entry : HTTP 404 : "
            + "\\[.+\\]: "
            + "Unable to find ConceptMap with URI http://snomed\\.info/sct\\?fhir_cm=xxxx",
        error.getMessage());
  }

  @Test
  public void testCorrectlyIntersectKnownAndUnknownSystems() {
    final Set<SimpleCoding> expansion = terminologyService
        .intersect("http://snomed.info/sct?fhir_vs=refset/32570521000036109",
            setOfSimpleFrom(CD_SNOMED_284551006, CD_SNOMED_VER_403190006,
                CD_SNOMED_72940011000036107, CD_AST_VIC,
                new Coding("uuid:unknown", "unknown", "Unknown")
            ));

    // TODO: Ask John - why the expansion is versioned if we include the CD_AST_VIC, but
    // unversioned otherwise? As this will affect the functioning of memberOf (since it uses
    // SimpleCoding equality).
    // Also if two versioned SNOMED codings are requested the response contains
    // their unversioned versions.
    assertEquals(setOfSimpleFrom(CD_SNOMED_VER_284551006, CD_SNOMED_VER_403190006), expansion);
  }


  @Test
  public void testCorrectlyBuildsClosureKnownAndUnknownSystems() {

    when(mockUUIDFactory.nextUUID())
        .thenReturn(UUID.fromString("5d1b976d-c50c-445a-8030-64074b83f355"));
    final Relation actualRelation = terminologyService
        .getSubsumesRelation(
            setOfSimpleFrom(CD_SNOMED_107963000, CD_SNOMED_VER_63816008,
                CD_SNOMED_72940011000036107, CD_AST_VIC,
                new Coding("uuid:unknown", "unknown", "Unknown")
            ));
    // It appears that in the response all codings are versioned regardless
    // of whether the version was present in the request
    final Relation expectedRelation = RelationBuilder.empty()
        .add(CD_SNOMED_VER_107963000, CD_SNOMED_VER_63816008).build();
    assertEquals(expectedRelation, actualRelation);
  }
}