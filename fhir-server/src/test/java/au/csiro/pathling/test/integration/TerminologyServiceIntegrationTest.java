/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.integration;

import static au.csiro.pathling.test.assertions.Assertions.assertMatches;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.ALL_EQUIVALENCES;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.CD_AST_VIC;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.CD_SNOMED_107963000;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.CD_SNOMED_284551006;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.CD_SNOMED_720471000168102;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.CD_SNOMED_72940011000036107;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.CD_SNOMED_VER_107963000;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.CD_SNOMED_VER_284551006;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.CD_SNOMED_VER_403190006;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.CD_SNOMED_VER_63816008;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.CM_HIST_ASSOCIATIONS;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.setOfSimpleFrom;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.simpleOf;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.snomedSimple;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.testSimple;
import static com.github.tomakehurst.wiremock.client.WireMock.proxyAllTo;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.TestPropertySource;

/**
 * @author Piotr Szul
 */
@TestPropertySource(properties = {
    "pathling.test.recording.terminologyServerUrl=https://tx.ontoserver.csiro.au/fhir",
    "pathling.terminology.serverUrl=http://localhost:" + 4072 + "/fhir"
})
@Tag("Tranche2")
@Slf4j
class TerminologyServiceIntegrationTest extends WireMockTest {

  @Autowired
  FhirContext fhirContext;

  @Autowired
  TerminologyService terminologyService;

  @MockBean
  UUIDFactory uuidFactory;

  @Value("${pathling.test.recording.terminologyServerUrl}")
  String recordingTxServerUrl;

  @BeforeEach
  @Override
  void setUp() {
    super.setUp();
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
  void testCorrectlyTranslatesKnownAndUnknownCodes() {

    final ConceptTranslator actualTranslation = terminologyService.translate(
        Arrays.asList(simpleOf(CD_SNOMED_72940011000036107), snomedSimple("444814009")),
        CM_HIST_ASSOCIATIONS, false, ALL_EQUIVALENCES);

    final ConceptTranslator expectedTranslation = ConceptTranslatorBuilder.empty()
        .put(CD_SNOMED_72940011000036107, CD_SNOMED_720471000168102)
        .build();
    assertEquals(expectedTranslation, actualTranslation);
  }

  @Test
  void testCorrectlyTranslatesInReverse() {

    final ConceptTranslator actualTranslation = terminologyService.translate(
        Arrays.asList(simpleOf(CD_SNOMED_720471000168102), snomedSimple("444814009")),
        CM_HIST_ASSOCIATIONS, true, ALL_EQUIVALENCES);

    final ConceptTranslator expectedTranslation = ConceptTranslatorBuilder.empty()
        .put(CD_SNOMED_720471000168102, CD_SNOMED_72940011000036107)
        .build();
    assertEquals(expectedTranslation, actualTranslation);
  }


  // TODO: Enable when fixed in terminology server, that is it does not accept ignore systems in
  //  codings.
  @Test
  @Disabled
  void testIgnoresUnknownSystems() {

    final ConceptTranslator actualTranslation = terminologyService.translate(
        Arrays.asList(testSimple("72940011000036107"), testSimple("444814009")),
        CM_HIST_ASSOCIATIONS, false, ALL_EQUIVALENCES);

    final ConceptTranslator expectedTranslation = ConceptTranslatorBuilder.empty().build();
    assertEquals(expectedTranslation, actualTranslation);
  }

  @Test
  void testFailsForUnknownConceptMap() {

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
  void testCorrectlyIntersectKnownAndUnknownSystems() {
    final Set<SimpleCoding> expansion = terminologyService
        .intersect("http://snomed.info/sct?fhir_vs=refset/32570521000036109",
            setOfSimpleFrom(CD_SNOMED_284551006, CD_SNOMED_VER_403190006,
                CD_SNOMED_72940011000036107, CD_AST_VIC,
                new Coding("uuid:unknown", "unknown", "Unknown")
            ));

    // TODO: Ask John - why the expansion is versioned if we include the CD_AST_VIC, but unversioned
    //  otherwise? As this will affect the functioning of memberOf (since it uses SimpleCoding
    //  equality). Also if two versioned SNOMED codings are requested the response contains their
    //  unversioned versions.
    assertEquals(setOfSimpleFrom(CD_SNOMED_VER_284551006, CD_SNOMED_VER_403190006), expansion);
  }


  @Test
  void testCorrectlyBuildsClosureKnownAndUnknownSystems() {
    when(uuidFactory.nextUUID())
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
