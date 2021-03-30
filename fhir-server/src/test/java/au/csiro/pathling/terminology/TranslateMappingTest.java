/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.terminology;


import static au.csiro.pathling.test.helpers.TestHelpers.getResourceAsStream;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.errors.UnexpectedResponseException;
import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import au.csiro.pathling.test.fixtures.ConceptTranslatorBuilder;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

@Tag("UnitTest")
public class TranslateMappingTest extends MappingTest {

  @Autowired
  protected FhirContext fhirContext;

  private static final String CONCEPT_MAP_URL_1 = "http://snomed.info/sct?fhir_cm=1";
  private static final String CONCEPT_MAP_URL_2 = "http://snomed.info/sct?fhir_cm=2";

  private static final SimpleCoding SIMPLE_CODING_1 = new SimpleCoding("uuid:system1", "code1");
  private static final SimpleCoding SIMPLE_CODING_2 = new SimpleCoding("uuid:system2", "code2",
      "12");
  private static final SimpleCoding SIMPLE_CODING_3 = new SimpleCoding("uuid:system3", "code3");


  private static final Coding CODING_1_WIDER = new Coding("http://snomed.info/sct",
      "wider-1",
      "Wider 1");

  private static final Coding CODING_1_EQUIVALENT = new Coding("http://snomed.info/sct",
      "equivalent-1",
      "Equivalent 1");

  private static final Coding CODING_2_EQUIVALENT = new Coding("http://snomed.info/sct",
      "equivalent-2",
      "Equivalent 2");

  @Test
  public void testToBundleEmpty() {
    final Bundle requestBundle = TranslateMapping
        .toRequestBundle(Collections.emptyList(), CONCEPT_MAP_URL_1,
            false);
    assertRequest(requestBundle);
  }

  @Test
  public void testToBundleForward() {
    final Bundle requestBundle = TranslateMapping
        .toRequestBundle(Arrays.asList(SIMPLE_CODING_1, SIMPLE_CODING_2), CONCEPT_MAP_URL_1,
            false);
    assertRequest(requestBundle);
  }

  @Test
  public void testToBundleReverse() {
    final Bundle requestBundle = TranslateMapping
        .toRequestBundle(
            Arrays.asList(SIMPLE_CODING_2, SIMPLE_CODING_1), CONCEPT_MAP_URL_2,
            true);
    assertRequest(requestBundle);
  }

  @Test
  public void testFromBundleWhenResponseHasMappings() {
    final Bundle responseBundle = (Bundle) jsonParser.parseResource(
        getResourceAsStream("txResponses/TranslateMappingTest/responseWithMappings3.Bundle.json"));

    final List<SimpleCoding> inputCodings = Arrays
        .asList(SIMPLE_CODING_1, SIMPLE_CODING_2, SIMPLE_CODING_3);

    // TC-1 Not matching equivalences
    final ConceptTranslator conceptTranslatorEmpty = TranslateMapping
        .fromResponseBundle(responseBundle, inputCodings,
            Collections.singletonList(ConceptMapEquivalence.INEXACT), fhirContext);
    assertEquals(new ConceptTranslator(), conceptTranslatorEmpty,
        "TC-1: Not matching equivalences");

    // TC-2 All equivalences match

    final ConceptTranslator conceptTranslatorAll = TranslateMapping
        .fromResponseBundle(responseBundle, inputCodings,
            Arrays.asList(ConceptMapEquivalence.values()), fhirContext);

    final ConceptTranslator expectedConcepMapperAll = ConceptTranslatorBuilder.empty()
        .put(SIMPLE_CODING_1, CODING_1_EQUIVALENT, CODING_1_WIDER)
        .put(SIMPLE_CODING_2, CODING_2_EQUIVALENT)
        .build();
    assertEquals(expectedConcepMapperAll, conceptTranslatorAll, "TC-2: All equivalences match");

    // TC-3 Selected equivalences match

    final ConceptTranslator conceptTranslatorSelect = TranslateMapping
        .fromResponseBundle(responseBundle, inputCodings,
            Collections.singletonList(ConceptMapEquivalence.WIDER), fhirContext);

    final ConceptTranslator expectedConcepMapperSelect = ConceptTranslatorBuilder.empty()
        .put(SIMPLE_CODING_1, CODING_1_WIDER)
        .build();
    assertEquals(expectedConcepMapperSelect, conceptTranslatorSelect,
        "TC-3: Selected equivalences match");
  }

  @Test
  public void testFromBundleWhenResponseWithNoMappings() {
    final Bundle responseBundle = (Bundle) jsonParser.parseResource(
        getResourceAsStream("txResponses/TranslateMappingTest/noMappingsResponse2.Bundle.json"));

    final List<SimpleCoding> inputCodings = Arrays
        .asList(SIMPLE_CODING_1, SIMPLE_CODING_2);

    // TC-4 Not mappins in response
    final ConceptTranslator conceptTranslatorEmpty = TranslateMapping
        .fromResponseBundle(responseBundle, inputCodings,
            Collections.singletonList(ConceptMapEquivalence.INEXACT), fhirContext);
    assertEquals(new ConceptTranslator(), conceptTranslatorEmpty, "TC-4: No mappings in response");
  }

  @Test
  public void throwsErrorIfResponsBundleSizeWrong() {

    final Bundle responseBundle = (Bundle) jsonParser.parseResource(
        getResourceAsStream("txResponses/TranslateMappingTest/responseWithMappings3.Bundle.json"));

    // Response bundle has three entries
    final UnexpectedResponseException error = assertThrows(UnexpectedResponseException.class,
        () -> TranslateMapping
            .fromResponseBundle(responseBundle, Arrays
                    .asList(SIMPLE_CODING_1, SIMPLE_CODING_2),
                Collections.emptyList(), fhirContext));
    assertEquals(
        "The size of the response bundle: 2 does not match the size of the request bundle: 3",
        error.getMessage());
  }

  @Test
  public void throwsErrorIfAnyEntryHasError() {

    final Bundle responseBundle = (Bundle) jsonParser.parseResource(
        getResourceAsStream("txResponses/TranslateMappingTest/responseWith404.Bundle.json"));

    // Response bundle has 2 entries
    final ResourceNotFoundException error = assertThrows(ResourceNotFoundException.class,
        () -> TranslateMapping
            .fromResponseBundle(responseBundle, Arrays
                    .asList(SIMPLE_CODING_1, SIMPLE_CODING_2),
                Collections.emptyList(), fhirContext));
    assertEquals(
        "Error in response entry : HTTP 404 : [ed835929-8734-4a4a-b4ed-8614f2d46321]: Unable to find ConceptMap with URI http://snomed.info/sct?fhir_cm=xxxx",
        error.getMessage());
  }


  @Test
  public void throwsErrorIfWrongBundleType() {

    final Bundle responseBundle = (Bundle) jsonParser.parseResource(
        getResourceAsStream("txResponses/TranslateMappingTest/noMappingsResponse2.Bundle.json"));

    // set one entry status to error
    responseBundle.setType(BundleType.BATCH);

    // Response bundle has three entries
    final UnexpectedResponseException error = assertThrows(UnexpectedResponseException.class,
        () -> TranslateMapping
            .fromResponseBundle(responseBundle, Arrays
                    .asList(SIMPLE_CODING_1, SIMPLE_CODING_2),
                Collections.emptyList(), fhirContext));
    assertEquals(
        "Expected bundle type 'batch-response' but got: 'batch'",
        error.getMessage());
  }
}
