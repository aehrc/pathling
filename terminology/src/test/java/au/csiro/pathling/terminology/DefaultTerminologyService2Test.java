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

package au.csiro.pathling.terminology;

import static au.csiro.pathling.terminology.PropertiesParametersBuilder.standardProperties;
import static au.csiro.pathling.test.helpers.FhirMatchers.deepEq;
import static au.csiro.pathling.test.helpers.TerminologyServiceHelpers.OUTCOME_EQUIVALENT;
import static au.csiro.pathling.test.helpers.TerminologyServiceHelpers.OUTCOME_SUBSUMEDBY;
import static au.csiro.pathling.test.helpers.TerminologyServiceHelpers.OUTCOME_SUBSUMES;
import static au.csiro.pathling.test.helpers.TerminologyServiceHelpers.RESULT_FALSE;
import static au.csiro.pathling.test.helpers.TerminologyServiceHelpers.RESULT_TRUE;
import static org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome.EQUIVALENT;
import static org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome.NOTSUBSUMED;
import static org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome.SUBSUMEDBY;
import static org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome.SUBSUMES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import au.csiro.pathling.fhir.TerminologyClient2;
import au.csiro.pathling.terminology.TerminologyService2.Property;
import au.csiro.pathling.terminology.TerminologyService2.Translation;
import au.csiro.pathling.test.AbstractTerminologyTestBase;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.UriType;
import org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DefaultTerminologyService2Test extends AbstractTerminologyTestBase {

  private static final String VALUE_SET_X = "uuid:valueSetX";
  private static final String VALUE_SET_Y = "uuid:valueSetY";

  private static final String CONCEPT_MAP_0 = "uuid:conceptMap0";
  private static final String CONCEPT_MAP_1 = "uuid:conceptMap1";

  private static final List<Translation> EMPTY_TRANSLATION = Collections.emptyList();


  public static final Coding USE_PREFFERED_FOR_LANG = new Coding(
      "http://terminology.hl7.org/CodeSystem/hl7TermMaintInfra",
      "preferredForLanguage", "Preferred For Language"
  );

  @Nonnull
  private static Parameters translation(@Nonnull final Translation... entries) {
    final Parameters translateResponse = new Parameters()
        .setParameter("result", true);

    for (Translation entry : entries) {
      final ParametersParameterComponent matchParameter1 = translateResponse.addParameter()
          .setName("match");
      matchParameter1.addPart().setName("equivalence")
          .setValue(new CodeType(entry.getEquivalence().toCode()));
      matchParameter1.addPart().setName("concept").setValue(entry.getConcept());
    }
    return translateResponse;
  }


  private TerminologyClient2 terminologClient;
  private DefaultTerminologyService2 terminologyService;

  @BeforeEach
  void setUp() {
    terminologClient = mock(TerminologyClient2.class);
    terminologyService = new DefaultTerminologyService2(
        terminologClient, null);
  }

  @Test
  public void testValidateCodingTrue() {
    when(terminologClient.validateCode(
        deepEq(new UriType(VALUE_SET_X)),
        deepEq(new UriType(SYSTEM_A)),
        isNull(),
        deepEq(new CodeType(CODE_A))
    )).thenReturn(RESULT_TRUE);
    assertTrue(terminologyService.validateCode(VALUE_SET_X, CODING_AA));
  }

  @Test
  public void testValidateVersionedCodingFalse() {
    when(terminologClient.validateCode(
        deepEq(new UriType(VALUE_SET_Y)),
        deepEq(new UriType(SYSTEM_B)),
        deepEq(new StringType(VERSION_1)),
        deepEq(new CodeType(CODE_B))
    )).thenReturn(RESULT_FALSE);
    assertFalse(terminologyService.validateCode(VALUE_SET_Y, CODING_BB_VERSION1));
  }

  @Test
  public void testValidateInvalidCodings() {
    assertFalse(terminologyService.validateCode(VALUE_SET_Y, INVALID_CODING_0));
    assertFalse(terminologyService.validateCode(VALUE_SET_Y, INVALID_CODING_1));
    assertFalse(terminologyService.validateCode(VALUE_SET_Y, INVALID_CODING_2));
  }

  @Test
  public void testSubsumesNoVersion() {
    when(terminologClient.subsumes(
        deepEq(new CodeType(CODE_A)),
        deepEq(new CodeType(CODE_B)),
        deepEq(new UriType(SYSTEM_A)),
        isNull()
    )).thenReturn(OUTCOME_SUBSUMES);
    assertEquals(SUBSUMES, terminologyService.subsumes(CODING_AA, CODING_AB));
  }

  @Test
  public void testSubsumesLeftVersion() {
    when(terminologClient.subsumes(
        deepEq(new CodeType(CODE_A)),
        deepEq(new CodeType(CODE_B)),
        deepEq(new UriType(SYSTEM_A)),
        deepEq(new StringType(VERSION_1))
    )).thenReturn(OUTCOME_EQUIVALENT);
    assertEquals(EQUIVALENT, terminologyService.subsumes(CODING_AA_VERSION1, CODING_AB));
  }

  @Test
  public void testSubsumesRightVersion() {
    when(terminologClient.subsumes(
        deepEq(new CodeType(CODE_A)),
        deepEq(new CodeType(CODE_B)),
        deepEq(new UriType(SYSTEM_A)),
        deepEq(new StringType(VERSION_2))
    )).thenReturn(OUTCOME_SUBSUMEDBY);
    assertEquals(SUBSUMEDBY, terminologyService.subsumes(CODING_AA, CODING_AB_VERSION2));
  }

  @Test
  public void testSubsumesBothVersionTheSame() {
    when(terminologClient.subsumes(
        deepEq(new CodeType(CODE_A)),
        deepEq(new CodeType(CODE_B)),
        deepEq(new UriType(SYSTEM_A)),
        deepEq(new StringType(VERSION_1))
    )).thenReturn(OUTCOME_EQUIVALENT);
    assertEquals(EQUIVALENT, terminologyService.subsumes(CODING_AA_VERSION1, CODING_AB_VERSION1));
  }

  @Test
  public void testSubsumesDifferentVersions() {
    assertEquals(NOTSUBSUMED, terminologyService.subsumes(CODING_AA_VERSION1, CODING_AB_VERSION2));
    verifyNoMoreInteractions(terminologClient);
  }

  @Test
  public void testSubsumesDifferentSystems() {
    assertEquals(NOTSUBSUMED, terminologyService.subsumes(CODING_AA, CODING_BB_VERSION1));
    verifyNoMoreInteractions(terminologClient);
  }

  @Test
  public void testSubsumesInvalidCodings() {
    assertEquals(NOTSUBSUMED, terminologyService.subsumes(INVALID_CODING_0, INVALID_CODING_0));
    assertEquals(NOTSUBSUMED, terminologyService.subsumes(INVALID_CODING_1, INVALID_CODING_1));
    assertEquals(NOTSUBSUMED, terminologyService.subsumes(INVALID_CODING_2, INVALID_CODING_2));
    assertEquals(NOTSUBSUMED, terminologyService.subsumes(INVALID_CODING_0, CODING_AB));
    assertEquals(NOTSUBSUMED, terminologyService.subsumes(INVALID_CODING_1, CODING_AB));
    assertEquals(NOTSUBSUMED, terminologyService.subsumes(INVALID_CODING_2, CODING_AB));
    assertEquals(NOTSUBSUMED, terminologyService.subsumes(CODING_BB_VERSION1, INVALID_CODING_0));
    assertEquals(NOTSUBSUMED, terminologyService.subsumes(CODING_BB_VERSION1, INVALID_CODING_1));
    assertEquals(NOTSUBSUMED, terminologyService.subsumes(CODING_BB_VERSION1, INVALID_CODING_2));
    assertEquals(NOTSUBSUMED, terminologyService.subsumes(INVALID_CODING_0, INVALID_CODING_1));
    assertEquals(NOTSUBSUMED, terminologyService.subsumes(INVALID_CODING_1, INVALID_CODING_2));
    assertEquals(NOTSUBSUMED, terminologyService.subsumes(INVALID_CODING_2, INVALID_CODING_0));
    verifyNoMoreInteractions(terminologClient);
  }


  @Test
  public void testTranslatesVersionedCodingWithDefaults() {

    when(terminologClient.translate(
        deepEq(new UriType(CONCEPT_MAP_0)),
        deepEq(new UriType(SYSTEM_A)),
        deepEq(new StringType(VERSION_1)),
        deepEq(new CodeType(CODE_A)),
        deepEq(new BooleanType(false)),
        isNull()
    )).thenReturn(RESULT_FALSE);

    assertEquals(EMPTY_TRANSLATION,
        terminologyService.translate(CODING_AA_VERSION1, CONCEPT_MAP_0, false, null));
  }

  @Test
  public void testTranslatesUnversionedCoding() {

    final Parameters translationResponse = translation(
        Translation.of(ConceptMapEquivalence.RELATEDTO, CODING_AA),
        Translation.of(ConceptMapEquivalence.EQUIVALENT, CODING_AB),
        Translation.of(ConceptMapEquivalence.SUBSUMES, CODING_AA_VERSION1),
        Translation.of(ConceptMapEquivalence.NARROWER, CODING_AB_VERSION1)
    );

    when(terminologClient.translate(
        deepEq(new UriType(CONCEPT_MAP_1)),
        deepEq(new UriType(SYSTEM_B)),
        isNull(),
        deepEq(new CodeType(CODE_B)),
        deepEq(new BooleanType(true)),
        deepEq(new UriType(SYSTEM_A))
    )).thenReturn(translationResponse);

    assertEquals(
        List.of(
            Translation.of(ConceptMapEquivalence.RELATEDTO, CODING_AA),
            Translation.of(ConceptMapEquivalence.EQUIVALENT, CODING_AB),
            Translation.of(ConceptMapEquivalence.SUBSUMES, CODING_AA_VERSION1),
            Translation.of(ConceptMapEquivalence.NARROWER, CODING_AB_VERSION1)
        ),
        terminologyService.translate(CODING_B, CONCEPT_MAP_1, true, SYSTEM_A));
  }

  @Test
  public void testTranslatesInvalidsCoding() {

    assertEquals(EMPTY_TRANSLATION,
        terminologyService.translate(INVALID_CODING_0, CONCEPT_MAP_0, false, null));
    assertEquals(EMPTY_TRANSLATION,
        terminologyService.translate(INVALID_CODING_1, CONCEPT_MAP_0, true, null));
    assertEquals(EMPTY_TRANSLATION,
        terminologyService.translate(INVALID_CODING_2, CONCEPT_MAP_0, false, SYSTEM_B));
    verifyNoMoreInteractions(terminologClient);
  }

  @Test
  public void testLooksUpInvalidCoding() {
    assertEquals(Collections.emptyList(),
        terminologyService.lookup(INVALID_CODING_0, null));
    assertEquals(Collections.emptyList(),
        terminologyService.lookup(INVALID_CODING_1, "display"));
    assertEquals(Collections.emptyList(),
        terminologyService.lookup(INVALID_CODING_2, "designation"));
    verifyNoMoreInteractions(terminologClient);
  }

  @Test
  public void testLooksUpStandardProperty() {

    when(terminologClient.lookup(
        deepEq(new UriType(SYSTEM_A)),
        isNull(),
        deepEq(new CodeType(CODE_A)),
        deepEq(new CodeType("display"))))
        .thenReturn(standardProperties(CODING_A).build());

    assertEquals(List.of(Property.of("display", new StringType(CODING_AA.getDisplay()))),
        terminologyService.lookup(CODING_AA, "display"));
  }

  @Test
  public void testLooksNamedProperties() {
    final Parameters response = standardProperties(CODING_BB_VERSION1)
        .withProperty("property_A", "string_value_a")
        .withProperty("property_A", new IntegerType(333))
        .withProperty("property_A", new CodeType("code_value_a"))
        .withProperty("property_A", new BooleanType(true))
        // adding some unexpected elemnts to make they are excluded
        .withProperty("property_B", "string_value_b")
        .withDesignation("en", USE_PREFFERED_FOR_LANG, "Coding BB")
        .build();

    when(terminologClient.lookup(
        deepEq(new UriType(SYSTEM_B)),
        deepEq(new StringType(VERSION_1)),
        deepEq(new CodeType(CODE_B)),
        deepEq(new CodeType("property_A"))
    )).thenReturn(response);

    assertEquals(List.of(
            Property.of("property_A", new StringType("string_value_a")),
            Property.of("property_A", new IntegerType(333)),
            Property.of("property_A", new CodeType("code_value_a")),
            Property.of("property_A", new BooleanType(true))
        ),
        terminologyService.lookup(CODING_BB_VERSION1, "property_A"));
  }

  @Test
  public void testLooksUpSupProperties() {
    final Parameters response = standardProperties(CODING_C)
        .withPropertyGroup("group_C")
        .withSubProperty("property_C", new StringType("string_value_c"))
        .withSubProperty("property_C", new CodeType("code_value_c"))
        .withSubProperty("property_D", new StringType("string_value_D"))
        .build();

    when(terminologClient.lookup(
        deepEq(new UriType(SYSTEM_C)),
        isNull(),
        deepEq(new CodeType(CODE_C)),
        any()
    )).thenReturn(response);

    assertEquals(List.of(
            Property.of("property_C", new StringType("string_value_c")),
            Property.of("property_C", new CodeType("code_value_c"))
        ),
        terminologyService.lookup(CODING_C, "property_C"));
    // does not include grouping property in the results
    assertEquals(Collections.emptyList(),
        terminologyService.lookup(CODING_C, "group_C"));
  }

  @Test
  public void testLookupHandles404Exceptions() {
    when(terminologClient.lookup(any(), any(), any(), any()
    )).thenThrow(BaseServerResponseException.newInstance(404, "Resource Not Found"));

    assertEquals(Collections.emptyList(),
        terminologyService.lookup(CODING_C, "property_A"));
  }
}
