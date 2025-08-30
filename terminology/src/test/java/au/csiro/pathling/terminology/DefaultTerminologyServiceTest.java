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

package au.csiro.pathling.terminology;

import static au.csiro.pathling.terminology.PropertiesParametersBuilder.standardProperties;
import static au.csiro.pathling.test.helpers.FhirMatchers.deepEq;
import static au.csiro.pathling.test.helpers.TerminologyServiceHelpers.OUTCOME_EQUIVALENT;
import static au.csiro.pathling.test.helpers.TerminologyServiceHelpers.OUTCOME_SUBSUMED_BY;
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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.terminology.TerminologyService.Designation;
import au.csiro.pathling.terminology.TerminologyService.Property;
import au.csiro.pathling.terminology.TerminologyService.Translation;
import au.csiro.pathling.test.AbstractTerminologyTestBase;
import ca.uhn.fhir.rest.gclient.IOperationUntypedWithInput;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import jakarta.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
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

public class DefaultTerminologyServiceTest extends AbstractTerminologyTestBase {

  private static final String VALUE_SET_X = "uuid:valueSetX";
  private static final String VALUE_SET_Y = "uuid:valueSetY";

  private static final String CONCEPT_MAP_0 = "uuid:conceptMap0";
  private static final String CONCEPT_MAP_1 = "uuid:conceptMap1";

  private static final List<Translation> EMPTY_TRANSLATION = Collections.emptyList();

  public static final Coding USE_PREFERRED_FOR_LANG = new Coding(
      "http://terminology.hl7.org/CodeSystem/hl7TermMaintInfra",
      "preferredForLanguage", "Preferred For Language"
  );

  @Nonnull
  private static Parameters translation(@Nonnull final Translation... entries) {
    final Parameters translateResponse = new Parameters()
        .setParameter("result", true);

    for (final Translation entry : entries) {
      final ParametersParameterComponent matchParameter1 = translateResponse.addParameter()
          .setName("match");
      matchParameter1.addPart().setName("equivalence")
          .setValue(new CodeType(entry.getEquivalence().toCode()));
      matchParameter1.addPart().setName("concept").setValue(entry.getConcept());
    }
    return translateResponse;
  }


  private TerminologyClient terminologyClient;
  private DefaultTerminologyService terminologyService;

  @BeforeEach
  void setUp() {
    terminologyClient = mock(TerminologyClient.class);
    terminologyService = new DefaultTerminologyService(
        terminologyClient);
  }

  @Test
  void testValidateCodingTrue() {
    final IOperationUntypedWithInput<Parameters> request = mockRequest(
        RESULT_TRUE);
    when(terminologyClient.buildValidateCode(
        deepEq(new UriType(VALUE_SET_X)),
        deepEq(new UriType(SYSTEM_A)),
        isNull(),
        deepEq(new CodeType(CODE_A))
    )).thenReturn(request);
    assertTrue(terminologyService.validateCode(VALUE_SET_X, CODING_AA));
  }

  @Test
  void testValidateVersionedCodingFalse() {
    final IOperationUntypedWithInput<Parameters> request = mockRequest(
        RESULT_FALSE);
    when(terminologyClient.buildValidateCode(
        deepEq(new UriType(VALUE_SET_Y)),
        deepEq(new UriType(SYSTEM_B)),
        deepEq(new StringType(VERSION_1)),
        deepEq(new CodeType(CODE_B))
    )).thenReturn(request);
    assertFalse(terminologyService.validateCode(VALUE_SET_Y, CODING_BB_VERSION1));
  }

  @Test
  void testValidateInvalidCodings() {
    assertFalse(terminologyService.validateCode(VALUE_SET_Y, INVALID_CODING_0));
    assertFalse(terminologyService.validateCode(VALUE_SET_Y, INVALID_CODING_1));
    assertFalse(terminologyService.validateCode(VALUE_SET_Y, INVALID_CODING_2));
  }

  @Test
  void testSubsumesNoVersion() {
    final IOperationUntypedWithInput<Parameters> request = mockRequest(
        OUTCOME_SUBSUMES);
    when(terminologyClient.buildSubsumes(
        deepEq(new CodeType(CODE_A)),
        deepEq(new CodeType(CODE_B)),
        deepEq(new UriType(SYSTEM_A)),
        isNull()
    )).thenReturn(request);
    assertEquals(SUBSUMES, terminologyService.subsumes(CODING_AA, CODING_AB));
  }

  @Test
  void testSubsumesLeftVersion() {
    final IOperationUntypedWithInput<Parameters> request = mockRequest(
        OUTCOME_EQUIVALENT);
    when(terminologyClient.buildSubsumes(
        deepEq(new CodeType(CODE_A)),
        deepEq(new CodeType(CODE_B)),
        deepEq(new UriType(SYSTEM_A)),
        deepEq(new StringType(VERSION_1))
    )).thenReturn(request);
    assertEquals(EQUIVALENT, terminologyService.subsumes(CODING_AA_VERSION1, CODING_AB));
  }

  @Test
  void testSubsumesRightVersion() {
    final IOperationUntypedWithInput<Parameters> request = mockRequest(
        OUTCOME_SUBSUMED_BY);
    when(terminologyClient.buildSubsumes(
        deepEq(new CodeType(CODE_A)),
        deepEq(new CodeType(CODE_B)),
        deepEq(new UriType(SYSTEM_A)),
        deepEq(new StringType(VERSION_2))
    )).thenReturn(request);
    assertEquals(SUBSUMEDBY, terminologyService.subsumes(CODING_AA, CODING_AB_VERSION2));
  }

  @Test
  void testSubsumesBothVersionTheSame() {
    final IOperationUntypedWithInput<Parameters> request = mockRequest(
        OUTCOME_EQUIVALENT);
    when(terminologyClient.buildSubsumes(
        deepEq(new CodeType(CODE_A)),
        deepEq(new CodeType(CODE_B)),
        deepEq(new UriType(SYSTEM_A)),
        deepEq(new StringType(VERSION_1))
    )).thenReturn(request);
    assertEquals(EQUIVALENT, terminologyService.subsumes(CODING_AA_VERSION1, CODING_AB_VERSION1));
  }

  @Test
  void testSubsumesDifferentVersions() {
    assertEquals(NOTSUBSUMED, terminologyService.subsumes(CODING_AA_VERSION1, CODING_AB_VERSION2));
    verifyNoMoreInteractions(terminologyClient);
  }

  @Test
  void testSubsumesDifferentSystems() {
    assertEquals(NOTSUBSUMED, terminologyService.subsumes(CODING_AA, CODING_BB_VERSION1));
    verifyNoMoreInteractions(terminologyClient);
  }

  @Test
  void testSubsumesInvalidCodings() {
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
    verifyNoMoreInteractions(terminologyClient);
  }


  @Test
  void testSubsumesEqualCodingsLocally() {
    assertEquals(EQUIVALENT, terminologyService.subsumes(CODING_B, CODING_B));
    assertEquals(EQUIVALENT, terminologyService.subsumes(CODING_AA, CODING_AA_VERSION1));
    assertEquals(EQUIVALENT, terminologyService.subsumes(CODING_AB_VERSION1, CODING_AB));
    assertEquals(EQUIVALENT, terminologyService.subsumes(CODING_AB_VERSION2, CODING_AB_VERSION2));
    assertEquals(EQUIVALENT, terminologyService.subsumes(CODING_AA, CODING_AA_DISPLAY1));
    verifyNoMoreInteractions(terminologyClient);
  }


  @Test
  void testTranslatesVersionedCodingWithDefaults() {
    final IOperationUntypedWithInput<Parameters> request = mockRequest(
        RESULT_FALSE);
    when(terminologyClient.buildTranslate(
        deepEq(new UriType(CONCEPT_MAP_0)),
        deepEq(new UriType(SYSTEM_A)),
        deepEq(new StringType(VERSION_1)),
        deepEq(new CodeType(CODE_A)),
        deepEq(new BooleanType(false)),
        isNull()
    )).thenReturn(request);

    assertEquals(EMPTY_TRANSLATION,
        terminologyService.translate(CODING_AA_VERSION1, CONCEPT_MAP_0, false, null));
  }

  @Test
  void testTranslatesUnversionedCoding() {

    final Parameters translationResponse = translation(
        Translation.of(ConceptMapEquivalence.RELATEDTO, CODING_AA),
        Translation.of(ConceptMapEquivalence.EQUIVALENT, CODING_AB),
        Translation.of(ConceptMapEquivalence.SUBSUMES, CODING_AA_VERSION1),
        Translation.of(ConceptMapEquivalence.NARROWER, CODING_AB_VERSION1)
    );
    final IOperationUntypedWithInput<Parameters> request = mockRequest(translationResponse);

    when(terminologyClient.buildTranslate(
        deepEq(new UriType(CONCEPT_MAP_1)),
        deepEq(new UriType(SYSTEM_B)),
        isNull(),
        deepEq(new CodeType(CODE_B)),
        deepEq(new BooleanType(true)),
        deepEq(new UriType(SYSTEM_A))
    )).thenReturn(request);

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
  void testTranslatesInvalidsCoding() {

    assertEquals(EMPTY_TRANSLATION,
        terminologyService.translate(INVALID_CODING_0, CONCEPT_MAP_0, false, null));
    assertEquals(EMPTY_TRANSLATION,
        terminologyService.translate(INVALID_CODING_1, CONCEPT_MAP_0, true, null));
    assertEquals(EMPTY_TRANSLATION,
        terminologyService.translate(INVALID_CODING_2, CONCEPT_MAP_0, false, SYSTEM_B));
    verifyNoMoreInteractions(terminologyClient);
  }

  @Test
  void testLooksUpInvalidCoding() {
    assertEquals(Collections.emptyList(),
        terminologyService.lookup(INVALID_CODING_0, null));
    assertEquals(Collections.emptyList(),
        terminologyService.lookup(INVALID_CODING_1, "display"));
    assertEquals(Collections.emptyList(),
        terminologyService.lookup(INVALID_CODING_2, "designation"));
    verifyNoMoreInteractions(terminologyClient);
  }

  @Test
  void testLooksUpStandardProperty() {

    final IOperationUntypedWithInput<Parameters> request = mockRequest(
        standardProperties(CODING_A).build());
    when(terminologyClient.buildLookup(
        deepEq(new UriType(SYSTEM_A)),
        isNull(),
        deepEq(new CodeType(CODE_A)),
        deepEq(new CodeType("display")),
        deepEq(new StringType("xx-XX"))))
        .thenReturn(request);

    assertEquals(List.of(Property.of("display", new StringType(CODING_AA.getDisplay()))),
        terminologyService.lookup(CODING_AA, "display", "xx-XX"));
  }

  @Test
  void testLooksNamedProperties() {
    final Parameters response = standardProperties(CODING_BB_VERSION1)
        .withProperty("property_A", "string_value_a")
        .withProperty("property_A", new IntegerType(333))
        .withProperty("property_A", new CodeType("code_value_a"))
        .withProperty("property_A", new BooleanType(true))
        // Adding some unexpected elements to make they are excluded.
        .withProperty("property_B", "string_value_b")
        .withDesignation("Coding BB", USE_PREFERRED_FOR_LANG, "en")
        .build();

    final IOperationUntypedWithInput<Parameters> request = mockRequest(response);
    when(terminologyClient.buildLookup(
        deepEq(new UriType(SYSTEM_B)),
        deepEq(new StringType(VERSION_1)),
        deepEq(new CodeType(CODE_B)),
        deepEq(new CodeType("property_A")),
        isNull()
    )).thenReturn(request);

    assertEquals(List.of(
            Property.of("property_A", new StringType("string_value_a")),
            Property.of("property_A", new IntegerType(333)),
            Property.of("property_A", new CodeType("code_value_a")),
            Property.of("property_A", new BooleanType(true))
        ),
        terminologyService.lookup(CODING_BB_VERSION1, "property_A"));
  }

  @Test
  void testLookupSubProperties() {
    final Parameters response = standardProperties(CODING_C)
        .withPropertyGroup("group_C")
        .withSubProperty("property_C", new StringType("string_value_c"))
        .withSubProperty("property_C", new CodeType("code_value_c"))
        .withSubProperty("property_D", new StringType("string_value_D"))
        .build();

    final IOperationUntypedWithInput<Parameters> request = mockRequest(
        response);
    when(terminologyClient.buildLookup(
        deepEq(new UriType(SYSTEM_C)),
        isNull(),
        deepEq(new CodeType(CODE_C)),
        any(),
        isNull()
    )).thenReturn(request);

    assertEquals(List.of(
            Property.of("property_C", new StringType("string_value_c")),
            Property.of("property_C", new CodeType("code_value_c"))
        ),
        terminologyService.lookup(CODING_C, "property_C"));
    // Does not include grouping property in the results.
    assertEquals(Collections.emptyList(),
        terminologyService.lookup(CODING_C, "group_C"));
  }

  @Test
  void testLookupDesignations() {
    final Parameters response = standardProperties(CODING_A)
        .withProperty("property_A", "value_A")
        .withDesignation("designation_D_X", CODING_D, "lang_X")
        .withDesignation("designation_E_Y", CODING_E, "lang_Y")
        .withDesignation("designation_E_?", Optional.of(CODING_E), Optional.empty())
        .withDesignation("designation_?_Z", Optional.empty(), Optional.of("lang_Z"))
        .withDesignation("designation_?_?", Optional.empty(), Optional.empty())
        .build();

    final IOperationUntypedWithInput<Parameters> request = mockRequest(
        response);
    when(terminologyClient.buildLookup(
        deepEq(new UriType(SYSTEM_A)),
        isNull(),
        deepEq(new CodeType(CODE_A)),
        deepEq(new CodeType("designation")),
        isNull()
    )).thenReturn(request);

    assertEquals(List.of(
            Designation.of(CODING_D, "lang_X", "designation_D_X"),
            Designation.of(CODING_E, "lang_Y", "designation_E_Y"),
            Designation.of(CODING_E, null, "designation_E_?"),
            Designation.of(null, "lang_Z", "designation_?_Z"),
            Designation.of(null, null, "designation_?_?")
        ),
        terminologyService.lookup(CODING_A, Designation.PROPERTY_CODE));
  }

  @Test
  void testLooksUpDesignationsForVersionedCodingAndUse() {
    final Parameters response = standardProperties(CODING_BB_VERSION1)
        .withProperty("property_A", "value_A")
        .withDesignation("designation_AB2_Z", CODING_AB_VERSION2, "lang_Z")
        .build();

    final IOperationUntypedWithInput<Parameters> request = mockRequest(
        response);
    when(terminologyClient.buildLookup(
        deepEq(new UriType(SYSTEM_B)),
        deepEq(new StringType(VERSION_1)),
        deepEq(new CodeType(CODE_B)),
        deepEq(new CodeType("designation")),
        isNull()
    )).thenReturn(request);

    assertEquals(List.of(
            Designation.of(CODING_AB_VERSION2, "lang_Z", "designation_AB2_Z")
        ),
        terminologyService.lookup(CODING_BB_VERSION1, Designation.PROPERTY_CODE));
  }

  @Test
  void testLookupHandles404Exceptions() {
    when(terminologyClient.buildLookup(any(), any(), any(), any(), any()
    )).thenThrow(BaseServerResponseException.newInstance(404, "Resource Not Found"));

    assertEquals(Collections.emptyList(),
        terminologyService.lookup(CODING_C, "property_A"));
  }

  @SuppressWarnings("unchecked")
  <R> IOperationUntypedWithInput<R> mockRequest(final R response) {
    final IOperationUntypedWithInput<R> request = (IOperationUntypedWithInput<R>) mock(
        IOperationUntypedWithInput.class);
    when(request.withAdditionalHeader(anyString(), anyString())).thenReturn(request);
    when(request.execute()).thenReturn(response);
    return request;
  }
}
