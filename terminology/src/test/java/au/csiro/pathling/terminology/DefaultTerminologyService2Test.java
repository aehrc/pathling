package au.csiro.pathling.terminology;


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
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import au.csiro.pathling.fhir.TerminologyClient2;
import au.csiro.pathling.terminology.TerminologyService2.Translation;
import au.csiro.pathling.test.TerminologyTest;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.UriType;
import org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DefaultTerminologyService2Test extends TerminologyTest {

  private static final String VALUE_SET_X = "uuid:valueSetX";
  private static final String VALUE_SET_Y = "uuid:valueSetY";

  private static final String CONCEPT_MAP_0 = "uuid:conceptMap0";
  private static final String CONCEPT_MAP_1 = "uuid:conceptMap1";

  private static final List<Translation> EMPTY_TRANSLATION = Collections.emptyList();

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
    assertTrue(terminologyService.validate(VALUE_SET_X, CODING_AA));
  }

  @Test
  public void testValidateVersionedCodingFalse() {
    when(terminologClient.validateCode(
        deepEq(new UriType(VALUE_SET_Y)),
        deepEq(new UriType(SYSTEM_B)),
        deepEq(new StringType(VERSION_1)),
        deepEq(new CodeType(CODE_B))
    )).thenReturn(RESULT_FALSE);
    assertFalse(terminologyService.validate(VALUE_SET_Y, CODING_BB_VERSION1));
  }

  @Test
  public void testValidateInvalidCodings() {
    assertFalse(terminologyService.validate(VALUE_SET_Y, INVALID_CODING_0));
    assertFalse(terminologyService.validate(VALUE_SET_Y, INVALID_CODING_1));
    assertFalse(terminologyService.validate(VALUE_SET_Y, INVALID_CODING_2));
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

}


