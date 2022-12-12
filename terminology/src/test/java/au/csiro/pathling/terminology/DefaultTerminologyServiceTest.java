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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.terminology.TerminologyService.Property;
import au.csiro.pathling.terminology.TerminologyService.Translation;
import au.csiro.pathling.test.AbstractTerminologyTestBase;
import ca.uhn.fhir.rest.gclient.IOperationUntypedWithInput;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Coding;
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


  private TerminologyClient terminologyClient;
  private DefaultTerminologyService terminologyService;

  @BeforeEach
  void setUp() {
    terminologyClient = mock(TerminologyClient.class);
    terminologyService = new DefaultTerminologyService(
        terminologyClient, null);
  }

  @Test
  public void testValidateCodingTrue() {
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
  public void testValidateVersionedCodingFalse() {
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
  public void testValidateInvalidCodings() {
    assertFalse(terminologyService.validateCode(VALUE_SET_Y, INVALID_CODING_0));
    assertFalse(terminologyService.validateCode(VALUE_SET_Y, INVALID_CODING_1));
    assertFalse(terminologyService.validateCode(VALUE_SET_Y, INVALID_CODING_2));
  }

  @Test
  public void testSubsumesNoVersion() {
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
  public void testSubsumesLeftVersion() {
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
  public void testSubsumesRightVersion() {
    final IOperationUntypedWithInput<Parameters> request = mockRequest(
        OUTCOME_SUBSUMEDBY);
    when(terminologyClient.buildSubsumes(
        deepEq(new CodeType(CODE_A)),
        deepEq(new CodeType(CODE_B)),
        deepEq(new UriType(SYSTEM_A)),
        deepEq(new StringType(VERSION_2))
    )).thenReturn(request);
    assertEquals(SUBSUMEDBY, terminologyService.subsumes(CODING_AA, CODING_AB_VERSION2));
  }

  @Test
  public void testSubsumesBothVersionTheSame() {
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
  public void testSubsumesDifferentVersions() {
    assertEquals(NOTSUBSUMED, terminologyService.subsumes(CODING_AA_VERSION1, CODING_AB_VERSION2));
    verifyNoMoreInteractions(terminologyClient);
  }

  @Test
  public void testSubsumesDifferentSystems() {
    assertEquals(NOTSUBSUMED, terminologyService.subsumes(CODING_AA, CODING_BB_VERSION1));
    verifyNoMoreInteractions(terminologyClient);
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
    verifyNoMoreInteractions(terminologyClient);
  }


  @Test
  public void testTranslatesVersionedCodingWithDefaults() {
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
  public void testTranslatesUnversionedCoding() {

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
  public void testTranslatesInvalidsCoding() {

    assertEquals(EMPTY_TRANSLATION,
        terminologyService.translate(INVALID_CODING_0, CONCEPT_MAP_0, false, null));
    assertEquals(EMPTY_TRANSLATION,
        terminologyService.translate(INVALID_CODING_1, CONCEPT_MAP_0, true, null));
    assertEquals(EMPTY_TRANSLATION,
        terminologyService.translate(INVALID_CODING_2, CONCEPT_MAP_0, false, SYSTEM_B));
    verifyNoMoreInteractions(terminologyClient);
  }

  @Test
  public void testLooksUpInvalidCoding() {
    assertEquals(Collections.emptyList(),
        terminologyService.lookup(INVALID_CODING_0, null, null));
    assertEquals(Collections.emptyList(),
        terminologyService.lookup(INVALID_CODING_1, "display", null));
    assertEquals(Collections.emptyList(),
        terminologyService.lookup(INVALID_CODING_2, "designation", "en"));
    verifyNoMoreInteractions(terminologyClient);
  }

  @Nonnull
  private static Parameters standardProperties(@Nonnull final Coding coding) {
    return new Parameters()
        .addParameter("display", coding.getDisplay())
        .addParameter("code", new CodeType(coding.getCode()))
        .addParameter("name", "My Test Coding System");
  }

  @Test
  public void testLooksUpStandardProperty() {

    final IOperationUntypedWithInput<Parameters> request1 = mockRequest(
        standardProperties(CODING_A));
    when(terminologyClient.buildLookup(
        deepEq(new UriType(SYSTEM_A)),
        isNull(),
        deepEq(new CodeType(CODE_A)),
        deepEq(new CodeType("display")),
        isNull())).thenReturn(request1);

    final IOperationUntypedWithInput<Parameters> request2 = mockRequest(
        standardProperties(CODING_BB_VERSION1));
    when(terminologyClient.buildLookup(
        deepEq(new UriType(SYSTEM_B)),
        deepEq(new StringType(VERSION_1)),
        deepEq(new CodeType(CODE_B)),
        deepEq(new CodeType("code")),
        deepEq(new CodeType("en")))).thenReturn(request2);

    assertEquals(List.of(Property.of("display", new StringType(CODING_AA.getDisplay()))),
        terminologyService.lookup(CODING_AA, "display", null));

    assertEquals(List.of(Property.of("code", new CodeType(CODING_BB_VERSION1.getCode()))),
        terminologyService.lookup(CODING_BB_VERSION1, "code", "en"));
  }

  @SuppressWarnings("unchecked")
  <ResponseType> IOperationUntypedWithInput<ResponseType> mockRequest(final ResponseType response) {
    final IOperationUntypedWithInput<ResponseType> request = (IOperationUntypedWithInput<ResponseType>) mock(
        IOperationUntypedWithInput.class);
    when(request.withAdditionalHeader(anyString(), anyString())).thenReturn(request);
    when(request.execute()).thenReturn(response);
    return request;
  }

}
