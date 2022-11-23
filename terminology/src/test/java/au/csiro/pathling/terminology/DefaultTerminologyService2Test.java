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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import au.csiro.pathling.fhir.TerminologyClient2;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.UriType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DefaultTerminologyService2Test {

  public static final String SYSTEM_A = "uuid:systemA";
  public static final String CODE_A = "codeA";
  private static final String SYSTEM_B = "uuid:systemB";
  private static final String CODE_B = "codeB";

  private static final Coding CODING_AA = new Coding(SYSTEM_A, CODE_A, "displayAA");
  private static final Coding CODING_AB = new Coding(SYSTEM_A, CODE_B, "displayAB");

  public static final String VERSION_1 = "version1";
  public static final String VERSION_2 = "version2";

  private static final Coding CODING_AA_VERSION1 = new Coding(SYSTEM_A, CODE_A,
      "displayAA").setVersion(
      VERSION_1);
  private static final Coding CODING_AB_VERSION1 = new Coding(SYSTEM_A, CODE_B,
      "displayAB").setVersion(
      VERSION_1);
  private static final Coding CODING_AB_VERSION2 = new Coding(SYSTEM_A, CODE_B,
      "displayAB").setVersion(
      VERSION_2);

  private static final Coding CODING_B = new Coding(SYSTEM_B, CODE_B, "displayB").setVersion(
      VERSION_1);

  private static final String VALUE_SET_X = "uuid:valueSetX";
  private static final String VALUE_SET_Y = "uuid:valueSetY";


  private static final Coding INVALID_CODING_0 = new Coding(null, null, "");
  private static final Coding INVALID_CODING_1 = new Coding("uiid:system", null, "");
  private static final Coding INVALID_CODING_2 = new Coding(null, "someCode", "");

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
    assertFalse(terminologyService.validate(VALUE_SET_Y, CODING_B));
  }

  @Test
  public void throwsExceptionForIllegalArguments() {
    assertThrows(NullPointerException.class,
        () -> terminologyService.validate(null, CODING_AA));
    assertThrows(NullPointerException.class,
        () -> terminologyService.validate(VALUE_SET_Y, INVALID_CODING_0));
    assertThrows(NullPointerException.class,
        () -> terminologyService.validate(VALUE_SET_Y, INVALID_CODING_1));
    assertThrows(NullPointerException.class,
        () -> terminologyService.validate(VALUE_SET_Y, INVALID_CODING_2));
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
    assertEquals(NOTSUBSUMED, terminologyService.subsumes(CODING_AA, CODING_B));
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
    assertEquals(NOTSUBSUMED, terminologyService.subsumes(CODING_B, INVALID_CODING_0));
    assertEquals(NOTSUBSUMED, terminologyService.subsumes(CODING_B, INVALID_CODING_1));
    assertEquals(NOTSUBSUMED, terminologyService.subsumes(CODING_B, INVALID_CODING_2));
    assertEquals(NOTSUBSUMED, terminologyService.subsumes(INVALID_CODING_0, INVALID_CODING_1));
    assertEquals(NOTSUBSUMED, terminologyService.subsumes(INVALID_CODING_1, INVALID_CODING_2));
    assertEquals(NOTSUBSUMED, terminologyService.subsumes(INVALID_CODING_2, INVALID_CODING_0));
    verifyNoMoreInteractions(terminologClient);
  }

}


