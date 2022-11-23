package au.csiro.pathling.terminology;


import static au.csiro.pathling.test.helpers.FhirMatchers.deepEq;
import static au.csiro.pathling.test.helpers.TerminologyServiceHelpers.RESULT_FALSE;
import static au.csiro.pathling.test.helpers.TerminologyServiceHelpers.RESULT_TRUE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
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
  private static final String VERSION_B = "versionB";

  private static final Coding CODING_A = new Coding(SYSTEM_A, CODE_A, "displayA");
  private static final Coding CODING_B = new Coding(SYSTEM_B, CODE_B, "displayB").setVersion(
      VERSION_B);

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

    assertTrue(terminologyService.validate(VALUE_SET_X, CODING_A));
  }

  @Test
  public void testValidateVersionedCodingFalse() {
    when(terminologClient.validateCode(
        deepEq(new UriType(VALUE_SET_Y)),
        deepEq(new UriType(SYSTEM_B)),
        deepEq(new StringType(VERSION_B)),
        deepEq(new CodeType(CODE_B))
    )).thenReturn(RESULT_FALSE);
    assertFalse(terminologyService.validate(VALUE_SET_Y, CODING_B));
  }

  @Test
  public void throwsExceptionForIllegalArguments() {
    assertThrows(NullPointerException.class,
        () -> terminologyService.validate(null, CODING_A));
    assertThrows(NullPointerException.class,
        () -> terminologyService.validate(VALUE_SET_Y, INVALID_CODING_0));
    assertThrows(NullPointerException.class,
        () -> terminologyService.validate(VALUE_SET_Y, INVALID_CODING_1));
    assertThrows(NullPointerException.class,
        () -> terminologyService.validate(VALUE_SET_Y, INVALID_CODING_2));
  }

}
