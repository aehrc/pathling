package au.csiro.pathling.sql.udf;

import static au.csiro.pathling.fhirpath.encoding.CodingEncoding.encode;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import au.csiro.pathling.terminology.TerminologyService2;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.AbstractTerminologyTestBase;
import au.csiro.pathling.test.helpers.TerminologyServiceHelpers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DisplayUdfTest extends AbstractTerminologyTestBase {


  private static final String DISPLAY_NAME_A = "Test Display Name A";
  private static final String DISPLAY_NAME_B = "Test Display Name B";

  private DisplayUdf displayUdf;
  private TerminologyService2 terminologyService2;


  @BeforeEach
  void setUp() {
    terminologyService2 = mock(TerminologyService2.class);
    final TerminologyServiceFactory terminologyServiceFactory = mock(
        TerminologyServiceFactory.class);
    when(terminologyServiceFactory.buildService2()).thenReturn(terminologyService2);
    displayUdf = new DisplayUdf(terminologyServiceFactory);
  }

  @Test
  void testNullCoding() {
    assertNull(displayUdf.call(null));
    verifyNoMoreInteractions(terminologyService2);
  }

  @Test
  void testInvalidCodings() {
    assertNull(displayUdf.call(encode(INVALID_CODING_0)));
    assertNull(displayUdf.call(encode(INVALID_CODING_1)));
    assertNull(displayUdf.call(encode(INVALID_CODING_2)));
    verifyNoMoreInteractions(terminologyService2);
  }


  @Test
  void testGetsDisplayName() {
    TerminologyServiceHelpers.setupLookup(terminologyService2)
        .withDisplay(CODING_A, DISPLAY_NAME_A)
        .withDisplay(CODING_BB_VERSION1, DISPLAY_NAME_B);

    assertEquals(DISPLAY_NAME_A, displayUdf.call(encode(CODING_A)));
    assertEquals(DISPLAY_NAME_B, displayUdf.call(encode(CODING_BB_VERSION1)));

    // null when display property it not present
    assertNull(displayUdf.call(encode(CODING_C)));
  }
}
