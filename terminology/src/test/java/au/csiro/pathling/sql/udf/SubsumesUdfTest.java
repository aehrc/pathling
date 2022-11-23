package au.csiro.pathling.sql.udf;

import au.csiro.pathling.fhirpath.encoding.CodingEncoding;
import au.csiro.pathling.terminology.TerminologyService2;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.helpers.TerminologyServiceHelpers;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Coding;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.collection.mutable.WrappedArray;
import java.util.stream.Stream;

import static au.csiro.pathling.fhirpath.encoding.CodingEncoding.encode;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.CD_SNOMED_284551006;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.CD_SNOMED_40055000;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.CD_SNOMED_403190006;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.CD_SNOMED_444814009;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@SuppressWarnings("ConstantConditions")
public class SubsumesUdfTest {

  private static final Coding CODING_A = CD_SNOMED_284551006;
  private static final Coding CODING_B = CD_SNOMED_40055000;
  private static final Coding CODING_C = CD_SNOMED_403190006;
  private static final Coding CODING_D = CD_SNOMED_444814009;

  private static final Coding INVALID_CODING_0 = new Coding(null, null, "");
  private static final Coding INVALID_CODING_1 = new Coding("uiid:system", null, "");
  private static final Coding INVALID_CODING_2 = new Coding(null, "someCode", "");


  private SubsumesUdf subsumesUdf;
  private TerminologyService2 terminologyService2;

  public static WrappedArray<Row> encodeMany(Coding... codings) {
    return WrappedArray.make(Stream.of(codings).map(CodingEncoding::encode).toArray(Row[]::new));
  }

  @BeforeEach
  void setUp() {
    terminologyService2 = mock(TerminologyService2.class);
    final TerminologyServiceFactory terminologyServiceFactory = mock(
        TerminologyServiceFactory.class);
    when(terminologyServiceFactory.buildService2()).thenReturn(terminologyService2);

    TerminologyServiceHelpers.setupSubsumes(terminologyService2)
        .withSubsumes(CODING_A, CODING_B)
        .withSubsumes(CODING_C, CODING_D);
    subsumesUdf = new SubsumesUdf(terminologyServiceFactory);
  }

  @Test
  void testNullCodings() {
    assertNull(subsumesUdf.call(null, null, null));
    assertNull(subsumesUdf.call(encode(CODING_A), null, true));
    assertNull(subsumesUdf.call(null, encode(CODING_B), false));
    verifyNoMoreInteractions(terminologyService2);
  }

  @Test
  void testInvalidCodings() {
    assertFalse(subsumesUdf.call(encode(INVALID_CODING_0), encode(INVALID_CODING_1), null));
    assertFalse(subsumesUdf.call(encode(INVALID_CODING_1), encode(INVALID_CODING_2), false));
    assertFalse(subsumesUdf.call(encode(INVALID_CODING_2), encode(INVALID_CODING_0), true));
    verifyNoMoreInteractions(terminologyService2);
  }

  @Test
  void testNullAndInvalidCodings() {
    assertFalse(subsumesUdf.call(encodeMany(null, INVALID_CODING_0),
        encodeMany(null, INVALID_CODING_1, INVALID_CODING_2), false));
    assertFalse(subsumesUdf.call(encodeMany(null, INVALID_CODING_1, INVALID_CODING_2),
        encodeMany(null, INVALID_CODING_0), true));
    verifyNoMoreInteractions(terminologyService2);
  }


  @Test
  void testSubsumesCoding() {
    // self subsumption
    assertTrue(subsumesUdf.call(encode(CODING_A), encode(CODING_A), null));
    assertTrue(subsumesUdf.call(encode(CODING_B), encode(CODING_B), false));
    assertTrue(subsumesUdf.call(encode(CODING_C), encode(CODING_C), true));

    // positive cases 
    assertTrue(subsumesUdf.call(encode(CODING_A), encode(CODING_B), null));
    assertTrue(subsumesUdf.call(encode(CODING_C), encode(CODING_D), false));
    assertTrue(subsumesUdf.call(encode(CODING_B), encode(CODING_A), true));

    // negative cases
    assertFalse(subsumesUdf.call(encode(CODING_D), encode(CODING_C), null));
    assertFalse(subsumesUdf.call(encode(CODING_B), encode(CODING_A), false));
    assertFalse(subsumesUdf.call(encode(CODING_A), encode(CODING_B), true));
    assertFalse(subsumesUdf.call(encode(CODING_C), encode(CODING_B), null));
    assertFalse(subsumesUdf.call(encode(CODING_A), encode(CODING_C), true));
  }

  @Test
  void testSubsumesCodings() {
    // positive cases 
    assertTrue(
        subsumesUdf.call(encodeMany(null, INVALID_CODING_0, CODING_A, CODING_D), encode(CODING_B),
            null));
    assertTrue(
        subsumesUdf.call(encodeMany(CODING_B, CODING_C), encodeMany(CODING_A, CODING_D), false));
    assertTrue(
        subsumesUdf.call(encodeMany(CODING_B, CODING_C), encodeMany(CODING_A, CODING_D), true));
    assertTrue(
        subsumesUdf.call(encodeMany(null, INVALID_CODING_1, CODING_B, CODING_C), encode(CODING_D),
            false));

    // NegativeCases
    assertFalse(subsumesUdf.call(encodeMany(), encode(CODING_B), null));
    assertFalse(subsumesUdf.call(encode(CODING_C), encodeMany(), true));
    assertFalse(subsumesUdf.call(encodeMany(), encodeMany(), true));

    assertFalse(subsumesUdf.call(encode(CODING_B), encodeMany(CODING_A, CODING_C, CODING_D), null));
    assertFalse(
        subsumesUdf.call(encodeMany(CODING_D, CODING_A, CODING_B), encode(CODING_C), false));
    assertFalse(
        subsumesUdf.call(encodeMany(CODING_A, CODING_C), encodeMany(CODING_B, CODING_D), true));
  }

}
