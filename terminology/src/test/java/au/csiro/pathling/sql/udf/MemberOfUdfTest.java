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
import static au.csiro.pathling.test.helpers.FhirMatchers.deepEq;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.CD_SNOMED_284551006;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.CD_SNOMED_40055000;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.CD_SNOMED_403190006;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@SuppressWarnings("ConstantConditions")
public class MemberOfUdfTest {


  private static final Coding CODING_A = CD_SNOMED_284551006;
  private static final Coding CODING_B = CD_SNOMED_40055000;
  private static final Coding CODING_C = CD_SNOMED_403190006;

  private static final String VALUE_SET_URL_A = "uuid:vsA";
  private static final String VALUE_SET_URL_AB = "uuid:vsAB";


  private static final Coding INVALID_CODING_0 = new Coding(null, null, "");
  private static final Coding INVALID_CODING_1 = new Coding("uiid:system", null, "");
  private static final Coding INVALID_CODING_2 = new Coding(null, "someCode", "");

  private MemberOfUdf memberUdf;
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
    memberUdf = new MemberOfUdf(terminologyServiceFactory);

    TerminologyServiceHelpers.setupValidate(terminologyService2)
        .withValueSet(VALUE_SET_URL_A, CODING_A)
        .withValueSet(VALUE_SET_URL_AB, CODING_A, CODING_B);
  }

  @Test
  void testNullCodings() throws Exception {
    assertNull(memberUdf.call(null, "uiid:url"));
  }

  @Test
  void testNullValueSetUrl() throws Exception {
    assertNull(memberUdf.call(encode(CD_SNOMED_284551006), null));
  }

  @Test
  void testInvalidAndNullCodings() throws Exception {
    assertFalse(
        memberUdf.call(encodeMany(INVALID_CODING_0, INVALID_CODING_1, INVALID_CODING_2, null),
            VALUE_SET_URL_A));
    verifyNoMoreInteractions(terminologyService2);
  }

  @Test
  void testInvalidCoding() throws Exception {
    assertFalse(memberUdf.call(encode(INVALID_CODING_0), VALUE_SET_URL_A));
    verifyNoMoreInteractions(terminologyService2);
  }

  @Test
  void testCodingBelongsToValueSet() throws Exception {
    assertTrue(memberUdf.call(encode(CODING_A), VALUE_SET_URL_A));
    assertTrue(memberUdf.call(encode(CODING_A), VALUE_SET_URL_AB));
    assertTrue(memberUdf.call(encode(CODING_B), VALUE_SET_URL_AB));

    assertFalse(memberUdf.call(encode(CODING_B), VALUE_SET_URL_A));
    assertFalse(memberUdf.call(encode(CODING_C), VALUE_SET_URL_A));
    assertFalse(memberUdf.call(encode(CODING_C), VALUE_SET_URL_AB));
  }

  @Test
  void testCodingsBelongsToValueSet() throws Exception {
    // positive cases
    assertTrue(memberUdf.call(encodeMany(CODING_C, CODING_A), VALUE_SET_URL_A));
    assertTrue(memberUdf.call(encodeMany(null, INVALID_CODING_0, CODING_B), VALUE_SET_URL_AB));
    assertTrue(memberUdf.call(encodeMany(CODING_A, CODING_B), VALUE_SET_URL_AB));
    // negative casses
    assertFalse(memberUdf.call(encodeMany(), VALUE_SET_URL_A));
    assertFalse(memberUdf.call(encodeMany(CODING_C, CODING_B), VALUE_SET_URL_A));
    assertFalse(memberUdf.call(encodeMany(null, INVALID_CODING_1, CODING_C), VALUE_SET_URL_AB));
  }

  @Test
  void testEarlyExitWhenMatchingCodingFound() throws Exception {
    assertTrue(memberUdf.call(encodeMany(CODING_A, CODING_B), VALUE_SET_URL_AB));
    verify(terminologyService2).validate(eq(VALUE_SET_URL_AB), deepEq(CODING_A));
    verifyNoMoreInteractions(terminologyService2);
  }
}
