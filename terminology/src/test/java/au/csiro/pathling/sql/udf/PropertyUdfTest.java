/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.sql.udf;

import static au.csiro.pathling.fhirpath.encoding.CodingEncoding.encode;
import static au.csiro.pathling.sql.udf.PropertyUdfTest.Values.ofPrimitive;
import static au.csiro.pathling.test.helpers.FhirMatchers.deepEq;
import static au.csiro.pathling.test.helpers.TerminologyServiceHelpers.setupLookup;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.AbstractTerminologyTestBase;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import lombok.Value;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.DecimalType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class PropertyUdfTest extends AbstractTerminologyTestBase {

  public static final int RANGE_ONE_FROM = 0;
  public static final int RANGE_ONE_TO = 2;
  public static final int RANGE_TWO_FROM = 1;
  public static final int RANGE_TWO_TO = 4;

  @Value(staticConstructor = "of")
  static class Values<T> {

    Type[] fhirValues;
    T[] objectValues;

    Type fhirValueAt(final int i) {
      return fhirValues[i];
    }

    Object[] objectValueAt(final int i) {
      return Arrays.copyOfRange(objectValues, i, i + 1);
    }

    Stream<Type> fhirRangeOne() {
      return Stream.of(Arrays.copyOfRange(fhirValues, RANGE_ONE_FROM, RANGE_ONE_TO));
    }

    Stream<Type> fhirRangeTwo() {
      return Stream.of(Arrays.copyOfRange(fhirValues, RANGE_TWO_FROM, RANGE_TWO_TO));
    }

    Object[] objectRangeOne() {
      return Arrays.copyOfRange(objectValues, RANGE_ONE_FROM, RANGE_ONE_TO);
    }

    Object[] objectRangeTwo() {
      return Arrays.copyOfRange(objectValues, RANGE_TWO_FROM, RANGE_TWO_TO);
    }

    @SafeVarargs
    public static <T, FT extends Type> Values<T> ofPrimitive(
        final Function<T, FT> constructor,
        final T... values) {
      return of(Stream.of(values).map(constructor).toArray(Type[]::new), values);
    }
  }

  private static final Map<Class<?>, Values<?>> TEST_VALUES = ImmutableMap.of(
      // The last value is repeated for purpose for testing that
      // duplicate values are allowed in the result
      StringType.class, ofPrimitive(StringType::new,
          "value_0", "value_1", "value_2", "value_2"),
      CodeType.class, ofPrimitive(CodeType::new, "value_0", "value_1", "value_2", "value_2"),
      BooleanType.class, ofPrimitive(BooleanType::new, true, false, true, true),
      IntegerType.class, ofPrimitive(IntegerType::new, 1, 2, 3, 3),
      DecimalType.class, ofPrimitive(DecimalType::new, new BigDecimal("1.1"),
          new BigDecimal("2.2"), new BigDecimal("3.3"), new BigDecimal("3.3")),
      DateTimeType.class, ofPrimitive(DateTimeType::new, "2001-01-01",
          "2002-02-02", "2003-03-03", "2003-03-03"),
      Coding.class, Values.of(new Coding[]{CODING_A, CODING_BB_VERSION1, CODING_C, CODING_C},
          asArray(CODING_A, CODING_BB_VERSION1, CODING_C, CODING_C))
  );

  private static Stream<Values<?>> allTestValues() {
    return TEST_VALUES.values().stream();
  }

  private PropertyUdf propertyUdf;
  private TerminologyService terminologyService;
  private TerminologyServiceFactory terminologyServiceFactory;

  @BeforeEach
  public void setUp() {
    terminologyService = mock(TerminologyService.class);
    terminologyServiceFactory = mock(TerminologyServiceFactory.class);
    when(terminologyServiceFactory.build()).thenReturn(terminologyService);
  }

  public static Stream<Class<?>> inputs() {
    return TEST_VALUES.keySet().stream();
  }

  private void setupUdf(final Class<? extends Type> udfClass) {
    propertyUdf = PropertyUdf.forClass(terminologyServiceFactory, udfClass);
  }

  @ParameterizedTest
  @MethodSource("inputs")
  public void testReturnsNullWhenNullCoding(final Class<? extends Type> udfClass) {
    setupUdf(udfClass);
    assertNull(propertyUdf.call(null, "display"));
    verifyNoMoreInteractions(terminologyService);
  }

  @ParameterizedTest
  @MethodSource("inputs")
  public void testReturnsNullWhenInvalidCodings(final Class<? extends Type> udfClass) {
    setupUdf(udfClass);
    assertNull(propertyUdf.call(encode(INVALID_CODING_0), "display"));
    assertNull(propertyUdf.call(encode(INVALID_CODING_1), "name"));
    assertNull(propertyUdf.call(encode(INVALID_CODING_2), "code"));
    verifyNoMoreInteractions(terminologyService);
  }

  @ParameterizedTest
  @MethodSource("inputs")
  public void testReturnsNullWhenNullPropertyCode(final Class<? extends Type> udfClass) {
    setupUdf(udfClass);
    assertNull(propertyUdf.call(encode(CODING_A), null));
    verifyNoMoreInteractions(terminologyService);
  }

  @ParameterizedTest
  @MethodSource("inputs")
  public void testReturnsEmptyArrayWhenNonExisingProperty(final Class<? extends Type> udfClass) {
    setupUdf(udfClass);
    assertArrayEquals(new String[RANGE_ONE_FROM],
        propertyUdf.call(encode(CODING_A), "unknownProperty_0"));
    assertArrayEquals(new String[RANGE_ONE_FROM],
        propertyUdf.call(encode(CODING_BB_VERSION1), "unknownProperty_1"));

    verify(terminologyService).lookup(deepEq(CODING_A), eq("unknownProperty_0"));
    verify(terminologyService).lookup(deepEq(CODING_BB_VERSION1), eq("unknownProperty_1"));
    verifyNoMoreInteractions(terminologyService);
  }

  @ParameterizedTest
  @MethodSource("inputs")
  public void testReturnsASingleValueForKnownProperty(final Class<? extends Type> udfClass) {
    setupUdf(udfClass);
    setupLookup(terminologyService)
        .withProperty(CODING_A, "property_0",
            allTestValues().map(v -> v.fhirValueAt(0)).toArray(Type[]::new))
        .withProperty(CODING_BB_VERSION1, "property_1",
            allTestValues().map(v -> v.fhirValueAt(1)).toArray(Type[]::new))
        .withProperty(CODING_A, "property_1",
            allTestValues().map(v -> v.fhirValueAt(2)).toArray(Type[]::new));

    final Object[] expectedSingletonOne = TEST_VALUES.get(udfClass).objectValueAt(0);
    final Object[] expectedSingletonTwo = TEST_VALUES.get(udfClass).objectValueAt(1);

    assertArrayEquals(expectedSingletonOne,
        propertyUdf.call(encode(CODING_A), "property_0"));
    assertArrayEquals(expectedSingletonTwo,
        propertyUdf.call(encode(CODING_BB_VERSION1), "property_1"));
  }

  @ParameterizedTest
  @MethodSource("inputs")
  public void testReturnsManyValueForKnownProperty(final Class<? extends Type> udfClass) {
    setupUdf(udfClass);
    setupLookup(terminologyService)
        .withProperty(CODING_AA_VERSION1, "property_a",
            allTestValues().flatMap(Values::fhirRangeOne).toArray(Type[]::new))
        .withProperty(CODING_B, "property_b",
            allTestValues().flatMap(Values::fhirRangeTwo).toArray(Type[]::new));

    final Object[] expectedArrayOne = TEST_VALUES.get(udfClass).objectRangeOne();
    final Object[] expectedArrayTwo = TEST_VALUES.get(udfClass).objectRangeTwo();

    assertArrayEquals(expectedArrayOne,
        propertyUdf.call(encode(CODING_AA_VERSION1), "property_a"));
    assertArrayEquals(expectedArrayTwo,
        propertyUdf.call(encode(CODING_B), "property_b"));
  }
}
