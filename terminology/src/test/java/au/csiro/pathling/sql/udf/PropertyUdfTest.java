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

package au.csiro.pathling.sql.udf;

import static au.csiro.pathling.fhirpath.encoding.CodingSchema.encode;
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
import jakarta.annotation.Nonnull;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
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

    List<Type> fhirValues;
    List<T> objectValues;

    Type fhirValueAt(final int i) {
      return fhirValues.get(i);
    }

    List<Object> objectValueAt(final int i) {
      return Collections.singletonList(objectValues.get(i));
    }

    Stream<Type> fhirRangeOne() {
      return fhirValues.subList(RANGE_ONE_FROM, RANGE_ONE_TO).stream();
    }

    Stream<Type> fhirRangeTwo() {
      return fhirValues.subList(RANGE_TWO_FROM, RANGE_TWO_TO).stream();
    }

    List<T> objectRangeOne() {
      return objectValues.subList(RANGE_ONE_FROM, RANGE_ONE_TO);
    }

    List<T> objectRangeTwo() {
      return objectValues.subList(RANGE_TWO_FROM, RANGE_TWO_TO);
    }

    public static <T> Values<T> ofPrimitive(
        final Function<T, Type> constructor,
        final List<T> values) {
      return of(values.stream().map(constructor).toList(), values);
    }
  }

  private static final Map<Class<?>, Values<?>> TEST_VALUES = ImmutableMap.of(
      // The last value is repeated for purpose for testing that
      // duplicate values are allowed in the result
      StringType.class, ofPrimitive(StringType::new,
          List.of("value_0", "value_1", "value_2", "value_2")),
      CodeType.class,
      ofPrimitive(CodeType::new, List.of("value_0", "value_1", "value_2", "value_2")),
      BooleanType.class, ofPrimitive(BooleanType::new, List.of(true, false, true, true)),
      IntegerType.class, ofPrimitive(IntegerType::new, List.of(1, 2, 3, 3)),
      DecimalType.class, ofPrimitive(DecimalType::new, List.of(new BigDecimal("1.1"),
          new BigDecimal("2.2"), new BigDecimal("3.3"), new BigDecimal("3.3"))),
      DateTimeType.class, ofPrimitive(DateTimeType::new, List.of("2001-01-01",
          "2002-02-02", "2003-03-03", "2003-03-03")),
      Coding.class, Values.of(List.of(CODING_A, CODING_BB_VERSION1, CODING_C, CODING_C),
          List.of(asArray(
              List.of(CODING_A, CODING_BB_VERSION1, CODING_C, CODING_C).toArray(new Coding[]{})))
      ));

  @Nonnull
  private static Stream<Values<?>> allTestValues() {
    return TEST_VALUES.values().stream();
  }

  private PropertyUdf propertyUdf;
  private TerminologyService terminologyService;
  private TerminologyServiceFactory terminologyServiceFactory;

  @BeforeEach
  void setUp() {
    terminologyService = mock(TerminologyService.class);
    terminologyServiceFactory = mock(TerminologyServiceFactory.class);
    when(terminologyServiceFactory.build()).thenReturn(terminologyService);
  }

  @Nonnull
  public static Stream<Class<?>> inputs() {
    return TEST_VALUES.keySet().stream();
  }

  private void setupUdf(final Class<? extends Type> udfClass) {
    propertyUdf = PropertyUdf.forClass(terminologyServiceFactory, udfClass);
  }

  @ParameterizedTest
  @MethodSource("inputs")
  void testReturnsNullWhenNullCoding(final Class<? extends Type> udfClass) {
    setupUdf(udfClass);
    assertNull(propertyUdf.call(null, "display", null));
    verifyNoMoreInteractions(terminologyService);
  }

  @ParameterizedTest
  @MethodSource("inputs")
  void testReturnsNullWhenInvalidCodings(final Class<? extends Type> udfClass) {
    setupUdf(udfClass);
    assertNull(propertyUdf.call(encode(INVALID_CODING_0), "display", null));
    assertNull(propertyUdf.call(encode(INVALID_CODING_1), "name", null));
    assertNull(propertyUdf.call(encode(INVALID_CODING_2), "code", null));
    verifyNoMoreInteractions(terminologyService);
  }

  @ParameterizedTest
  @MethodSource("inputs")
  void testReturnsNullWhenNullPropertyCode(final Class<? extends Type> udfClass) {
    setupUdf(udfClass);
    assertNull(propertyUdf.call(encode(CODING_A), null, null));
    verifyNoMoreInteractions(terminologyService);
  }

  @ParameterizedTest
  @MethodSource("inputs")
  void testReturnsEmptyArrayWhenNonExisingProperty(final Class<? extends Type> udfClass) {
    setupUdf(udfClass);
    assertArrayEquals(new String[RANGE_ONE_FROM],
        propertyUdf.call(encode(CODING_A), "unknownProperty_0", null));
    assertArrayEquals(new String[RANGE_ONE_FROM],
        propertyUdf.call(encode(CODING_BB_VERSION1), "unknownProperty_1", null));

    verify(terminologyService).lookup(deepEq(CODING_A), eq("unknownProperty_0"), eq(null));
    verify(terminologyService).lookup(deepEq(CODING_BB_VERSION1), eq("unknownProperty_1"),
        eq(null));
    verifyNoMoreInteractions(terminologyService);
  }

  @ParameterizedTest
  @MethodSource("inputs")
  void testReturnsASingleValueForKnownProperty(final Class<? extends Type> udfClass) {
    setupUdf(udfClass);
    setupLookup(terminologyService)
        .withProperty(CODING_A, "property_0", null,
            allTestValues().map(v -> v.fhirValueAt(0)).toList())
        .withProperty(CODING_BB_VERSION1, "property_1", null,
            allTestValues().map(v -> v.fhirValueAt(1)).toList())
        .withProperty(CODING_A, "property_1", null,
            allTestValues().map(v -> v.fhirValueAt(2)).toList());

    final List<Object> expectedSingletonOne = TEST_VALUES.get(udfClass).objectValueAt(0);
    final List<Object> expectedSingletonTwo = TEST_VALUES.get(udfClass).objectValueAt(1);

    assertArrayEquals(expectedSingletonOne.toArray(),
        propertyUdf.call(encode(CODING_A), "property_0", null));
    assertArrayEquals(expectedSingletonTwo.toArray(),
        propertyUdf.call(encode(CODING_BB_VERSION1), "property_1", null));
  }

  @ParameterizedTest
  @MethodSource("inputs")
  void testReturnsManyValueForKnownProperty(final Class<? extends Type> udfClass) {
    setupUdf(udfClass);
    setupLookup(terminologyService)
        .withProperty(CODING_AA_VERSION1, "property_a", null,
            allTestValues().flatMap(Values::fhirRangeOne).toList())
        .withProperty(CODING_B, "property_b", null,
            allTestValues().flatMap(Values::fhirRangeTwo).toList());

    final List<?> expectedArrayOne = TEST_VALUES.get(udfClass).objectRangeOne();
    final List<?> expectedArrayTwo = TEST_VALUES.get(udfClass).objectRangeTwo();

    assertArrayEquals(expectedArrayOne.toArray(),
        propertyUdf.call(encode(CODING_AA_VERSION1), "property_a", null));
    assertArrayEquals(expectedArrayTwo.toArray(),
        propertyUdf.call(encode(CODING_B), "property_b", null));
  }


  @ParameterizedTest
  @MethodSource("inputs")
  void testReturnsManyValueForKnownPropertyWithLanguage(
      final Class<? extends Type> udfClass) {
    setupUdf(udfClass);
    setupLookup(terminologyService)
        .withProperty(CODING_AA_VERSION1, "property_a", "de",
            allTestValues().flatMap(Values::fhirRangeOne).toList())
        .withProperty(CODING_B, "property_b", "fr",
            allTestValues().flatMap(Values::fhirRangeTwo).toList());

    final List<?> expectedArrayOne = TEST_VALUES.get(udfClass).objectRangeOne();
    final List<?> expectedArrayTwo = TEST_VALUES.get(udfClass).objectRangeTwo();

    assertArrayEquals(expectedArrayOne.toArray(),
        propertyUdf.call(encode(CODING_AA_VERSION1), "property_a", "de"));
    assertArrayEquals(expectedArrayTwo.toArray(),
        propertyUdf.call(encode(CODING_B), "property_b", "fr"));
  }
}
