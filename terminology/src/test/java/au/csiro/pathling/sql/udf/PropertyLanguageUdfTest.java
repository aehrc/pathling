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

import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.AbstractTerminologyTestBase;
import com.google.common.collect.ImmutableMap;
import lombok.Value;
import org.hl7.fhir.r4.model.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static au.csiro.pathling.fhirpath.encoding.CodingEncoding.encode;
import static au.csiro.pathling.sql.udf.PropertyUdfTest.Values.ofPrimitive;
import static au.csiro.pathling.sql.udf.PropertyUdfTest.Values;
import static au.csiro.pathling.test.helpers.FhirMatchers.deepEq;
import static au.csiro.pathling.test.helpers.TerminologyServiceHelpers.setupLookup;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.*;

public class PropertyLanguageUdfTest extends AbstractTerminologyTestBase {

  public static final int RANGE_ONE_FROM = 0;
  public static final int RANGE_ONE_TO = 2;
  public static final int RANGE_TWO_FROM = 1;
  public static final int RANGE_TWO_TO = 4;


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

  private PropertyLanguageUdf propertyLanguageUdf;
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
    propertyLanguageUdf = PropertyLanguageUdf.forClass(terminologyServiceFactory, udfClass);
  }

  @ParameterizedTest
  @MethodSource("inputs")
  public void testReturnsNullWhenNullCoding(final Class<? extends Type> udfClass) {
    setupUdf(udfClass);
    assertNull(propertyLanguageUdf.call(null, "display", null));
    verifyNoMoreInteractions(terminologyService);
  }

  @ParameterizedTest
  @MethodSource("inputs")
  public void testReturnsNullWhenInvalidCodings(final Class<? extends Type> udfClass) {
    setupUdf(udfClass);
    assertNull(propertyLanguageUdf.call(encode(INVALID_CODING_0), "display", null));
    assertNull(propertyLanguageUdf.call(encode(INVALID_CODING_1), "name", null));
    assertNull(propertyLanguageUdf.call(encode(INVALID_CODING_2), "code", null));
    verifyNoMoreInteractions(terminologyService);
  }

  @ParameterizedTest
  @MethodSource("inputs")
  public void testReturnsNullWhenNullPropertyCode(final Class<? extends Type> udfClass) {
    setupUdf(udfClass);
    assertNull(propertyLanguageUdf.call(encode(CODING_A), null, null));
    verifyNoMoreInteractions(terminologyService);
  }

  @ParameterizedTest
  @MethodSource("inputs")
  public void testReturnsEmptyArrayWhenNonExisingProperty(final Class<? extends Type> udfClass) {
    setupUdf(udfClass);
    assertArrayEquals(new String[RANGE_ONE_FROM],
        propertyLanguageUdf.call(encode(CODING_A), "unknownProperty_0", null));
    assertArrayEquals(new String[RANGE_ONE_FROM],
        propertyLanguageUdf.call(encode(CODING_BB_VERSION1), "unknownProperty_1", null));

    verify(terminologyService).lookup(deepEq(CODING_A), eq("unknownProperty_0"), eq(null));
    verify(terminologyService).lookup(deepEq(CODING_BB_VERSION1), eq("unknownProperty_1"), eq(null));
    verifyNoMoreInteractions(terminologyService);
  }

  @ParameterizedTest
  @MethodSource("inputs")
  public void testReturnsASingleValueForKnownProperty(final Class<? extends Type> udfClass) {
    setupUdf(udfClass);
    setupLookup(terminologyService)
        .withPropertyLanguage(CODING_A, "property_0", null,
            allTestValues().map(v -> v.fhirValueAt(0)).toArray(Type[]::new))
        .withPropertyLanguage(CODING_BB_VERSION1, "property_1", null,
            allTestValues().map(v -> v.fhirValueAt(1)).toArray(Type[]::new))
        .withPropertyLanguage(CODING_A, "property_1", null,
            allTestValues().map(v -> v.fhirValueAt(2)).toArray(Type[]::new));

    final Object[] expectedSingletonOne = TEST_VALUES.get(udfClass).objectValueAt(0);
    final Object[] expectedSingletonTwo = TEST_VALUES.get(udfClass).objectValueAt(1);

    assertArrayEquals(expectedSingletonOne,
        propertyLanguageUdf.call(encode(CODING_A), "property_0", null));
    assertArrayEquals(expectedSingletonTwo,
        propertyLanguageUdf.call(encode(CODING_BB_VERSION1), "property_1", null));
  }

  @ParameterizedTest
  @MethodSource("inputs")
  public void testReturnsManyValueForKnownProperty(final Class<? extends Type> udfClass) {
    setupUdf(udfClass);
    setupLookup(terminologyService)
        .withPropertyLanguage(CODING_AA_VERSION1, "property_a", null,
            allTestValues().flatMap(Values::fhirRangeOne).toArray(Type[]::new))
        .withPropertyLanguage(CODING_B, "property_b", null,
            allTestValues().flatMap(Values::fhirRangeTwo).toArray(Type[]::new));

    final Object[] expectedArrayOne = TEST_VALUES.get(udfClass).objectRangeOne();
    final Object[] expectedArrayTwo = TEST_VALUES.get(udfClass).objectRangeTwo();

    assertArrayEquals(expectedArrayOne,
        propertyLanguageUdf.call(encode(CODING_AA_VERSION1), "property_a", null));
    assertArrayEquals(expectedArrayTwo,
        propertyLanguageUdf.call(encode(CODING_B), "property_b", null));
  }


  @ParameterizedTest
  @MethodSource("inputs")
  public void testReturnsManyValueForKnownPropertyLanguage(final Class<? extends Type> udfClass) {
    setupUdf(udfClass);
    setupLookup(terminologyService)
        .withPropertyLanguage(CODING_AA_VERSION1, "property_a", "de",
            allTestValues().flatMap(Values::fhirRangeOne).toArray(Type[]::new))
        .withPropertyLanguage(CODING_B, "property_b", "de",
            allTestValues().flatMap(Values::fhirRangeTwo).toArray(Type[]::new));

    final Object[] expectedArrayOne = TEST_VALUES.get(udfClass).objectRangeOne();
    final Object[] expectedArrayTwo = TEST_VALUES.get(udfClass).objectRangeTwo();

    assertArrayEquals(expectedArrayOne,
        propertyLanguageUdf.call(encode(CODING_AA_VERSION1), "property_a", "de"));
    assertArrayEquals(expectedArrayTwo,
        propertyLanguageUdf.call(encode(CODING_B), "property_b", "de"));
  }
}
