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

package au.csiro.pathling.fhirpath.dsl;

import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;

/**
 * Tests for FHIRPath conversion functions.
 *
 * @author John Grimes
 */
public class ConversionFunctionsDslTest extends FhirPathDslTestBase {

  @FhirPathTest
  public Stream<DynamicTest> testToBoolean() {
    return builder()
        .withSubject(sb -> sb
            .boolArray("boolArray", true, false, true)
            .integerArray("intArray", 0, 1, 2)
            .dateArray("dateArray", "2023-01-15", "2023-12-25")
        )
        .group("toBoolean() with Boolean literals")
        .testEquals(true, "true.toBoolean()", "toBoolean() returns true for true")
        .testEquals(false, "false.toBoolean()", "toBoolean() returns false for false")

        .group("toBoolean() with String literals - true values")
        .testEquals(true, "'true'.toBoolean()", "toBoolean() converts 'true'")
        .testEquals(true, "'True'.toBoolean()", "toBoolean() converts 'True'")
        .testEquals(true, "'TRUE'.toBoolean()", "toBoolean() converts 'TRUE'")
        .testEquals(true, "'t'.toBoolean()", "toBoolean() converts 't'")
        .testEquals(true, "'T'.toBoolean()", "toBoolean() converts 'T'")
        .testEquals(true, "'yes'.toBoolean()", "toBoolean() converts 'yes'")
        .testEquals(true, "'Yes'.toBoolean()", "toBoolean() converts 'Yes'")
        .testEquals(true, "'YES'.toBoolean()", "toBoolean() converts 'YES'")
        .testEquals(true, "'y'.toBoolean()", "toBoolean() converts 'y'")
        .testEquals(true, "'Y'.toBoolean()", "toBoolean() converts 'Y'")
        .testEquals(true, "'1'.toBoolean()", "toBoolean() converts '1'")
        .testEquals(true, "'1.0'.toBoolean()", "toBoolean() converts '1.0'")

        .group("toBoolean() with String literals - false values")
        .testEquals(false, "'false'.toBoolean()", "toBoolean() converts 'false'")
        .testEquals(false, "'False'.toBoolean()", "toBoolean() converts 'False'")
        .testEquals(false, "'FALSE'.toBoolean()", "toBoolean() converts 'FALSE'")
        .testEquals(false, "'f'.toBoolean()", "toBoolean() converts 'f'")
        .testEquals(false, "'F'.toBoolean()", "toBoolean() converts 'F'")
        .testEquals(false, "'no'.toBoolean()", "toBoolean() converts 'no'")
        .testEquals(false, "'No'.toBoolean()", "toBoolean() converts 'No'")
        .testEquals(false, "'NO'.toBoolean()", "toBoolean() converts 'NO'")
        .testEquals(false, "'n'.toBoolean()", "toBoolean() converts 'n'")
        .testEquals(false, "'N'.toBoolean()", "toBoolean() converts 'N'")
        .testEquals(false, "'0'.toBoolean()", "toBoolean() converts '0'")
        .testEquals(false, "'0.0'.toBoolean()", "toBoolean() converts '0.0'")

        .group("toBoolean() with String literals - invalid values")
        .testEmpty("'notBoolean'.toBoolean()", "toBoolean() returns empty for invalid string")
        .testEmpty("'2'.toBoolean()", "toBoolean() returns empty for '2'")
        .testEmpty("'maybe'.toBoolean()", "toBoolean() returns empty for 'maybe'")

        .group("toBoolean() with Integer literals")
        .testEquals(true, "1.toBoolean()", "toBoolean() converts 1 to true")
        .testEquals(false, "0.toBoolean()", "toBoolean() converts 0 to false")
        .testEmpty("42.toBoolean()", "toBoolean() returns empty for other integers")

        .group("toBoolean() with Decimal literals")
        .testEquals(true, "1.0.toBoolean()", "toBoolean() converts 1.0 to true")
        .testEquals(false, "0.0.toBoolean()", "toBoolean() converts 0.0 to false")
        .testEmpty("3.14.toBoolean()", "toBoolean() returns empty for other decimals")

        .group("toBoolean() with non-convertible types")
        .testEmpty("@2023-01-15.toBoolean()", "toBoolean() returns empty for Date")

        .group("toBoolean() error cases with arrays")
        .testEmpty("{}.toBoolean()", "toBoolean() returns empty for empty collection")
        .testError("boolArray.toBoolean()", "toBoolean() errors on array of source type (Boolean)")
        .testError("intArray.toBoolean()", "toBoolean() errors on array of convertible type (Integer)")
        .testError("dateArray.toBoolean()", "toBoolean() errors on array of non-convertible type (Date)")

        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testConvertsToBoolean() {
    return builder()
        .withSubject(sb -> sb
            .boolArray("boolArray", true, false)
            .decimalArray("decArray", 1.0, 0.0)
            .dateArray("dateArray", "2023-01-15", "2023-12-25")
        )
        .group("convertsToBoolean() with convertible literals")
        .testEquals(true, "true.convertsToBoolean()", "convertsToBoolean() returns true for Boolean")
        .testEquals(true, "'true'.convertsToBoolean()", "convertsToBoolean() returns true for 'true'")
        .testEquals(true, "'t'.convertsToBoolean()", "convertsToBoolean() returns true for 't'")
        .testEquals(true, "'yes'.convertsToBoolean()", "convertsToBoolean() returns true for 'yes'")
        .testEquals(true, "'y'.convertsToBoolean()", "convertsToBoolean() returns true for 'y'")
        .testEquals(true, "'1'.convertsToBoolean()", "convertsToBoolean() returns true for '1'")
        .testEquals(true, "'1.0'.convertsToBoolean()", "convertsToBoolean() returns true for '1.0'")
        .testEquals(true, "'false'.convertsToBoolean()", "convertsToBoolean() returns true for 'false'")
        .testEquals(true, "1.convertsToBoolean()", "convertsToBoolean() returns true for 1")
        .testEquals(true, "0.convertsToBoolean()", "convertsToBoolean() returns true for 0")
        .testEquals(true, "1.0.convertsToBoolean()", "convertsToBoolean() returns true for 1.0")
        .testEquals(true, "0.0.convertsToBoolean()", "convertsToBoolean() returns true for 0.0")

        .group("convertsToBoolean() with non-convertible literals")
        .testEquals(false, "'notBoolean'.convertsToBoolean()", "convertsToBoolean() returns false for invalid string")
        .testEquals(false, "42.convertsToBoolean()", "convertsToBoolean() returns false for other integer")
        .testEquals(false, "3.14.convertsToBoolean()", "convertsToBoolean() returns false for other decimal")
        .testEquals(false, "@2023-01-15.convertsToBoolean()", "convertsToBoolean() returns false for date")

        .group("convertsToBoolean() error cases with arrays")
        .testEmpty("{}.convertsToBoolean()", "convertsToBoolean() returns empty for empty collection")
        .testError("boolArray.convertsToBoolean()", "convertsToBoolean() errors on array of source type (Boolean)")
        .testError("decArray.convertsToBoolean()", "convertsToBoolean() errors on array of convertible type (Decimal)")
        .testError("dateArray.convertsToBoolean()", "convertsToBoolean() errors on array of non-convertible type (Date)")

        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testToInteger() {
    return builder()
        .withSubject(sb -> sb
            .integerArray("intArray", 1, 2, 3)
            .stringArray("stringArray", "1", "2", "3")
            .dateArray("dateArray", "2023-01-15", "2023-12-25")
        )
        .group("toInteger() with literal values")
        .testEquals(1, "true.toInteger()", "toInteger() converts true to 1")
        .testEquals(0, "false.toInteger()", "toInteger() converts false to 0")
        .testEquals(42, "42.toInteger()", "toInteger() returns integer as-is")
        .testEquals(123, "'123'.toInteger()", "toInteger() converts valid string")
        .testEquals(-42, "'-42'.toInteger()", "toInteger() converts negative string")
        .testEmpty("'notNumber'.toInteger()", "toInteger() returns empty for invalid string")
        .testEmpty("3.14.toInteger()", "toInteger() returns empty for Decimal (not in spec)")

        .group("toInteger() with non-convertible types")
        .testEmpty("@2023-01-15.toInteger()", "toInteger() returns empty for Date")

        .group("toInteger() error cases with arrays")
        .testEmpty("{}.toInteger()", "toInteger() returns empty for empty collection")
        .testError("intArray.toInteger()", "toInteger() errors on array of source type (Integer)")
        .testError("stringArray.toInteger()", "toInteger() errors on array of convertible type (String)")
        .testError("dateArray.toInteger()", "toInteger() errors on array of non-convertible type (Date)")

        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testConvertsToInteger() {
    return builder()
        .withSubject(sb -> sb
            .integerArray("intArray", 1, 2, 3)
            .boolArray("boolArray", true, false)
            .dateArray("dateArray", "2023-01-15", "2023-12-25")
        )
        .group("convertsToInteger() with convertible literals")
        .testEquals(true, "true.convertsToInteger()", "convertsToInteger() returns true for Boolean")
        .testEquals(true, "42.convertsToInteger()", "convertsToInteger() returns true for Integer")
        .testEquals(true, "'123'.convertsToInteger()", "convertsToInteger() returns true for valid string")

        .group("convertsToInteger() with non-convertible literals")
        .testEquals(false, "'notNumber'.convertsToInteger()", "convertsToInteger() returns false for invalid string")
        .testEquals(false, "3.14.convertsToInteger()", "convertsToInteger() returns false for Decimal")
        .testEquals(false, "@2023-01-15.convertsToInteger()", "convertsToInteger() returns false for Date")

        .group("convertsToInteger() error cases with arrays")
        .testEmpty("{}.convertsToInteger()", "convertsToInteger() returns empty for empty collection")
        .testError("intArray.convertsToInteger()", "convertsToInteger() errors on array of source type (Integer)")
        .testError("boolArray.convertsToInteger()", "convertsToInteger() errors on array of convertible type (Boolean)")
        .testError("dateArray.convertsToInteger()", "convertsToInteger() errors on array of non-convertible type (Date)")

        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testToDecimal() {
    return builder()
        .withSubject(sb -> sb
            .decimalArray("decArray", 1.1, 2.2, 3.3)
            .integerArray("intArray", 1, 2, 3)
            .dateArray("dateArray", "2023-01-15", "2023-12-25")
        )
        .group("toDecimal() with literal values")
        .testEquals(1.0, "true.toDecimal()", "toDecimal() converts true to 1.0")
        .testEquals(0.0, "false.toDecimal()", "toDecimal() converts false to 0.0")
        .testEquals(42.0, "42.toDecimal()", "toDecimal() converts integer")
        .testEquals(3.14, "3.14.toDecimal()", "toDecimal() returns decimal as-is")
        .testEquals(3.14159, "'3.14159'.toDecimal()", "toDecimal() converts valid string")
        .testEmpty("'notNumber'.toDecimal()", "toDecimal() returns empty for invalid string")

        .group("toDecimal() with non-convertible types")
        .testEmpty("@2023-01-15.toDecimal()", "toDecimal() returns empty for Date")

        .group("toDecimal() error cases with arrays")
        .testEmpty("{}.toDecimal()", "toDecimal() returns empty for empty collection")
        .testError("decArray.toDecimal()", "toDecimal() errors on array of source type (Decimal)")
        .testError("intArray.toDecimal()", "toDecimal() errors on array of convertible type (Integer)")
        .testError("dateArray.toDecimal()", "toDecimal() errors on array of non-convertible type (Date)")

        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testToString() {
    return builder()
        .withSubject(sb -> sb
            .stringArray("stringArray", "hello", "world")
            .decimalArray("decArray", 1.1, 2.2)
            .dateArray("dateArray", "2023-01-15", "2023-12-25")
        )
        .group("toString() with primitive type literals")
        .testEquals("true", "true.toString()", "toString() converts true")
        .testEquals("false", "false.toString()", "toString() converts false")
        .testEquals("42", "42.toString()", "toString() converts integer")
        .testEquals("3.14", "3.14.toString()", "toString() converts decimal without trailing zeros")
        .testEquals("hello", "'hello'.toString()", "toString() returns string as-is")

        .group("toString() with date/time literals")
        .testEquals("2023-01-15", "@2023-01-15.toString()", "toString() converts date")
        .testEquals("2023-01-15T10:30:00Z", "@2023-01-15T10:30:00Z.toString()", "toString() converts datetime")
        .testEquals("10:30:00", "@T10:30:00.toString()", "toString() converts time")

        .group("toString() error cases with arrays")
        .testEmpty("{}.toString()", "toString() returns empty for empty collection")
        .testError("stringArray.toString()", "toString() errors on array of source type (String)")
        .testError("decArray.toString()", "toString() errors on array of convertible type (Decimal)")
        .testError("dateArray.toString()", "toString() errors on array of convertible type (Date)")

        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testConvertsToString() {
    return builder()
        .withSubject(sb -> sb
            .stringArray("stringArray", "hello", "world")
            .integerArray("intArray", 1, 2, 3)
            .dateArray("dateArray", "2023-01-15", "2023-12-25")
        )
        .group("convertsToString() with convertible types")
        .testEquals(true, "true.convertsToString()", "convertsToString() returns true for Boolean")
        .testEquals(true, "42.convertsToString()", "convertsToString() returns true for Integer")
        .testEquals(true, "3.14.convertsToString()", "convertsToString() returns true for Decimal")
        .testEquals(true, "'hello'.convertsToString()", "convertsToString() returns true for String")
        .testEquals(true, "@2023-01-15.convertsToString()", "convertsToString() returns true for Date")
        .testEquals(true, "@2023-01-15T10:30:00Z.convertsToString()", "convertsToString() returns true for DateTime")
        .testEquals(true, "@T10:30:00.convertsToString()", "convertsToString() returns true for Time")

        .group("convertsToString() error cases with arrays")
        .testEmpty("{}.convertsToString()", "convertsToString() returns empty for empty collection")
        .testError("stringArray.convertsToString()", "convertsToString() errors on array of source type (String)")
        .testError("intArray.convertsToString()", "convertsToString() errors on array of convertible type (Integer)")
        .testError("dateArray.convertsToString()", "convertsToString() errors on array of convertible type (Date)")

        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testToDate() {
    return builder()
        .withSubject(sb -> sb
            .dateArray("dateArray", "2023-01-15", "2023-12-25")
            .stringArray("stringArray", "2023-01-15", "2023-12-25")
            .integerArray("intArray", 1, 2, 3)
        )
        .group("toDate() with String and Date literals")
        .testEquals("2023-01-15", "'2023-01-15'.toDate()", "toDate() converts valid date string")
        .testEquals("2023-12-25", "@2023-12-25.toDate()", "toDate() returns date as-is")
        .testEmpty("'notADate'.toDate()", "toDate() returns empty for invalid date string")

        .group("toDate() with invalid types")
        .testEmpty("42.toDate()", "toDate() returns empty for non-string")

        .group("toDate() error cases with arrays")
        .testEmpty("{}.toDate()", "toDate() returns empty for empty collection")
        .testError("dateArray.toDate()", "toDate() errors on array of source type (Date)")
        .testError("stringArray.toDate()", "toDate() errors on array of convertible type (String)")
        .testError("intArray.toDate()", "toDate() errors on array of non-convertible type (Integer)")

        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testConvertsToDate() {
    return builder()
        .withSubject(sb -> sb
            .dateArray("dateArray", "2023-01-15", "2023-12-25")
            .stringArray("stringArray", "2023-01-15", "2023-12-25")
            .integerArray("intArray", 1, 2, 3)
        )
        .group("convertsToDate() with convertible types")
        .testEquals(true, "'2023-01-15'.convertsToDate()", "convertsToDate() returns true for string")
        .testEquals(true, "@2023-12-25.convertsToDate()", "convertsToDate() returns true for date")

        .group("convertsToDate() with non-convertible types")
        .testEquals(false, "'notADate'.convertsToDate()", "convertsToDate() returns false for invalid date string")
        .testEquals(false, "42.convertsToDate()", "convertsToDate() returns false for integer")

        .group("convertsToDate() error cases with arrays")
        .testEmpty("{}.convertsToDate()", "convertsToDate() returns empty for empty collection")
        .testError("dateArray.convertsToDate()", "convertsToDate() errors on array of source type (Date)")
        .testError("stringArray.convertsToDate()", "convertsToDate() errors on array of convertible type (String)")
        .testError("intArray.convertsToDate()", "convertsToDate() errors on array of non-convertible type (Integer)")

        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testToDateTime() {
    return builder()
        .withSubject(sb -> sb
            .dateTimeArray("dateTimeArray", "2023-01-15T10:30:00Z", "2023-12-25T12:00:00Z")
            .stringArray("stringArray", "2023-01-15T10:30:00Z", "2023-12-25T12:00:00Z")
            .integerArray("intArray", 1, 2, 3)
        )
        .group("toDateTime() with String and DateTime literals")
        .testEquals("2023-01-15T10:30:00Z", "'2023-01-15T10:30:00Z'.toDateTime()", "toDateTime() converts valid datetime string")
        .testEquals("2023-12-25T12:00:00Z", "@2023-12-25T12:00:00Z.toDateTime()", "toDateTime() returns datetime as-is")
        .testEmpty("'not-a-datetime'.toDateTime()", "toDateTime() returns empty for invalid datetime string")

        .group("toDateTime() with invalid types")
        .testEmpty("42.toDateTime()", "toDateTime() returns empty for non-string")

        .group("toDateTime() error cases with arrays")
        .testEmpty("{}.toDateTime()", "toDateTime() returns empty for empty collection")
        .testError("dateTimeArray.toDateTime()", "toDateTime() errors on array of source type (DateTime)")
        .testError("stringArray.toDateTime()", "toDateTime() errors on array of convertible type (String)")
        .testError("intArray.toDateTime()", "toDateTime() errors on array of non-convertible type (Integer)")

        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testConvertsToDateTime() {
    return builder()
        .withSubject(sb -> sb
            .dateTimeArray("dateTimeArray", "2023-01-15T10:30:00Z", "2023-12-25T12:00:00Z")
            .stringArray("stringArray", "2023-01-15T10:30:00Z", "2023-12-25T12:00:00Z")
            .integerArray("intArray", 1, 2, 3)
        )
        .group("convertsToDateTime() with convertible types")
        .testEquals(true, "'2023-01-15T10:30:00Z'.convertsToDateTime()", "convertsToDateTime() returns true for string")
        .testEquals(true, "@2023-12-25T12:00:00Z.convertsToDateTime()", "convertsToDateTime() returns true for datetime")

        .group("convertsToDateTime() with non-convertible types")
        .testEquals(false, "'not-a-datetime'.convertsToDateTime()", "convertsToDateTime() returns false for invalid datetime string")
        .testEquals(false, "42.convertsToDateTime()", "convertsToDateTime() returns false for integer")

        .group("convertsToDateTime() error cases with arrays")
        .testEmpty("{}.convertsToDateTime()", "convertsToDateTime() returns empty for empty collection")
        .testError("dateTimeArray.convertsToDateTime()", "convertsToDateTime() errors on array of source type (DateTime)")
        .testError("stringArray.convertsToDateTime()", "convertsToDateTime() errors on array of convertible type (String)")
        .testError("intArray.convertsToDateTime()", "convertsToDateTime() errors on array of non-convertible type (Integer)")

        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testToTime() {
    return builder()
        .withSubject(sb -> sb
            .timeArray("timeArray", "10:30:00", "12:00:00")
            .stringArray("stringArray", "10:30:00", "12:00:00")
            .integerArray("intArray", 1, 2, 3)
        )
        .group("toTime() with String and Time literals")
        .testEquals("10:30:00", "'10:30:00'.toTime()", "toTime() converts valid time string")
        .testEquals("12:00:00", "@T12:00:00.toTime()", "toTime() returns time as-is")
        .testEmpty("'not-a-time'.toTime()", "toTime() returns empty for invalid time string")

        .group("toTime() with invalid types")
        .testEmpty("42.toTime()", "toTime() returns empty for non-string")

        .group("toTime() error cases with arrays")
        .testEmpty("{}.toTime()", "toTime() returns empty for empty collection")
        .testError("timeArray.toTime()", "toTime() errors on array of source type (Time)")
        .testError("stringArray.toTime()", "toTime() errors on array of convertible type (String)")
        .testError("intArray.toTime()", "toTime() errors on array of non-convertible type (Integer)")

        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testConvertsToTime() {
    return builder()
        .withSubject(sb -> sb
            .timeArray("timeArray", "10:30:00", "12:00:00")
            .stringArray("stringArray", "10:30:00", "12:00:00")
            .integerArray("intArray", 1, 2, 3)
        )
        .group("convertsToTime() with convertible types")
        .testEquals(true, "'10:30:00'.convertsToTime()", "convertsToTime() returns true for time string")
        .testEquals(false, "'non-time'.convertsToTime()", "convertsToTime() returns false for non-time string")
        .testEquals(true, "@T12:00:00.convertsToTime()", "convertsToTime() returns true for time")

        .group("convertsToTime() with non-convertible types")
        .testEquals(false, "42.convertsToTime()", "convertsToTime() returns false for integer")

        .group("convertsToTime() error cases with arrays")
        .testEmpty("{}.convertsToTime()", "convertsToTime() returns empty for empty collection")
        .testError("timeArray.convertsToTime()", "convertsToTime() errors on array of source type (Time)")
        .testError("stringArray.convertsToTime()", "convertsToTime() errors on array of convertible type (String)")
        .testError("intArray.convertsToTime()", "convertsToTime() errors on array of non-convertible type (Integer)")

        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testToLong() {
    return builder()
        .withSubject(sb -> sb
            .integerArray("intArray", 1, 2, 3)
            .stringArray("stringArray", "123", "456")
            .dateArray("dateArray", "2023-01-15", "2023-12-25")
        )
        .group("toLong() with literal values")
        .testEquals(1, "true.toLong()", "toLong() converts true to 1")
        .testEquals(0, "false.toLong()", "toLong() converts false to 0")
        .testEquals(42, "42.toLong()", "toLong() returns integer as-is")
        .testEquals(123456789, "'123456789'.toLong()", "toLong() converts valid string")
        .testEmpty("'notNumber'.toLong()", "toLong() returns empty for invalid string")

        .group("toLong() with non-convertible types")
        .testEmpty("@2023-01-15.toLong()", "toLong() returns empty for Date")

        .group("toLong() error cases with arrays")
        .testEmpty("{}.toLong()", "toLong() returns empty for empty collection")
        .testError("intArray.toLong()", "toLong() errors on array of source type (Integer)")
        .testError("stringArray.toLong()", "toLong() errors on array of convertible type (String)")
        .testError("dateArray.toLong()", "toLong() errors on array of non-convertible type (Date)")

        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testConvertsToLong() {
    return builder()
        .withSubject(sb -> sb
            .integerArray("intArray", 1, 2, 3)
            .boolArray("boolArray", true, false)
            .dateArray("dateArray", "2023-01-15", "2023-12-25")
        )
        .group("convertsToLong() with convertible literals")
        .testEquals(true, "true.convertsToLong()", "convertsToLong() returns true for Boolean")
        .testEquals(true, "42.convertsToLong()", "convertsToLong() returns true for Integer")
        .testEquals(true, "'123456789'.convertsToLong()", "convertsToLong() returns true for valid string")

        .group("convertsToLong() with non-convertible literals")
        .testEquals(false, "'notNumber'.convertsToLong()", "convertsToLong() returns false for invalid string")

        .group("convertsToLong() error cases with arrays")
        .testEmpty("{}.convertsToLong()", "convertsToLong() returns empty for empty collection")
        .testError("intArray.convertsToLong()", "convertsToLong() errors on array of source type (Integer)")
        .testError("boolArray.convertsToLong()", "convertsToLong() errors on array of convertible type (Boolean)")
        .testError("dateArray.convertsToLong()", "convertsToLong() errors on array of non-convertible type (Date)")

        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testConvertsToDecimal() {
    return builder()
        .withSubject(sb -> sb
            .decimalArray("decArray", 1.1, 2.2, 3.3)
            .stringArray("stringArray", "1.1", "2.2")
            .dateArray("dateArray", "2023-01-15", "2023-12-25")
        )
        .group("convertsToDecimal() with convertible literals")
        .testEquals(true, "true.convertsToDecimal()", "convertsToDecimal() returns true for Boolean")
        .testEquals(true, "42.convertsToDecimal()", "convertsToDecimal() returns true for Integer")
        .testEquals(true, "3.14.convertsToDecimal()", "convertsToDecimal() returns true for Decimal")
        .testEquals(true, "'3.14159'.convertsToDecimal()", "convertsToDecimal() returns true for valid string")

        .group("convertsToDecimal() with non-convertible literals")
        .testEquals(false, "'notNumber'.convertsToDecimal()", "convertsToDecimal() returns false for invalid string")

        .group("convertsToDecimal() error cases with arrays")
        .testEmpty("{}.convertsToDecimal()", "convertsToDecimal() returns empty for empty collection")
        .testError("decArray.convertsToDecimal()", "convertsToDecimal() errors on array of source type (Decimal)")
        .testError("stringArray.convertsToDecimal()", "convertsToDecimal() errors on array of convertible type (String)")
        .testError("dateArray.convertsToDecimal()", "convertsToDecimal() errors on array of non-convertible type (Date)")

        .build();
  }
}
