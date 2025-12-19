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

import static au.csiro.pathling.test.yaml.FhirTypedLiteral.toQuantity;

/**
 * Tests for FHIRPath conversion functions.
 *
 * @author Piotr Szul
 */
public class ConversionFunctionsDslTest extends FhirPathDslTestBase {

  @FhirPathTest
  public Stream<DynamicTest> testToBoolean() {
    return builder()
        .withSubject(sb -> sb
            .boolArray("boolArray", true, false, true)
            .integerArray("intArray", 0, 1, 2)
            .stringArray("stringArray", "true", "false", "yes")
            .dateArray("dateArray", "2023-01-15", "2023-12-25")
            .boolEmpty("emptyBool")
            .integerEmpty("emptyInt")
            .dateEmpty("emptyDate")
        )
        .group("toBoolean() with Boolean literals")
        .testTrue("true.toBoolean()", "toBoolean() returns true for true")
        .testFalse("false.toBoolean()", "toBoolean() returns false for false")

        .group("toBoolean() with String literals - true values")
        .testTrue("'true'.toBoolean()", "toBoolean() converts 'true'")
        .testTrue("'True'.toBoolean()", "toBoolean() converts 'True'")
        .testTrue("'TRUE'.toBoolean()", "toBoolean() converts 'TRUE'")
        .testTrue("'t'.toBoolean()", "toBoolean() converts 't'")
        .testTrue("'T'.toBoolean()", "toBoolean() converts 'T'")
        .testTrue("'yes'.toBoolean()", "toBoolean() converts 'yes'")
        .testTrue("'Yes'.toBoolean()", "toBoolean() converts 'Yes'")
        .testTrue("'YES'.toBoolean()", "toBoolean() converts 'YES'")
        .testTrue("'y'.toBoolean()", "toBoolean() converts 'y'")
        .testTrue("'Y'.toBoolean()", "toBoolean() converts 'Y'")
        .testTrue("'1'.toBoolean()", "toBoolean() converts '1'")
        .testTrue("'1.0'.toBoolean()", "toBoolean() converts '1.0'")

        .group("toBoolean() with String literals - false values")
        .testFalse("'false'.toBoolean()", "toBoolean() converts 'false'")
        .testFalse("'False'.toBoolean()", "toBoolean() converts 'False'")
        .testFalse("'FALSE'.toBoolean()", "toBoolean() converts 'FALSE'")
        .testFalse("'f'.toBoolean()", "toBoolean() converts 'f'")
        .testFalse("'F'.toBoolean()", "toBoolean() converts 'F'")
        .testFalse("'no'.toBoolean()", "toBoolean() converts 'no'")
        .testFalse("'No'.toBoolean()", "toBoolean() converts 'No'")
        .testFalse("'NO'.toBoolean()", "toBoolean() converts 'NO'")
        .testFalse("'n'.toBoolean()", "toBoolean() converts 'n'")
        .testFalse("'N'.toBoolean()", "toBoolean() converts 'N'")
        .testFalse("'0'.toBoolean()", "toBoolean() converts '0'")
        .testFalse("'0.0'.toBoolean()", "toBoolean() converts '0.0'")

        .group("toBoolean() with String literals - invalid values")
        .testEmpty("'notBoolean'.toBoolean()", "toBoolean() returns empty for invalid string")
        .testEmpty("'2'.toBoolean()", "toBoolean() returns empty for '2'")
        .testEmpty("'maybe'.toBoolean()", "toBoolean() returns empty for 'maybe'")

        .group("toBoolean() with Integer literals")
        .testTrue("1.toBoolean()", "toBoolean() converts 1 to true")
        .testFalse("0.toBoolean()", "toBoolean() converts 0 to false")
        .testEmpty("42.toBoolean()", "toBoolean() returns empty for other integers")

        .group("toBoolean() with Decimal literals")
        .testTrue("1.0.toBoolean()", "toBoolean() converts 1.0 to true")
        .testFalse("0.0.toBoolean()", "toBoolean() converts 0.0 to false")
        .testEmpty("3.14.toBoolean()", "toBoolean() returns empty for other decimals")

        .group("toBoolean() with non-convertible types")
        .testEmpty("@2023-01-15.toBoolean()", "toBoolean() returns empty for Date")

        .group("toBoolean() with empty values")
        .testEmpty("emptyBool.toBoolean()", "toBoolean() returns empty for empty Boolean")
        .testEmpty("emptyInt.toBoolean()", "toBoolean() returns empty for empty Integer")
        .testEmpty("emptyDate.toBoolean()", "toBoolean() returns empty for empty Date")

        .group("toBoolean() error cases with arrays")
        .testEmpty("{}.toBoolean()", "toBoolean() returns empty for empty collection")
        .testError("boolArray.toBoolean()", "toBoolean() errors on array of source type (Boolean)")
        .testError("intArray.toBoolean()",
            "toBoolean() errors on array of convertible type (Integer)")
        .testError("dateArray.toBoolean()",
            "toBoolean() errors on array of non-convertible type (Date)")

        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testConvertsToBoolean() {
    return builder()
        .withSubject(sb -> sb
            .boolArray("boolArray", true, false)
            .decimalArray("decArray", 1.0, 0.0)
            .dateArray("dateArray", "2023-01-15", "2023-12-25")
            .boolEmpty("emptyBool")
            .decimalEmpty("emptyDec")
            .dateEmpty("emptyDate")
        )
        .group("convertsToBoolean() with convertible literals")
        .testTrue("true.convertsToBoolean()",
            "convertsToBoolean() returns true for Boolean")
        .testTrue("'true'.convertsToBoolean()",
            "convertsToBoolean() returns true for 'true'")
        .testTrue("'t'.convertsToBoolean()", "convertsToBoolean() returns true for 't'")
        .testTrue("'yes'.convertsToBoolean()", "convertsToBoolean() returns true for 'yes'")
        .testTrue("'y'.convertsToBoolean()", "convertsToBoolean() returns true for 'y'")
        .testTrue("'1'.convertsToBoolean()", "convertsToBoolean() returns true for '1'")
        .testTrue("'1.0'.convertsToBoolean()", "convertsToBoolean() returns true for '1.0'")
        .testTrue("'false'.convertsToBoolean()",
            "convertsToBoolean() returns true for 'false'")
        .testTrue("1.convertsToBoolean()", "convertsToBoolean() returns true for 1")
        .testTrue("0.convertsToBoolean()", "convertsToBoolean() returns true for 0")
        .testTrue("1.0.convertsToBoolean()", "convertsToBoolean() returns true for 1.0")
        .testTrue("0.0.convertsToBoolean()", "convertsToBoolean() returns true for 0.0")

        .group("convertsToBoolean() with non-convertible literals")
        .testFalse("'notBoolean'.convertsToBoolean()",
            "convertsToBoolean() returns false for invalid string")
        .testFalse("42.convertsToBoolean()",
            "convertsToBoolean() returns false for other integer")
        .testFalse("3.14.convertsToBoolean()",
            "convertsToBoolean() returns false for other decimal")
        .testFalse("@2023-01-15.convertsToBoolean()",
            "convertsToBoolean() returns false for date")

        .group("convertsToBoolean() with empty values")
        .testEmpty("emptyBool.convertsToBoolean()",
            "convertsToBoolean() returns empty for empty Boolean")
        .testEmpty("emptyDec.convertsToBoolean()",
            "convertsToBoolean() returns empty for empty Decimal")
        .testEmpty("emptyDate.convertsToBoolean()",
            "convertsToBoolean() returns empty for empty Date")

        .group("convertsToBoolean() error cases with arrays")
        .testEmpty("{}.convertsToBoolean()",
            "convertsToBoolean() returns empty for empty collection")
        .testError("boolArray.convertsToBoolean()",
            "convertsToBoolean() errors on array of source type (Boolean)")
        .testError("decArray.convertsToBoolean()",
            "convertsToBoolean() errors on array of convertible type (Decimal)")
        .testError("dateArray.convertsToBoolean()",
            "convertsToBoolean() errors on array of non-convertible type (Date)")

        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testToInteger() {
    return builder()
        .withSubject(sb -> sb
            .integerArray("intArray", 1, 2, 3)
            .stringArray("stringArray", "1", "2", "3")
            .dateArray("dateArray", "2023-01-15", "2023-12-25")
            .integerEmpty("emptyInt")
            .boolEmpty("emptyBool")
            .dateEmpty("emptyDate")
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

        .group("toInteger() with empty values")
        .testEmpty("emptyInt.toInteger()", "toInteger() returns empty for empty Integer")
        .testEmpty("emptyBool.toInteger()", "toInteger() returns empty for empty Boolean")
        .testEmpty("emptyDate.toInteger()", "toInteger() returns empty for empty Date")

        .group("toInteger() error cases with arrays")
        .testEmpty("{}.toInteger()", "toInteger() returns empty for empty collection")
        .testError("intArray.toInteger()", "toInteger() errors on array of source type (Integer)")
        .testError("stringArray.toInteger()",
            "toInteger() errors on array of convertible type (String)")
        .testError("dateArray.toInteger()",
            "toInteger() errors on array of non-convertible type (Date)")

        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testConvertsToInteger() {
    return builder()
        .withSubject(sb -> sb
            .integerArray("intArray", 1, 2, 3)
            .boolArray("boolArray", true, false)
            .dateArray("dateArray", "2023-01-15", "2023-12-25")
            .integerEmpty("emptyInt")
            .boolEmpty("emptyBool")
            .dateEmpty("emptyDate")
        )
        .group("convertsToInteger() with convertible literals")
        .testTrue("true.convertsToInteger()",
            "convertsToInteger() returns true for Boolean")
        .testTrue("42.convertsToInteger()", "convertsToInteger() returns true for Integer")
        .testTrue("'123'.convertsToInteger()",
            "convertsToInteger() returns true for valid string")

        .group("convertsToInteger() with non-convertible literals")
        .testFalse("'notNumber'.convertsToInteger()",
            "convertsToInteger() returns false for invalid string")
        .testFalse("3.14.convertsToInteger()",
            "convertsToInteger() returns false for Decimal")
        .testFalse("@2023-01-15.convertsToInteger()",
            "convertsToInteger() returns false for Date")

        .group("convertsToInteger() with empty values")
        .testEmpty("emptyInt.convertsToInteger()",
            "convertsToInteger() returns empty for empty Integer")
        .testEmpty("emptyBool.convertsToInteger()",
            "convertsToInteger() returns empty for empty Boolean")
        .testEmpty("emptyDate.convertsToInteger()",
            "convertsToInteger() returns empty for empty Date")

        .group("convertsToInteger() error cases with arrays")
        .testEmpty("{}.convertsToInteger()",
            "convertsToInteger() returns empty for empty collection")
        .testError("intArray.convertsToInteger()",
            "convertsToInteger() errors on array of source type (Integer)")
        .testError("boolArray.convertsToInteger()",
            "convertsToInteger() errors on array of convertible type (Boolean)")
        .testError("dateArray.convertsToInteger()",
            "convertsToInteger() errors on array of non-convertible type (Date)")

        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testToDecimal() {
    return builder()
        .withSubject(sb -> sb
            .decimalArray("decArray", 1.1, 2.2, 3.3)
            .integerArray("intArray", 1, 2, 3)
            .dateArray("dateArray", "2023-01-15", "2023-12-25")
            .decimalEmpty("emptyDec")
            .integerEmpty("emptyInt")
            .dateEmpty("emptyDate")
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

        .group("toDecimal() with empty values")
        .testEmpty("emptyDec.toDecimal()", "toDecimal() returns empty for empty Decimal")
        .testEmpty("emptyInt.toDecimal()", "toDecimal() returns empty for empty Integer")
        .testEmpty("emptyDate.toDecimal()", "toDecimal() returns empty for empty Date")

        .group("toDecimal() error cases with arrays")
        .testEmpty("{}.toDecimal()", "toDecimal() returns empty for empty collection")
        .testError("decArray.toDecimal()", "toDecimal() errors on array of source type (Decimal)")
        .testError("intArray.toDecimal()",
            "toDecimal() errors on array of convertible type (Integer)")
        .testError("dateArray.toDecimal()",
            "toDecimal() errors on array of non-convertible type (Date)")

        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testToString() {
    return builder()
        .withSubject(sb -> sb
            .stringArray("stringArray", "hello", "world")
            .decimalArray("decArray", 1.1, 2.2)
            .dateArray("dateArray", "2023-01-15", "2023-12-25")
            .stringEmpty("emptyStr")
            .decimalEmpty("emptyDec")
            .dateEmpty("emptyDate")
        )
        .group("toString() with primitive type literals")
        .testEquals("true", "true.toString()", "toString() converts true")
        .testEquals("false", "false.toString()", "toString() converts false")
        .testEquals("42", "42.toString()", "toString() converts integer")
        .testEquals("3.14", "3.14.toString()", "toString() converts decimal without trailing zeros")
        .testEquals("hello", "'hello'.toString()", "toString() returns string as-is")

        .group("toString() with date/time literals")
        .testEquals("2023-01-15", "@2023-01-15.toString()", "toString() converts date")
        .testEquals("2023-01-15T10:30:00Z", "@2023-01-15T10:30:00Z.toString()",
            "toString() converts datetime")
        .testEquals("10:30:00", "@T10:30:00.toString()", "toString() converts time")

        .group("toString() with empty values")
        .testEmpty("emptyStr.toString()", "toString() returns empty for empty String")
        .testEmpty("emptyDec.toString()", "toString() returns empty for empty Decimal")
        .testEmpty("emptyDate.toString()", "toString() returns empty for empty Date")

        .group("toString() error cases with arrays")
        .testEmpty("{}.toString()", "toString() returns empty for empty collection")
        .testError("stringArray.toString()", "toString() errors on array of source type (String)")
        .testError("decArray.toString()",
            "toString() errors on array of convertible type (Decimal)")
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
            .stringEmpty("emptyStr")
            .integerEmpty("emptyInt")
            .dateEmpty("emptyDate")
        )
        .group("convertsToString() with convertible types")
        .testTrue("true.convertsToString()", "convertsToString() returns true for Boolean")
        .testTrue("42.convertsToString()", "convertsToString() returns true for Integer")
        .testTrue("3.14.convertsToString()", "convertsToString() returns true for Decimal")
        .testTrue("'hello'.convertsToString()",
            "convertsToString() returns true for String")
        .testTrue("@2023-01-15.convertsToString()",
            "convertsToString() returns true for Date")
        .testTrue("@2023-01-15T10:30:00Z.convertsToString()",
            "convertsToString() returns true for DateTime")
        .testTrue("@T10:30:00.convertsToString()",
            "convertsToString() returns true for Time")

        .group("convertsToString() with empty values")
        .testEmpty("emptyStr.convertsToString()",
            "convertsToString() returns empty for empty String")
        .testEmpty("emptyInt.convertsToString()",
            "convertsToString() returns empty for empty Integer")
        .testEmpty("emptyDate.convertsToString()",
            "convertsToString() returns empty for empty Date")

        .group("convertsToString() error cases with arrays")
        .testEmpty("{}.convertsToString()", "convertsToString() returns empty for empty collection")
        .testError("stringArray.convertsToString()",
            "convertsToString() errors on array of source type (String)")
        .testError("intArray.convertsToString()",
            "convertsToString() errors on array of convertible type (Integer)")
        .testError("dateArray.convertsToString()",
            "convertsToString() errors on array of convertible type (Date)")

        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testToDate() {
    return builder()
        .withSubject(sb -> sb
            .dateArray("dateArray", "2023-01-15", "2023-12-25")
            .stringArray("stringArray", "2023-01-15", "2023-12-25")
            .integerArray("intArray", 1, 2, 3)
            .dateEmpty("emptyDate")
            .stringEmpty("emptyStr")
            .integerEmpty("emptyInt")
        )
        .group("toDate() with String and Date literals")
        .testEquals("2023-01-15", "'2023-01-15'.toDate()", "toDate() converts valid date string")
        .testEquals("2023-12-25", "@2023-12-25.toDate()", "toDate() returns date as-is")
        .testEmpty("'notADate'.toDate()", "toDate() returns empty for invalid date string")

        .group("toDate() with invalid types")
        .testEmpty("42.toDate()", "toDate() returns empty for non-string")

        .group("toDate() with empty values")
        .testEmpty("emptyDate.toDate()", "toDate() returns empty for empty Date")
        .testEmpty("emptyStr.toDate()", "toDate() returns empty for empty String")
        .testEmpty("emptyInt.toDate()", "toDate() returns empty for empty Integer")

        .group("toDate() error cases with arrays")
        .testEmpty("{}.toDate()", "toDate() returns empty for empty collection")
        .testError("dateArray.toDate()", "toDate() errors on array of source type (Date)")
        .testError("stringArray.toDate()", "toDate() errors on array of convertible type (String)")
        .testError("intArray.toDate()",
            "toDate() errors on array of non-convertible type (Integer)")

        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testConvertsToDate() {
    return builder()
        .withSubject(sb -> sb
            .dateArray("dateArray", "2023-01-15", "2023-12-25")
            .stringArray("stringArray", "2023-01-15", "2023-12-25")
            .integerArray("intArray", 1, 2, 3)
            .dateEmpty("emptyDate")
            .stringEmpty("emptyStr")
            .integerEmpty("emptyInt")
        )
        .group("convertsToDate() with convertible types")
        .testTrue("'2023-01-15'.convertsToDate()",
            "convertsToDate() returns true for string")
        .testTrue("@2023-12-25.convertsToDate()", "convertsToDate() returns true for date")

        .group("convertsToDate() with non-convertible types")
        .testFalse("'notADate'.convertsToDate()",
            "convertsToDate() returns false for invalid date string")
        .testFalse("42.convertsToDate()", "convertsToDate() returns false for integer")

        .group("convertsToDate() with empty values")
        .testEmpty("emptyDate.convertsToDate()", "convertsToDate() returns empty for empty Date")
        .testEmpty("emptyStr.convertsToDate()", "convertsToDate() returns empty for empty String")
        .testEmpty("emptyInt.convertsToDate()", "convertsToDate() returns empty for empty Integer")

        .group("convertsToDate() error cases with arrays")
        .testEmpty("{}.convertsToDate()", "convertsToDate() returns empty for empty collection")
        .testError("dateArray.convertsToDate()",
            "convertsToDate() errors on array of source type (Date)")
        .testError("stringArray.convertsToDate()",
            "convertsToDate() errors on array of convertible type (String)")
        .testError("intArray.convertsToDate()",
            "convertsToDate() errors on array of non-convertible type (Integer)")

        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testToDateTime() {
    return builder()
        .withSubject(sb -> sb
            .dateTimeArray("dateTimeArray", "2023-01-15T10:30:00Z", "2023-12-25T12:00:00Z")
            .stringArray("stringArray", "2023-01-15T10:30:00Z", "2023-12-25T12:00:00Z")
            .integerArray("intArray", 1, 2, 3)
            .dateTimeEmpty("emptyDateTime")
            .stringEmpty("emptyStr")
            .integerEmpty("emptyInt")
        )
        .group("toDateTime() with String and DateTime literals")
        .testEquals("2023-01-15T10:30:00Z", "'2023-01-15T10:30:00Z'.toDateTime()",
            "toDateTime() converts valid datetime string")
        .testEquals("2023-12-25T12:00:00Z", "@2023-12-25T12:00:00Z.toDateTime()",
            "toDateTime() returns datetime as-is")
        .testEmpty("'not-a-datetime'.toDateTime()",
            "toDateTime() returns empty for invalid datetime string")

        .group("toDateTime() with invalid types")
        .testEmpty("42.toDateTime()", "toDateTime() returns empty for non-string")

        .group("toDateTime() with empty values")
        .testEmpty("emptyDateTime.toDateTime()", "toDateTime() returns empty for empty DateTime")
        .testEmpty("emptyStr.toDateTime()", "toDateTime() returns empty for empty String")
        .testEmpty("emptyInt.toDateTime()", "toDateTime() returns empty for empty Integer")

        .group("toDateTime() error cases with arrays")
        .testEmpty("{}.toDateTime()", "toDateTime() returns empty for empty collection")
        .testError("dateTimeArray.toDateTime()",
            "toDateTime() errors on array of source type (DateTime)")
        .testError("stringArray.toDateTime()",
            "toDateTime() errors on array of convertible type (String)")
        .testError("intArray.toDateTime()",
            "toDateTime() errors on array of non-convertible type (Integer)")

        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testConvertsToDateTime() {
    return builder()
        .withSubject(sb -> sb
            .dateTimeArray("dateTimeArray", "2023-01-15T10:30:00Z", "2023-12-25T12:00:00Z")
            .stringArray("stringArray", "2023-01-15T10:30:00Z", "2023-12-25T12:00:00Z")
            .integerArray("intArray", 1, 2, 3)
            .dateTimeEmpty("emptyDateTime")
            .stringEmpty("emptyStr")
            .integerEmpty("emptyInt")
        )
        .group("convertsToDateTime() with convertible types")
        .testTrue("'2023-01-15T10:30:00Z'.convertsToDateTime()",
            "convertsToDateTime() returns true for string")
        .testTrue("@2023-12-25T12:00:00Z.convertsToDateTime()",
            "convertsToDateTime() returns true for datetime")

        .group("convertsToDateTime() with non-convertible types")
        .testFalse("'not-a-datetime'.convertsToDateTime()",
            "convertsToDateTime() returns false for invalid datetime string")
        .testFalse("42.convertsToDateTime()",
            "convertsToDateTime() returns false for integer")

        .group("convertsToDateTime() with empty values")
        .testEmpty("emptyDateTime.convertsToDateTime()",
            "convertsToDateTime() returns empty for empty DateTime")
        .testEmpty("emptyStr.convertsToDateTime()",
            "convertsToDateTime() returns empty for empty String")
        .testEmpty("emptyInt.convertsToDateTime()",
            "convertsToDateTime() returns empty for empty Integer")

        .group("convertsToDateTime() error cases with arrays")
        .testEmpty("{}.convertsToDateTime()",
            "convertsToDateTime() returns empty for empty collection")
        .testError("dateTimeArray.convertsToDateTime()",
            "convertsToDateTime() errors on array of source type (DateTime)")
        .testError("stringArray.convertsToDateTime()",
            "convertsToDateTime() errors on array of convertible type (String)")
        .testError("intArray.convertsToDateTime()",
            "convertsToDateTime() errors on array of non-convertible type (Integer)")

        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testToTime() {
    return builder()
        .withSubject(sb -> sb
            .timeArray("timeArray", "10:30:00", "12:00:00")
            .stringArray("stringArray", "10:30:00", "12:00:00")
            .integerArray("intArray", 1, 2, 3)
            .timeEmpty("emptyTime")
            .stringEmpty("emptyStr")
            .integerEmpty("emptyInt")
        )
        .group("toTime() with String and Time literals")
        .testEquals("10:30:00", "'10:30:00'.toTime()", "toTime() converts valid time string")
        .testEquals("12:00:00", "@T12:00:00.toTime()", "toTime() returns time as-is")
        .testEmpty("'not-a-time'.toTime()", "toTime() returns empty for invalid time string")

        .group("toTime() with invalid types")
        .testEmpty("42.toTime()", "toTime() returns empty for non-string")

        .group("toTime() with empty values")
        .testEmpty("emptyTime.toTime()", "toTime() returns empty for empty Time")
        .testEmpty("emptyStr.toTime()", "toTime() returns empty for empty String")
        .testEmpty("emptyInt.toTime()", "toTime() returns empty for empty Integer")

        .group("toTime() error cases with arrays")
        .testEmpty("{}.toTime()", "toTime() returns empty for empty collection")
        .testError("timeArray.toTime()", "toTime() errors on array of source type (Time)")
        .testError("stringArray.toTime()", "toTime() errors on array of convertible type (String)")
        .testError("intArray.toTime()",
            "toTime() errors on array of non-convertible type (Integer)")

        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testConvertsToTime() {
    return builder()
        .withSubject(sb -> sb
            .timeArray("timeArray", "10:30:00", "12:00:00")
            .stringArray("stringArray", "10:30:00", "12:00:00")
            .integerArray("intArray", 1, 2, 3)
            .timeEmpty("emptyTime")
            .stringEmpty("emptyStr")
            .integerEmpty("emptyInt")
        )
        .group("convertsToTime() with convertible types")
        .testTrue("'10:30:00'.convertsToTime()",
            "convertsToTime() returns true for time string")
        .testFalse("'non-time'.convertsToTime()",
            "convertsToTime() returns false for non-time string")
        .testTrue("@T12:00:00.convertsToTime()", "convertsToTime() returns true for time")

        .group("convertsToTime() with non-convertible types")
        .testFalse("42.convertsToTime()", "convertsToTime() returns false for integer")

        .group("convertsToTime() with empty values")
        .testEmpty("emptyTime.convertsToTime()", "convertsToTime() returns empty for empty Time")
        .testEmpty("emptyStr.convertsToTime()", "convertsToTime() returns empty for empty String")
        .testEmpty("emptyInt.convertsToTime()", "convertsToTime() returns empty for empty Integer")

        .group("convertsToTime() error cases with arrays")
        .testEmpty("{}.convertsToTime()", "convertsToTime() returns empty for empty collection")
        .testError("timeArray.convertsToTime()",
            "convertsToTime() errors on array of source type (Time)")
        .testError("stringArray.convertsToTime()",
            "convertsToTime() errors on array of convertible type (String)")
        .testError("intArray.convertsToTime()",
            "convertsToTime() errors on array of non-convertible type (Integer)")

        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testConvertsToDecimal() {
    return builder()
        .withSubject(sb -> sb
            .decimalArray("decArray", 1.1, 2.2, 3.3)
            .stringArray("stringArray", "1.1", "2.2")
            .dateArray("dateArray", "2023-01-15", "2023-12-25")
            .decimalEmpty("emptyDec")
            .integerEmpty("emptyInt")
            .dateEmpty("emptyDate")
        )
        .group("convertsToDecimal() with convertible literals")
        .testTrue("true.convertsToDecimal()",
            "convertsToDecimal() returns true for Boolean")
        .testTrue("42.convertsToDecimal()", "convertsToDecimal() returns true for Integer")
        .testTrue("3.14.convertsToDecimal()",
            "convertsToDecimal() returns true for Decimal")
        .testTrue("'3.14159'.convertsToDecimal()",
            "convertsToDecimal() returns true for valid string")

        .group("convertsToDecimal() with non-convertible literals")
        .testFalse("'notNumber'.convertsToDecimal()",
            "convertsToDecimal() returns false for invalid string")

        .group("convertsToDecimal() with empty values")
        .testEmpty("emptyDec.convertsToDecimal()",
            "convertsToDecimal() returns empty for empty Decimal")
        .testEmpty("emptyInt.convertsToDecimal()",
            "convertsToDecimal() returns empty for empty Integer")
        .testEmpty("emptyDate.convertsToDecimal()",
            "convertsToDecimal() returns empty for empty Date")

        .group("convertsToDecimal() error cases with arrays")
        .testEmpty("{}.convertsToDecimal()",
            "convertsToDecimal() returns empty for empty collection")
        .testError("decArray.convertsToDecimal()",
            "convertsToDecimal() errors on array of source type (Decimal)")
        .testError("stringArray.convertsToDecimal()",
            "convertsToDecimal() errors on array of convertible type (String)")
        .testError("dateArray.convertsToDecimal()",
            "convertsToDecimal() errors on array of non-convertible type (Date)")

        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testToQuantity() {
    return builder()
        .withSubject(sb -> sb
            .integerArray("intArray", 1, 2, 3)
            .decimalArray("decArray", 1.5, 2.5, 3.5)
            .stringArray("stringArray", "10 'mg'", "4 days", "1.5 'kg'")
            .dateArray("dateArray", "2023-01-15", "2023-12-25")
            .boolEmpty("emptyBool")
            .integerEmpty("emptyInt")
            .stringEmpty("emptyStr")
            .dateEmpty("emptyDate")
        )
        .group("toQuantity() - Boolean sources")
        .testEquals(toQuantity("1 '1'"), "true.toQuantity()", "toQuantity() converts true to 1.0 '1'")
        .testEquals(toQuantity("0 '1'"), "false.toQuantity()", "toQuantity() converts false to 0.0 '1'")

        .group("toQuantity() - Integer sources")
        .testEquals(toQuantity("42 '1'"), "42.toQuantity()",
            "toQuantity() converts integer to quantity with unitCode '1'")

        .group("toQuantity() - Decimal sources")
        .testEquals(toQuantity("3.14 '1'"), "3.14.toQuantity()",
            "toQuantity() converts decimal to quantity with unitCode '1'")

        .group("toQuantity() - String sources (UCUM literals)")
        .testEquals(toQuantity("10 'mg'"), "'10 \\'mg\\''.toQuantity()",
            "toQuantity() parses UCUM quantity string")
        .testEquals(toQuantity("1.5 'kg'"), "'1.5 \\'kg\\''.toQuantity()",
            "toQuantity() parses decimal UCUM quantity")
        .testEquals(toQuantity("-5.2 'cm'"), "'-5.2 \\'cm\\''.toQuantity()",
            "toQuantity() parses negative UCUM quantity")

        .group("toQuantity() - String sources (calendar duration literals)")
        .testEquals(toQuantity("4 days"), "'4 days'.toQuantity()",
            "toQuantity() parses calendar duration (days)")
        .testEquals(toQuantity("1 year"), "'1 year'.toQuantity()",
            "toQuantity() parses calendar duration (year)")
        .testEquals(toQuantity("3 months"), "'3 months'.toQuantity()",
            "toQuantity() parses calendar duration (months)")

        .group("toQuantity() - String sources (numeric literals)")
        .testEquals(toQuantity("42 '1'"), "'42'.toQuantity()",
            "toQuantity() converts number string without unitCode to quantity with unitCode '1'")
        .testEquals(toQuantity("3.14 '1'"), "'3.14'.toQuantity()",
            "toQuantity() converts decimal string without unitCode to quantity with unitCode '1'")

        .group("toQuantity() - String sources (invalid)")
        .testEmpty("'notQuantity'.toQuantity()", "toQuantity() returns empty for invalid string")
        .testEmpty("'true'.toQuantity()",
            "toQuantity() returns empty for non-quantity string with boolean content")
        .testEmpty("'mg'.toQuantity()", "toQuantity() returns empty for unitCode without value")

        .group("toQuantity() - Non-convertible sources")
        .testEmpty("@2023-01-15.toQuantity()", "toQuantity() returns empty for Date")

        .group("toQuantity() - Empty values")
        .testEmpty("emptyBool.toQuantity()", "toQuantity() returns empty for empty Boolean")
        .testEmpty("emptyInt.toQuantity()", "toQuantity() returns empty for empty Integer")
        .testEmpty("emptyStr.toQuantity()", "toQuantity() returns empty for empty String")
        .testEmpty("emptyDate.toQuantity()", "toQuantity() returns empty for empty Date")

        .group("toQuantity() - Error cases (arrays)")
        .testEmpty("{}.toQuantity()", "toQuantity() returns empty for empty collection")
        .testError("intArray.toQuantity()",
            "toQuantity() errors on array of convertible type (Integer)")
        .testError("decArray.toQuantity()",
            "toQuantity() errors on array of convertible type (Decimal)")
        .testError("stringArray.toQuantity()",
            "toQuantity() errors on array of convertible type (String)")
        .testError("dateArray.toQuantity()",
            "toQuantity() errors on array of non-convertible type (Date)")

        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testToQuantityWithUnit() {
    return builder()
        .withSubject(sb -> sb
            .stringArray("stringArray", "1 'wk'", "1 'cm'", "1000 'g'")
            .stringEmpty("emptyStr")
        )
        .group("toQuantity(unitCode) - Numeric sources → exact unitCode match")
        .testEquals(toQuantity("42 '1'"), "42.toQuantity('1')",
            "toQuantity() with matching unitCode '1' returns quantity for integer")
        .testEquals(toQuantity("3.14 '1'"), "3.14.toQuantity('1')",
            "toQuantity() with matching unitCode '1' returns quantity for decimal")
        .testEquals(toQuantity("1 '1'"), "true.toQuantity('1')",
            "toQuantity() with matching unitCode '1' returns quantity for true")
        .testEquals(toQuantity("0 '1'"), "false.toQuantity('1')",
            "toQuantity() with matching unitCode '1' returns quantity for false")

        .group("toQuantity(unitCode) - Numeric sources → different unitCode (incompatible)")
        .testEmpty("42.toQuantity('mg')",
            "toQuantity() with different unitCode returns empty for integer")
        .testEmpty("3.14.toQuantity('kg')",
            "toQuantity() with different unitCode returns empty for decimal")
        .testEmpty("true.toQuantity('mg')",
            "toQuantity() with different unitCode returns empty for boolean")

        .group("toQuantity(unitCode) - UCUM string sources → exact unitCode match")
        .testEquals(toQuantity("10 'mg'"), "'10 \\'mg\\''.toQuantity('mg')",
            "toQuantity() with matching UCUM unitCode returns quantity")
        .testEquals(toQuantity("1.5 'kg'"), "'1.5 \\'kg\\''.toQuantity('kg')",
            "toQuantity() with matching UCUM unitCode returns quantity for decimal")
        .testEquals(toQuantity("-5.2 'cm'"), "'-5.2 \\'cm\\''.toQuantity('cm')",
            "toQuantity() with matching UCUM unitCode returns quantity for negative")

        .group("toQuantity(unitCode) - UCUM string sources → UCUM conversion (compatible)")
        .testEquals(toQuantity("0.01 'g'"), "'10 \\'mg\\''.toQuantity('g')",
            "toQuantity() converts mg to g (mass)")
        .testEquals(toQuantity("10 'mm'"), "'1 \\'cm\\''.toQuantity('mm')",
            "toQuantity() converts cm to mm (length)")
        .testEquals(toQuantity("1000 'mL'"), "'1 \\'L\\''.toQuantity('mL')",
            "toQuantity() converts L to mL (volume)")
        .testEquals(toQuantity("273.15 'K'"), "'0 \\'Cel\\''.toQuantity('K')",
            "toQuantity() converts Celsius to Kelvin (temperature, additive)")
        

        .group("toQuantity(unitCode) - UCUM string sources → incompatible units")
        .testEmpty("'1 \\'kg\\''.toQuantity('m')",
            "toQuantity() returns empty for incompatible units (mass to length)")

        .group("toQuantity(unitCode) - UCUM string sources → invalid target units")
        .testEmpty("'1 \\'kg\\''.toQuantity('invalid_unit')",
            "toQuantity() returns empty for invalid target unitCode")

        .group("toQuantity(unitCode) - Calendar duration sources → exact unitCode match")
        .testEquals(toQuantity("4 days"), "'4 days'.toQuantity('days')",
            "toQuantity() with matching calendar unitCode returns quantity")

        .group(
            "toQuantity(unitCode) - Calendar duration sources → calendar-to-calendar conversions")
        .testEquals(toQuantity("86400 seconds"), "'1 day'.toQuantity('seconds')",
            "toQuantity() converts calendar day to seconds")
        .testEquals(toQuantity("86400000 milliseconds"), "'1 day'.toQuantity('milliseconds')",
            "toQuantity() converts calendar day to milliseconds")

        .group("toQuantity(unitCode) - Calendar duration sources → calendar-to-UCUM conversions")
        .testEquals(toQuantity("120 's'"), "'2 minutes'.toQuantity('s')",
            "toQuantity() converts calendar minutes to UCUM 's'")
        .testEquals(toQuantity("1500 'ms'"), "'1500 milliseconds'.toQuantity('ms')",
            "toQuantity() converts calendar milliseconds to UCUM 'ms'")

        .group("toQuantity(unitCode) - Calendar duration sources → unsupported conversions")
        .testEmpty("'1 week'.toQuantity('months')",
            "toQuantity() returns empty for week to months (blocked)")

        .group("toQuantity(unitCode) - Numeric string sources → exact unitCode match")
        .testEquals(toQuantity("42 '1'"), "'42'.toQuantity('1')",
            "toQuantity() with matching unitCode '1' returns quantity for numeric string")
        .testEquals(toQuantity("3.14 '1'"), "'3.14'.toQuantity('1')",
            "toQuantity() with matching unitCode '1' returns quantity for decimal string")

        .group("toQuantity(unitCode) - Numeric string sources → different unitCode (incompatible)")
        .testEmpty("'42'.toQuantity('mg')",
            "toQuantity() with different unitCode returns empty for numeric string")
        .testEmpty("'3.14'.toQuantity('kg')",
            "toQuantity() with different unitCode returns empty for decimal string")

        .group("toQuantity(unitCode) - Empty values")
        .testEmpty("emptyStr.toQuantity('mg')",
            "toQuantity() returns empty for empty String with unitCode parameter")

        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testConvertsToQuantity() {
    return builder()
        .withSubject(sb -> sb
            .integerArray("intArray", 1, 2, 3)
            .boolArray("boolArray", true, false)
            .stringArray("stringArray", "10 'mg'", "4 days")
            .dateArray("dateArray", "2023-01-15", "2023-12-25")
            .boolEmpty("emptyBool")
            .integerEmpty("emptyInt")
            .stringEmpty("emptyStr")
            .dateEmpty("emptyDate")
        )
        .group("convertsToQuantity() - Boolean sources")
        .testTrue("true.convertsToQuantity()",
            "convertsToQuantity() returns true for Boolean")

        .group("convertsToQuantity() - Integer sources")
        .testTrue("42.convertsToQuantity()",
            "convertsToQuantity() returns true for Integer")

        .group("convertsToQuantity() - Decimal sources")
        .testTrue("3.14.convertsToQuantity()",
            "convertsToQuantity() returns true for Decimal")

        .group("convertsToQuantity() - String sources (UCUM literals)")
        .testTrue("'10 \\'mg\\''.convertsToQuantity()",
            "convertsToQuantity() returns true for UCUM string")

        .group("convertsToQuantity() - String sources (calendar duration literals)")
        .testTrue("'4 days'.convertsToQuantity()",
            "convertsToQuantity() returns true for calendar duration string")

        .group("convertsToQuantity() - String sources (numeric literals)")
        .testTrue("'42'.convertsToQuantity()",
            "convertsToQuantity() returns true for integer string without unitCode")
        .testTrue("'3.14'.convertsToQuantity()",
            "convertsToQuantity() returns true for decimal string without unitCode")

        .group("convertsToQuantity() - String sources (invalid)")
        .testFalse("'notQuantity'.convertsToQuantity()",
            "convertsToQuantity() returns false for invalid string")
        .testFalse("'true'.convertsToQuantity()",
            "convertsToQuantity() returns false for string with boolean content")

        .group("convertsToQuantity() - Non-convertible sources")
        .testFalse("@2023-01-15.convertsToQuantity()",
            "convertsToQuantity() returns false for Date")

        .group("convertsToQuantity() - Empty values")
        .testEmpty("emptyBool.convertsToQuantity()",
            "convertsToQuantity() returns empty for empty Boolean")
        .testEmpty("emptyInt.convertsToQuantity()",
            "convertsToQuantity() returns empty for empty Integer")
        .testEmpty("emptyStr.convertsToQuantity()",
            "convertsToQuantity() returns empty for empty String")
        .testEmpty("emptyDate.convertsToQuantity()",
            "convertsToQuantity() returns empty for empty Date")

        .group("convertsToQuantity() - Error cases (arrays)")
        .testEmpty("{}.convertsToQuantity()",
            "convertsToQuantity() returns empty for empty collection")
        .testError("boolArray.convertsToQuantity()",
            "convertsToQuantity() errors on array of convertible type (Boolean)")
        .testError("intArray.convertsToQuantity()",
            "convertsToQuantity() errors on array of convertible type (Integer)")
        .testError("stringArray.convertsToQuantity()",
            "convertsToQuantity() errors on array of convertible type (String)")
        .testError("dateArray.convertsToQuantity()",
            "convertsToQuantity() errors on array of non-convertible type (Date)")

        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testConvertsToQuantityWithUnit() {
    return builder()
        .withSubject(sb -> sb
            .stringArray("stringArray", "1 'wk'", "1 'cm'", "1000 'g'")
            .stringEmpty("emptyStr")
        )
        .group("convertsToQuantity(unitCode) - Numeric sources → exact unitCode match")
        .testTrue("42.convertsToQuantity('1')",
            "convertsToQuantity() with matching unitCode '1' returns true for integer")
        .testTrue("3.14.convertsToQuantity('1')",
            "convertsToQuantity() with matching unitCode '1' returns true for decimal")
        .testTrue("true.convertsToQuantity('1')",
            "convertsToQuantity() with matching unitCode '1' returns true for boolean")

        .group("convertsToQuantity(unitCode) - Numeric sources → different unitCode (incompatible)")
        .testFalse("42.convertsToQuantity('mg')",
            "convertsToQuantity() with different unitCode returns false for integer")
        .testFalse("3.14.convertsToQuantity('kg')",
            "convertsToQuantity() with different unitCode returns false for decimal")
        .testFalse("true.convertsToQuantity('mg')",
            "convertsToQuantity() with different unitCode returns false for boolean")

        .group("convertsToQuantity(unitCode) - UCUM string sources → exact unitCode match")
        .testTrue("'10 \\'mg\\''.convertsToQuantity('mg')",
            "convertsToQuantity() with matching UCUM unitCode returns true")
        .testTrue("'1.5 \\'kg\\''.convertsToQuantity('kg')",
            "convertsToQuantity() with matching UCUM unitCode returns true for decimal")

        .group("convertsToQuantity(unitCode) - UCUM string sources → UCUM conversion (compatible)")
        .testTrue("'10 \\'mg\\''.convertsToQuantity('g')",
            "convertsToQuantity() returns true for UCUM mass conversion")
        .testTrue("'1 \\'cm\\''.convertsToQuantity('mm')",
            "convertsToQuantity() returns true for UCUM length conversion")
        .testTrue("'1 \\'L\\''.convertsToQuantity('mL')",
            "convertsToQuantity() returns true for UCUM volume conversion")

        .group("convertsToQuantity(unitCode) - UCUM string sources → incompatible units")
        .testFalse("'1 \\'kg\\''.convertsToQuantity('m')",
            "convertsToQuantity() returns false for incompatible units (mass to length)")

        .group("convertsToQuantity(unitCode) - UCUM string sources → invalid target units")
        .testFalse("'1 \\'kg\\''.convertsToQuantity('invalid_unit')",
            "convertsToQuantity() returns false for invalid target unitCode")

        .group("convertsToQuantity(unitCode) - Calendar duration sources → exact unitCode match")
        .testTrue("'4 days'.convertsToQuantity('days')",
            "convertsToQuantity() with matching calendar unitCode returns true")

        .group(
            "convertsToQuantity(unitCode) - Calendar duration sources → calendar-to-calendar conversions")
        .testTrue("'1 day'.convertsToQuantity('seconds')",
            "convertsToQuantity() returns true for calendar day to seconds")
        .testTrue("'1 day'.convertsToQuantity('milliseconds')",
            "convertsToQuantity() returns true for calendar day to milliseconds")

        .group(
            "convertsToQuantity(unitCode) - Calendar duration sources → calendar-to-UCUM conversions")
        .testTrue("'2 minutes'.convertsToQuantity('s')",
            "convertsToQuantity() returns true for calendar minutes to UCUM 's'")
        .testTrue("'1500 milliseconds'.convertsToQuantity('ms')",
            "convertsToQuantity() returns true for calendar milliseconds to UCUM 'ms'")

        .group("convertsToQuantity(unitCode) - Calendar duration sources → unsupported conversions")
        .testFalse("'1 week'.convertsToQuantity('months')",
            "convertsToQuantity() returns false for week to months (blocked)")

        .group("convertsToQuantity(unitCode) - Numeric string sources → exact unitCode match")
        .testTrue("'42'.convertsToQuantity('1')",
            "convertsToQuantity() with matching unitCode '1' returns true for numeric string")
        .testTrue("'3.14'.convertsToQuantity('1')",
            "convertsToQuantity() with matching unitCode '1' returns true for decimal string")

        .group(
            "convertsToQuantity(unitCode) - Numeric string sources → different unitCode (incompatible)")
        .testFalse("'42'.convertsToQuantity('mg')",
            "convertsToQuantity() with different unitCode returns false for numeric string")
        .testFalse("'3.14'.convertsToQuantity('kg')",
            "convertsToQuantity() with different unitCode returns false for decimal string")

        .group("convertsToQuantity(unitCode) - Non-convertible sources")
        .testFalse("@2023-01-15.convertsToQuantity('1')",
            "convertsToQuantity() with unitCode returns false for non-convertible Date")
        .testFalse("'notQuantity'.convertsToQuantity('mg')",
            "convertsToQuantity() with unitCode returns false for invalid string")

        .group("convertsToQuantity(unitCode) - Empty values")
        .testEmpty("emptyStr.convertsToQuantity('mg')",
            "convertsToQuantity() returns empty for empty String with unitCode parameter")

        .build();
  }
}
