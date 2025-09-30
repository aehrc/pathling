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

public class EqualityOperatorsDslTest extends FhirPathDslTestBase {

  @FhirPathTest
  public Stream<DynamicTest> testBooleanEquality() {
    return builder()
        .withSubject(sb -> sb
            .boolArray("allTrue", true, true)
            .boolArray("allFalse", false, false)
            .boolArray("allFalse3", false, false, false)
            .boolEmpty("emptyBool")
        )
        .group("Boolean equality: singleton vs singleton")
        .testTrue("true = true", "true equals true")
        .testTrue("false = false", "false equals false")
        .testFalse("true = false", "true not equals false")
        .testTrue("true != false", "true not equals false (!=)")
        .testFalse("true != true", "true equals true (not !=)")
        .group("Boolean equality: singleton vs explicit empty")
        .testEmpty("true = {}", "true equals explicit empty")
        .testEmpty("true != {}", "true not equals explicit empty")
        .testEmpty("false = {}", "false equals explicit empty")
        .testEmpty("false != {}", "false not equals explicit empty")
        .group("Boolean equality: singleton vs computed empty")
        .testEmpty("true = true.where(false)", "true equals computed empty")
        .testEmpty("emptyBool != false", "true not equals computed empty")
        .group("Boolean equality: array vs singleton")
        .testFalse("allTrue = true", "array equals singleton")
        .testTrue("false != allFalse", "singleton not equals array")
        .group("Boolean equality: array vs array")
        .testTrue("allTrue = allTrue", "array equals itself")
        .testTrue("allFalse = allFalse", "array equals itself (all false)")
        .testFalse("allTrue = allFalse", "array not equals different array")
        .testTrue("allTrue != allFalse", "array not equals different array (!=)")
        .testTrue("allFalse3 != allFalse", "arrays of different sizes not equals")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testStringEquality() {
    return builder()
        .withSubject(sb -> sb
            .string("str1", "abc")
            .string("str2", "efg")
            .stringEmpty("strEmpty")
            .stringArray("strArray1", "a", "b")
            .stringArray("strArray2", "a", "b", "c")
            .stringArray("strArray3", "a", "b", "d")
        )
        .group("String equality: singleton vs singleton")
        .testTrue("str1 = 'abc'", "str1 equals 'abc'")
        .testFalse("str1 = str2", "str1 not equals str2")
        .testTrue("str1 != str2", "str1 not equals str2 (!=)")
        .testFalse("str1 != 'abc'", "str1 equals 'abc' (not !=)")
        .group("String equality: singleton vs explicit empty")
        .testEmpty("str1 = {}", "str1 equals explicit empty")
        .testEmpty("str1 != {}", "str1 not equals explicit empty")
        .testEmpty("strEmpty = 'abc'", "empty string equals 'abc'")
        .testEmpty("strEmpty != 'abc'", "empty string not equals 'abc'")
        .group("String equality: singleton vs computed empty")
        .testEmpty("str1 = str1.where(false)", "str1 equals computed empty")
        .testEmpty("strEmpty != str2", "empty string not equals str2 (computed empty)")
        .group("String equality: array vs singleton")
        .testFalse("strArray1 = 'a'", "array equals singleton")
        .testTrue("'d' != strArray2", "singleton not equals array")
        .group("String equality: array vs array")
        .testTrue("strArray1 = strArray1", "array equals itself")
        .testFalse("strArray2 != strArray2", "array equals itself (not !=)")
        .testFalse("strArray1 = strArray2", "array not equals different array")
        .testTrue("strArray2 != strArray3", "array not equals different array (!=)")
        .testTrue("strArray1 != strArray2", "array not equals different array size (!=)")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testNumericEquality() {
    return builder()
        .withSubject(sb -> sb
            .integer("int1", 5)
            .integer("int2", 2)
            .integer("int3", 5)
            .decimal("dec1", 5.0)
            .decimal("dec2", 2.5)
            .decimal("dec3", 5.0)
            .integerArray("intArray1", 1, 2, 3)
            .integerArray("intArray2", 1, 2, 4)
            .decimalArray("decArray1", 1.0, 2.0, 3.0)
            .decimalArray("decArray2", 1.0, 2.0, 4.0)
            .integerArray("emptyIntArray")
            .decimalArray("emptyDecArray")
        )
        .group("Numeric equality: integer vs integer")
        .testTrue("int1 = int3", "int1 equals int3 (5 = 5)")
        .testFalse("int1 = int2", "int1 not equals int2 (5 != 2)")
        .testTrue("int1 != int2", "int1 not equals int2 (!=)")
        .testFalse("int1 != int3", "int1 equals int3 (not !=)")
        .group("Numeric equality: decimal vs decimal")
        .testTrue("dec1 = dec3", "dec1 equals dec3 (5.0 = 5.0)")
        .testFalse("dec1 = dec2", "dec1 not equals dec2 (5.0 != 2.5)")
        .testTrue("dec1 != dec2", "dec1 not equals dec2 (!=)")
        .testFalse("dec1 != dec3", "dec1 equals dec3 (not !=)")
        .group("Numeric equality: integer vs decimal")
        .testTrue("int1 = dec1", "int1 equals dec1 (5 = 5.0)")
        .testTrue("dec1 = int1", "dec1 equals int1 (5.0 = 5)")
        .testFalse("int2 = dec1", "int2 not equals dec1 (2 != 5.0)")
        .testTrue("int2 != dec1", "int2 not equals dec1 (!=)")
        .group("Numeric equality: singleton vs explicit empty")
        .testEmpty("int1 = {}", "int1 equals explicit empty")
        .testEmpty("dec1 = {}", "dec1 equals explicit empty")
        .testEmpty("int1 != {}", "int1 not equals explicit empty")
        .testEmpty("dec1 != {}", "dec1 not equals explicit empty")
        .group("Numeric equality: array vs singleton")
        .testFalse("intArray1 = 1", "int array equals singleton")
        .testFalse("decArray1 = 1.0", "decimal array equals singleton")
        .group("Numeric equality: array vs array")
        .testTrue("intArray1 = intArray1", "int array equals itself")
        .testFalse("intArray1 = intArray2", "int array not equals different array")
        .testFalse("intArray1 != intArray1", "int array not equals itself")
        .testTrue("intArray1 != intArray2", "int array not equals different array")
        .testTrue("decArray1 = decArray1", "decimal array equals itself")
        .testFalse("decArray1 = decArray2", "decimal array not equals different array")
        .group("Numeric equality: array vs array of different type")
        .testTrue("intArray1 = decArray1", "int array equals decimal array")
        .testFalse("intArray1 != decArray1", "int array not equals decimal array (!=)")
        .testTrue("intArray1 != decArray2", "int array not equals different decimal array (!=)")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testTimeEquality() {
    return builder()
        .withSubject(sb -> sb
            .time("t1", "12:00")
            .time("t2", "13:00")
            .time("t3", "12:00:00")
            .time("t4", "12:00:00.000")
            .timeArray("timeArray1", "12:00", "13:00")
            .timeArray("timeArray2", "12:00", "12:00")
            .timeArray("timeArray3", "12:00", "12:00:00")
            .timeArray("timeArray4", "12:00:00", "12:00:00")
            .timeArray("timeArray5", "12:00")
            .timeArray("timeArray6", "12:00", "13:00", "14:00")
            .timeArray("timeArray7", "12:00:00", "13:00:00")
            .timeArray("emptyTimeArray")
        )
        .group("Time equality: singleton vs singleton, same precision")
        .testTrue("t1 = @T12:00", "t1 equals @T12:00")
        .testFalse("t1 = t2", "t1 not equals t2")
        .testTrue("t1 != t2", "t1 not equals t2 (!=)")
        .testFalse("t1 != @T12:00", "t1 equals @T12:00 (not !=)")
        .group("Time equality: singleton vs singleton, different precision")
        .testEmpty("t1 = t3", "t1 equals t3 (different precision)")
        .testEmpty("t1 != t3", "t1 not equals t3 (different precision)")
        .testEmpty("t1 = t4", "t1 equals t4 (different precision)")
        .testTrue("t3 = t4", "t3 equals t4 (both fractional seconds)")
        .group("Time equality: singleton vs explicit empty")
        .testEmpty("t1 = {}", "t1 equals explicit empty")
        .testEmpty("t1 != {}", "t1 not equals explicit empty")
        .group("Time equality: singleton vs computed empty")
        .testEmpty("t1 = t1.where(false)", "t1 equals computed empty")
        .testEmpty("t1 != t1.where(false)", "t1 not equals computed empty")
        .group("Time equality: array vs singleton, all comparable")
        .testFalse("timeArray1 = t1", "array equals singleton (one matches, one does not)")
        .testTrue("timeArray2 != t1", "array does equals singleton")
        .group("Time equality: array vs singleton, some incomparable")
        .testFalse("timeArray3 = t1", "array equals singleton (one comparable, one incomparable)")
        .group("Time equality: array vs array, all comparable")
        .testTrue("timeArray1 = timeArray1", "array equals itself")
        .testFalse("timeArray1 = timeArray2", "array not equals different array")
        .testTrue("timeArray2 = timeArray2", "array equals itself (all same)")
        .group("Time equality: array vs array, some incomparable")
        .testFalse("timeArray3 = timeArray4", "array not equals (different precision)")
        .testTrue("timeArray3 != timeArray2", "array not equals (one element different precision)")
        .group("Time equality: array vs array, different sizes")
        .testFalse("timeArray5 = timeArray1", "array not equals different size")
        .testFalse("timeArray6 = timeArray7", "array not equals different size and precision")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testDateAndDateTimeEquality() {
    return builder()
        .withSubject(sb -> sb
            .dateArray("dateArray1", "2020-01-01", "2020-01-02")
            .dateArray("dateArray2", "2020-01-01", "2020-01-01")
            .dateArray("dateArray3", "2020-01-01", "2020-01")
            .dateArray("dateArray4", "2020-01", "2020-01")
            .dateArray("dateArray5", "2020-01-01")
            .dateArray("dateArray6", "2020-01-01", "2020-01-02", "2020-01-03")
            .dateArray("emptyDateArray")
            .dateTimeArray("dtArray1", "2020-01-01T10:00:00+00:00", "2020-01-01T11:00:00+00:00")
            .dateTimeArray("dtArray2", "2020-01-01T10:00:00+00:00", "2020-01-01T10:00:00+00:00")
            .dateTimeArray("dtArray3", "2020-01-01T10:00:00+00:00", "2020-01-01T10:00:00")
            .dateTimeArray("dtArray4", "2020-01-01T10:00:00", "2020-01-01T10:00:00")
            .dateTimeArray("dtArray5", "2020-01-01T10:00:00+00:00")
            .dateTimeArray("dtArray6", "2020-01-01T10:00:00+00:00", "2020-01-01T11:00:00+00:00",
                "2020-01-01T12:00:00+00:00")
            .dateTimeArray("emptyDateTimeArray")
        )
        .group("Date equality: singleton vs singleton, same precision")
        .testTrue("@2020-01-01 = @2020-01-01", "date literal equals itself")
        .testFalse("@2020-01-01 = @2020-01-02", "date literal not equals different date")
        .testTrue("@2020-01-01 != @2020-01-02", "date literal not equals different date (!=)")
        .testFalse("@2020-01-01 != @2020-01-01", "date literal equals itself (not !=)")
        .group("Date equality: singleton vs singleton, different precision")
        .testEmpty("@2020-01-01 = @2020-01", "date literal equals different precision")
        .testEmpty("@2020-01-01 != @2020-01", "date literal not equals different precision")
        .testEmpty("@2020-01 = @2020", "date literal equals different precision")
        .group("Date equality: singleton vs explicit empty")
        .testEmpty("@2020-01-01 = {}", "date literal equals explicit empty")
        .testEmpty("@2020-01-01 != {}", "date literal not equals explicit empty")
        .group("Date equality: singleton vs computed empty")
        .testEmpty("@2020-01-01 = @2020-01-01.where(false)", "date literal equals computed empty")
        .testEmpty("@2020-01-01 != @2020-01-01.where(false)",
            "date literal not equals computed empty")
        .group("Date equality: array vs singleton, all comparable")
        .testFalse("dateArray1 = @2020-01-01", "array equals singleton (one matches, one does not)")
        .testTrue("dateArray2 != @2020-01-01", "array differs from singleton (all match)")
        .group("Date equality: array vs singleton, some incomparable")
        .testFalse("dateArray3 = @2020-01-01",
            "array equals singleton (one comparable, one incomparable)")
        .group("Date equality: array vs array, all comparable")
        .testTrue("dateArray1 = dateArray1", "array equals itself")
        .testFalse("dateArray1 = dateArray2", "array not equals different array")
        .testTrue("dateArray2 = dateArray2", "array equals itself (all same)")
        .group("Date equality: array vs array, some incomparable")
        .testFalse("dateArray3 = dateArray4", "array not equals (different precision)")
        .testTrue("dateArray3 != dateArray2", "array not equals (one element different precision)")
        .group("Date equality: array vs array, different sizes")
        .testFalse("dateArray5 = dateArray1", "array not equals different size")
        .testFalse("dateArray6 = dateArray1", "array not equals different size and precision")
        .group("DateTime equality: singleton vs singleton, same precision")
        .testTrue("@2020-01-01T10:00:00+00:00 = @2020-01-01T10:00:00+00:00",
            "dateTime literal equals itself")
        .testFalse("@2020-01-01T10:00:00+00:00 = @2020-01-01T11:00:00+00:00",
            "dateTime literal not equals different instant")
        .testTrue("@2020-01-01T10:00:00+00:00 != @2020-01-01T11:00:00+00:00",
            "dateTime literal not equals different instant (!=)")
        .testFalse("@2020-01-01T10:00:00+00:00 != @2020-01-01T10:00:00+00:00",
            "dateTime literal equals itself (not !=)")
        .group("DateTime equality: singleton vs singleton, different precision")
        .testTrue("@2020-01-01T10:00:00+00:00 = @2020-01-01T10:00:00",
            "dateTime literal equals different one with timezone vs no timezone")
        .testFalse("@2020-01-01T10:00:00+00:00 != @2020-01-01T10:00:00.000",
            "dateTime literal equals different one with fractional seconds vs no fractional seconds")
        .testEmpty("@2020-01-01T10:00:00 = @2020-01-01T10:00",
            "dateTime literal equals different precision")
        .group("DateTime equality: singleton vs explicit empty")
        .testEmpty("@2020-01-01T10:00:00+00:00 = {}", "dateTime literal equals explicit empty")
        .testEmpty("@2020-01-01T10:00:00+00:00 != {}", "dateTime literal not equals explicit empty")
        .group("DateTime equality: singleton vs computed empty")
        .testEmpty("@2020-01-01T10:00:00+00:00 = @2020-01-01T10:00:00+00:00.where(false)",
            "dateTime literal equals computed empty")
        .testEmpty("@2020-01-01T10:00:00+00:00 != @2020-01-01T10:00:00+00:00.where(false)",
            "dateTime literal not equals computed empty")
        .group("DateTime equality: array vs singleton, all comparable")
        .testFalse("dtArray1 = @2020-01-01T10:00:00+00:00",
            "array equals singleton (one matches, one does not)")
        .testTrue("dtArray2 != @2020-01-01T10:00:00+00:00",
            "array differs from singleton (all match)")
        .group("DateTime equality: array vs singleton, some incomparable")
        .testFalse("dtArray3 = @2020-01-01T10:00:00+00:00",
            "array equals singleton (one comparable, one incomparable)")
        .group("DateTime equality: array vs array, all comparable")
        .testTrue("dtArray1 = dtArray1", "array equals itself")
        .testFalse("dtArray1 = dtArray2", "array not equals different array")
        .testTrue("dtArray2 = dtArray2", "array equals itself (all same)")
        .group("DateTime equality: array vs array, some incomparable")
        .testTrue("dtArray3 = dtArray4", "array equals (some with TZ, some without)")
        .testFalse("dtArray3 != dtArray2", "array not equals (some with TZ, some without)")
        .group("DateTime equality: array vs array, different sizes")
        .testFalse("dtArray5 = dtArray1", "array not equals different size")
        .testFalse("dtArray6 = dtArray1", "array not equals different size and precision")
        .group("Date vs DateTime equality: singleton vs singleton")
        .testEmpty("@2020-01-01 = @2020-01-01T00:00",
            "date vs dateTime with different precision (missing timezone)")
        .testEmpty("@2020-01-01 = @2020-01-01T00", "date vs dateTime with hour precision")
        .group("Date vs DateTime equality: array vs array")
        .testFalse("dateArray1 = dtArray1",
            "date array equals dateTime array (all match at midnight)")
        .testTrue("dtArray2 != dateArray1", "dateTime array not equals date array (!=)")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testCodingEquality() {
    return builder()
        .withSubject(sb -> sb
            .coding("noCoding", null)
            .coding("oneCoding", "http://loinc.org|8867-4||'Heart rate'")
            .codingArray("manyCoding",
                "http://loinc.org|8480-6||'Systolic blood pressure'",
                "http://loinc.org|8867-4||'Heart rate'"
            )
            .codingArray("manyCoding1",
                "http://loinc.org|8480-6||'Systolic blood pressure'",
                "http://loinc.org|8462-4||'Diastolic blood pressure'"
            )
        )
        .group("Coding equality: singleton vs singleton (literals)")
        .testTrue("http://loinc.org|8867-4||'Heart rate' = http://loinc.org|8867-4||'Heart rate'",
            "coding literal equals itself")
        .testFalse(
            "http://loinc.org|8867-4||'Heart rate' = http://loinc.org|8480-6||'Systolic blood pressure'",
            "coding literal not equals different code")
        .testTrue(
            "http://loinc.org|8867-4||'Heart rate' != http://loinc.org|8480-6||'Systolic blood pressure'",
            "coding literal not equals coding (!=)")
        .testFalse("http://loinc.org|8867-4||'Heart rate' != http://loinc.org|8867-4||'Heart rate'",
            "coding literal equals itself (not !=)")
        .group("Coding equality: singleton vs explicit empty")
        .testEmpty("http://loinc.org|8867-4||'Heart rate' = {}",
            "coding literal equals explicit empty")
        .testEmpty("http://loinc.org|8867-4||'Heart rate' != {}",
            "coding literal not equals explicit empty")
        .group("Coding equality: singleton vs computed empty")
        .testEmpty(
            "http://loinc.org|8867-4||'Heart rate' = http://loinc.org|8867-4||'Heart rate'.where(false)",
            "coding literal equals computed empty")
        .testEmpty(
            "http://loinc.org|8867-4||'Heart rate' != http://loinc.org|8867-4||'Heart rate'.where(false)",
            "coding literal not equals computed empty")
        .group("Coding equality: array vs singleton (literal)")
        .testTrue("manyCoding != http://loinc.org|8867-4||'Heart rate'",
            "coding array differs singleton literal (one match)")
        .testFalse("manyCoding = http://loinc.org|9999-9||'Other'",
            "coding array not equals singleton literal (no match)")
        .group("Coding equality: array vs array (literals)")
        .testTrue("manyCoding = manyCoding", "coding array equals itself")
        .testTrue(
            "manyCoding != manyCoding1",
            "coding array not equals different array (different vaules)")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testMinimalUncomparableTypesEquality() {
    return builder()
        .withSubject(sb -> sb
            .string("str", "abc")
            .integer("intVal", 123)
            .bool("boolVal", true)
            .date("dateVal", "2020-01-01")
            .stringArray("strArray", "a", "b")
            .integerArray("intArray", 1, 2)
            .boolArray("boolArray", true, false)
            .dateArray("dateArray", "2020-01-01", "2020-01-02")
        )
        .group("Minimal uncomparable types: singleton vs singleton")
        .testFalse("str = intVal", "string = integer is false")
        .testTrue("str != boolVal", "string != boolean is true")
        .testEmpty("intVal = dateVal.where(false)", "integer = empty date is empty")
        .group("Minimal uncomparable types: array vs array")
        .testFalse("strArray = intArray", "string[] = integer[] is false")
        .testTrue("boolArray != dateArray", "boolean[] != date[] is true")
        .testEmpty("intArray.where(false) != dateArray", "empty integer[] = date[] is empty")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testQuantityEquality() {
    return builder()
        .withSubject(sb -> sb
            .stringEmpty("str")
        )
        .group("Quantity equality: UCUM quantities with same units")
        .testTrue("1 'mg' = 1 'mg'", "UCUM: 1 mg equals 1 mg")
        .testFalse("1 'mg' != 1 'mg'", "UCUM: 1 mg not unequal to 1 mg")
        .testFalse("1 'mg' = 2 'mg'", "UCUM: 1 mg not equals 2 mg")
        .testTrue("1 'mg' != 2 'mg'", "UCUM: 1 mg not equals 2 mg (non-equality)")
        .group("Quantity equality: UCUM quantities with compatible units")
        .testTrue("1000 'mg' = 1 'g'", "UCUM: 1000 mg equals 1 g (compatible)")
        .testFalse("1000 'mg' != 1 'g'", "UCUM: 1000 mg not unequal to 1 g (compatible)")
        .testFalse("500 'mg' = 1 'g'", "UCUM: 500 mg not equals 1 g (compatible)")
        .testTrue("500 'mg' != 1 'g'", "UCUM: 500 mg not equals 1 g (non-equality)")
        .group("Quantity equality: UCUM quantities with incompatible units")
        .testEmpty("1 'mg' = 1 'cm'", "UCUM: mg and cm are incompatible (empty)")
        .testEmpty("1 'mg' != 1 'cm'", "UCUM: mg and cm are incompatible (empty)")
        .group("Quantity equality: Calendar duration with same units")
        .testTrue("1 year = 1 year", "Calendar: 1 year equals 1 year")
        .testFalse("1 year != 1 year", "Calendar: 1 year not unequal to 1 year")
        .testFalse("1 year = 2 years", "Calendar: 1 year not equals 2 years")
        .testTrue("1 year != 2 years", "Calendar: 1 year not equals 2 years (non-equality)")
        .testTrue("2 months = 2 months", "Calendar: 2 months equals 2 months")
        .testFalse("2 months != 2 months", "Calendar: 2 months not unequal to 2 months")
        .testTrue("3 weeks = 3 weeks", "Calendar: 3 weeks equals 3 weeks")
        .testFalse("3 weeks != 3 weeks", "Calendar: 3 weeks not unequal to 3 weeks")
        .testTrue("4 days = 4 days", "Calendar: 4 days equals 4 days")
        .testFalse("4 days != 4 days", "Calendar: 4 days not unequal to 4 days")
        .testTrue("5 hours = 5 hours", "Calendar: 5 hours equals 5 hours")
        .testFalse("5 hours != 5 hours", "Calendar: 5 hours not unequal to 5 hours")
        .testTrue("6 minutes = 6 minutes", "Calendar: 6 minutes equals 6 minutes")
        .testFalse("6 minutes != 6 minutes", "Calendar: 6 minutes not unequal to 6 minutes")
        .testTrue("7 seconds = 7 seconds", "Calendar: 7 seconds equals 7 seconds")
        .testFalse("7 seconds != 7 seconds", "Calendar: 7 seconds not unequal to 7 seconds")
        .testTrue("8 milliseconds = 8 milliseconds",
            "Calendar: 8 milliseconds equals 8 milliseconds")
        .testFalse("8 milliseconds != 8 milliseconds",
            "Calendar: 8 milliseconds not unequal to 8 milliseconds")
        .group("Quantity equality: Calendar duration with compatible units")
        .testTrue("1 second = 1000 milliseconds", "Calendar: 1 second equals 1000 milliseconds")
        .testFalse("1 millisecond != 0.001 second", "Calendar: 1 millisecond not unequal to 0.001 second")
        .group("Quantity equality: Calendar duration with incompatible units")
        .testEmpty("1 year = 1 day", "Calendar: year and day are incompatible (empty)")
        .testEmpty("1 year != 1 day", "Calendar: year and day are incompatible (empty)")
        .group("Quantity equality: Calendar duration and UCUM cross-comparisons (comparable)")
        .testTrue("60 seconds = 1 'min'", "Calendar/UCUM: 60 seconds equals 1 min (comparable)")
        .testFalse("59 seconds = 1 'min'",
            "Calendar/UCUM: 59 seconds not equals 1 min (comparable)")
        .group("Quantity equality: Calendar duration and UCUM cross-comparisons (not comparable)")
        .testEmpty("1 year = 1 'a'", "Calendar/UCUM: year and UCUM 'a' are not comparable (empty)")
        .testEmpty("1 year != 1 'a'", "Calendar/UCUM: year and UCUM 'a' are not comparable (empty)")
        .group(
            "Quantity equality: Calendar duration and non-time UCUM cross-comparisons (not comparable)")
        .testEmpty("1 year = 1 'mg'", "Calendar/UCUM: year and UCUM mg are not comparable (empty)")
        .testEmpty("1 day != 1 'cm'", "Calendar/UCUM: day and UCUM cm are not comparable (empty)")
        .group("Quantity equality: quantity literal vs empty collection")
        .testEmpty("1 'mg' = {}", "UCUM: 1 mg equals empty collection (should be empty)")
        .testEmpty("{} = 1 'mg'", "UCUM: empty collection equals 1 mg (should be empty)")
        .testEmpty("1 year = {}", "Calendar: 1 year equals empty collection (should be empty)")
        .testEmpty("{} = 1 year", "Calendar: empty collection equals 1 year (should be empty)")
        .testEmpty("1 'mg' != {}", "UCUM: 1 mg not equals empty collection (should be empty)")
        .testEmpty("{} != 1 'mg'", "UCUM: empty collection not equals 1 mg (should be empty)")
        .testEmpty("1 year != {}", "Calendar: 1 year not equals empty collection (should be empty)")
        .testEmpty("{} != 1 year", "Calendar: empty collection not equals 1 year (should be empty)")
        .build();
  }
}
