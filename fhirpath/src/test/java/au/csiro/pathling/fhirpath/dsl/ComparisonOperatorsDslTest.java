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

public class ComparisonOperatorsDslTest extends FhirPathDslTestBase {


  @FhirPathTest
  public Stream<DynamicTest> testUncomparableTypesComparison() {
    return builder()
        .withSubject(sb -> sb
            .stringEmpty("emptyString")
            .boolArray("allTrue", true, true)
            .boolArray("allFalse", false, false)
            .string("str", "abc")
            .decimalEmpty("emptyDecimal")
            .integer("intVal", 123)
            .date("dateVal", "2020-01-01")
            .bool("boolVal", true)
            .coding("codingVal", "http://loinc.org|8867-4||'Heart rate'")
            .stringArray("strArray", "a", "b")
            .integerArray("intArray", 1, 2)
            .dateArray("dateArray", "2020-01-01", "2020-01-02")
            .boolArray("boolArray", true, false)
            .codingArray("codingArray", "http://loinc.org|8867-4||'Heart rate'",
                "http://loinc.org|8480-6||'Systolic blood pressure'")
        )
        .group("Boolean comparison")
        // booleans are not comparable (orderable) despite having equality
        .testError("true > false", "Boolean comparison is not supported")
        .testEmpty("true > {}", "Boolean comparison with empty literal")
        .group("Coding comparison")
        // booleans are not comparable (orderable) despite having equality
        .testError("codingVal < codingVal", "Coding comparison is not supported")
        .testEmpty("{} >= codingVal", "Coding comparison with empty literal")
        .group("Other uncomparable types")
        .testError("str > intVal", "String > Integer is not supported")
        .testError("str < dateVal", "String > Date is not supported")
        .testError("str >= boolVal", "String > Boolean is not supported")
        .testError("str <= codingVal", "String > Coding is not supported")
        .testError("dateVal > boolVal", "Date > Boolean is not supported")
        .testError("dateVal < codingVal", "Date > Coding is not supported")
        .testError("codingVal >= intVal", "Coding > Integer is not supported")
        .testError("codingVal <= str", "Coding > String is not supported")
        .testError("codingVal > dateVal", "Coding > Date is not supported")
        .testError("codingVal < boolVal", "Coding > Boolean is not supported")
        .group("Uncomparable arrays")
        .testError("strArray > intArray", "String[] > Integer[] is not supported")
        .testError("dateArray < boolArray", "Date[] > Boolean[] is not supported")
        .testError("codingArray >= intArray", "Coding[] > Integer[] is not supported")
        .testError("codingArray <= strArray", "Coding[] > String[] is not supported")
        .testError("codingArray > dateArray", "Coding[] > Date[] is not supported")
        .testError("codingArray < boolArray", "Coding[] > Boolean[] is not supported")
        .group("Uncomparable implicit empty collections")
        .testError("emptyString > 10", "empty String > Integer is not supported")
        .testError("@2001-10-10 < emptyDecimal", "Date < empty Decimal is not supported")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testNumericComparison() {
    return builder()
        .withSubject(sb -> sb
            .integer("int1", 5)
            .integer("int2", 2)
            .decimal("dec1", 5.5)
            .decimal("dec2", 2.5)
            .integerArray("intArray", 1, 2, 3)
            .decimalArray("decArray", 1.1, 2.2, 3.3)
        )
        .group("Numeric comparisons operations")
        .testTrue("int1 > int2", "Integer greater than with variables")
        .testFalse("dec1 < dec2", "Decimal less than with variables")
        .testEmpty("{} > int1", "Empty greater than with variable")
        .testError("int1 <= intArray", "Integer less than or equal with array")
        .group("Cross numeric comparison operations")
        .testFalse("3.1 < 3 ", "Decimal less than Integer with literals")
        .testTrue("5 > 4.0", "Integer greater than Decimal with literals")
        // New tests for Quantity vs numeric literals
        .testTrue("5 '1' > 4", "Quantity with unit '1' greater than integer")
        .testTrue("5 '1' > 4.0", "Quantity with unit '1' greater than decimal")
        .testFalse("2 '1' < 1.5", "Quantity with unit '1' less than decimal")
        .testTrue("3.5 > 2 '1'", "Decimal greater than Quantity with unit '1'")
        .testFalse("2 < 2 '1'", "Integer less than or equal to Quantity with unit '1'")
        .testEmpty("5 'mg' > 4", "Quantity with non-'1' unit compared to integer yields empty")
        .testEmpty("5 'mg' > 4.0", "Quantity with non-'1' unit compared to decimal yields empty")
        .testEmpty("3.5 < 2 'mg'", "Decimal compared to Quantity with non-'1' unit yields empty")
        .testEmpty("2 < 2 'mg'", "Integer compared to Quantity with non-'1' unit yields empty")
        .testEmpty("2 years > 1", "Calendar duration compared to integer yields empty")
        .testEmpty("2 years > 1.0", "Calendar duration compared to decimal yields empty")
        .testEmpty("3.5 < 2 years", "Decimal compared to calendar duration yields empty")
        .testEmpty("2 < 2 years", "Integer compared to calendar duration yields empty")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testStringComparison() {
    return builder()
        .withSubject(sb -> sb
            .string("str1", "abc")
            .string("str2", "efg")
            .stringEmpty("strEmpty")
            .decimal("dec", 5.5)
            .boolEmpty("boolEmpty")
            .stringArray("strArray", "a", "b", "c")
        )
        .group("Strign comparison")
        .testTrue("str1 < str2", "String less than with variables")
        .testEmpty("str1 >= {}", "Empty greater than or equal with variable")
        .testEmpty("strEmpty <= str2", "Empty less than or equal with variable")
        .testEmpty("str1s < boolEmpty", "String less than with empty boolean")
        .testError("strArray <= str2", "String less than or equal with array")
        .testError("str1 >= dec", "String greater than or equal with decimal")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testCalendarComparison() {
    return builder()
        .withSubject(sb -> sb
            .stringEmpty("strEmpty")
        )
        .group("Date comparison")
        .testTrue("@2020-01-01 < @2020-01-02", "Full dates less than")
        .testFalse("@2008 <  @2008", "Equal partial dates less than false case")
        .testFalse("@2020-01 > @2020-02", "Partial date same precision greater than")
        .testTrue("@2020-03-01 >= @2020-02",
            "Comparable partial date different precision greater than")
        .testEmpty("@2020 <= @2020-01",
            "Incomparable partials dates less than with different precisions")
        .group("DateTime comparison")
        .testTrue("@2020-01-01T10:00:00+00:00 < @2020-01-01T11:00:00+00:00",
            "Full DateTimes less than")
        .testFalse("@2020-01-01T12:00 < @2020-01-01T11:00",
            "Partial same precision DateTimes less than false case")
        .testTrue("@2020-01-01T12:00 >= @2020-01-01T11",
            "Comparable partial date different precision >=")
        .testFalse("@2018-03-01T10:30:00 > @2018-03-01T10:30:00.0",
            "Seconds and miliseconds precision can be compared")
        .testEmpty("@2020-01-01T12:00 <= @2020-01-01T12",
            "Incomparable partials dates less than with different precisions")
        .group("Date vs DateTime comparison")
        .testTrue("@2020-01-02 > @2020-01-01T10:00:00Z", "Same full comparable Date and DateTime")
        .testEmpty("@2020-01-01 < @2020-01-01T10:00:00Z",
            "Same full uncomparable Date and DateTime")
        .testFalse("@2020-02-01T10 <= @2020-01", "Partial comparable DateTime and Date")
        .testEmpty("@2020-01 <= @2020-01-01T10", "Partial uncomparable Date and DateTime")
        .group("Timezone tests")
        .testTrue("@2018-03-01 < @2018-03-02T00:00:00", "Comparable no timezone")
        .testTrue("@2018-03-01 < @2018-03-02T00:00:00Z", "Comparable UTC timezone")
        .testTrue("@2018-03-01 < @2018-03-02T00:00:00-01:00", "Comparable before UTC timezone")
        .testEmpty("@2018-03-01 < @2018-03-02T00:00:00+01:00",
            "Uncomparable due to timezone after UTC")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testTimeComparison() {
    return builder()
        .withSubject(sb -> sb
            .stringEmpty("strEmpty")
        )
        .group("Time comparison - same precision")
        .testTrue("@T12:00 < @T13:00", "Full time less than")
        .testFalse("@T14:00 < @T14:00", "Equal times less than false case")
        .testTrue("@T15:30 > @T15:00", "Times greater than")
        .testTrue("@T16:45 >= @T16:45", "Times greater than or equal true case")
        .testFalse("@T17:15 <= @T17:00", "Times less than or equal false case")

        .group("Time comparison - different precision")
        .testTrue("@T10 < @T11:30", "Hour vs minute precision, comparable")
        .testFalse("@T11:45 < @T10", "Minute vs hour precision, comparable")
        .testTrue("@T12:31:45 >= @T12:30", "Second vs minute precision, comparable")
        .testFalse("@T13:15 > @T14:15:30", "Minute vs second precision, comparable")
        .testTrue("@T14:20:15.500 > @T14:20:15", "Millisecond precision comparison")

        .group("Time comparison - incomparable times")
        .testEmpty("@T10 <= @T10:00:00", "Hour vs second precision, incomparable")
        .testEmpty("@T12 > @T12:00:00.000", "Hour vs millisecond precision, incomparable")

        .group("Edge cases")
        .testTrue("@T23:59:59.999999999 > @T00:00", "End of day vs beginning comparison")
        .testTrue("@T00:00:00 < @T23:59:59.999999999", "Beginning vs end of day comparison")
        .testEmpty("{} < @T12:00", "Empty less than time")
        .testEmpty("@T12:30 > {}", "Time greater than empty")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testQuantityComparison() {
    return builder()
        .withSubject(sb -> sb
            .stringEmpty("strEmpty")
        )
        .group("UCUM quantities with same units")
        .testTrue("10 'mg' > 5 'mg'", "10 mg greater than 5 mg")
        .testFalse("3 'mg' > 5 'mg'", "3 mg not greater than 5 mg")
        .testTrue("5 'mg' <= 5 'mg'", "5 mg less than or equal to 5 mg")
        .testFalse("2 'mg' >= 5 'mg'", "2 mg not greater than or equal to 5 mg")
        .group("UCUM quantities with compatible units")
        .testTrue("1000 'mg' = 1 'g'", "1000 mg equals 1 g (compatible units)")
        .testTrue("2 'g' > 1000 'mg'", "2 g greater than 1000 mg (compatible units)")
        .testFalse("500 'mg' >= 1 'g'",
            "500 mg not greater than or equal to 1 g (compatible units)")
        .testTrue("1 'g' <= 1000 'mg'", "1 g less than or equal to 1000 mg (compatible units)")
        .group("UCUM quantities with incompatible units")
        .testEmpty("1 'mg' > 1 'cm'", "mg and cm are incompatible units")
        .testEmpty("5 'L' < 5 'kg'", "L and kg are incompatible units")
        .testEmpty("1 'mg' >= 1 'cm'", "mg and cm are incompatible units for >=")
        .testEmpty("5 'L' <= 5 'kg'", "L and kg are incompatible units for <=")
        .group("Calendar duration quantities with same units")
        .testTrue("2 years > 1 year", "2 years greater than 1 year")
        .testTrue("12 months >= 6 months", "12 months greater than or equal to 6 months")
        .testTrue("3 weeks < 4 weeks", "3 weeks less than 4 weeks")
        .testTrue("10 days <= 10 days", "10 days less than or equal to 10 days")
        .testTrue("5 hours > 2 hours", "5 hours greater than to 2 hours")
        .testTrue("30 minutes <= 30 minutes", "30 minutes equal to 30 minutes")
        .testTrue("45 seconds >= 20 seconds", "45 seconds greater than or equal to 20 seconds")
        .testTrue("1000 milliseconds < 2000 milliseconds",
            "1000 milliseconds less than 2000 milliseconds")
        .group("UCUM quantities with incompatible units (> seconds)")
        .testEmpty("1 year > 1 month", "year and month are not comparable")
        .testEmpty("1 year < 1 week", "year and week are not comparable")
        .testEmpty("1 month < 1 week", "month and week are not comparable")
        .testEmpty("1 year >= 1 day", "year and day are not comparable")
        .testEmpty("1 month <= 1 hour", "month and hour are not comparable")
        .testEmpty("1 day < 25 hours", "1 day less than 25 hours")
        .testEmpty("60 minutes <= 1 hour", "60 minutes less than or equal to 1 hour")
        .testEmpty("1 week > 6 days", "1 week greater than 6 days")
        .testEmpty("120 seconds > 3 minute", "120 seconds not greater than 3 minutes")
        .group("Calendar duration and UCUM cross-comparisons (calendar ≤ seconds)")
        .testTrue("31557600 's' >= 1 'a'",
            "31557600 seconds greater or equal to 1 UCUM year (comparable because calendar unit is ≤ seconds)")
        .testFalse("1000 milliseconds > 1 'wk'",
            "1000 milliseconds not greater than 1 UCUM week (comparable because calendar unit is ≤ seconds)")
        .testTrue("1 second <= 1 'mo'",
            "1 second less than or equal to 1 UCUM month (comparable because calendar unit is ≤ seconds)")
        .testTrue("500 milliseconds < 1 'min'",
            "500 milliseconds less than 1 UCUM minute (comparable because calendar unit is ≤ seconds)")
        .group("Calendar duration and UCUM cross-comparisons (calendar > seconds, not comparable)")
        .testEmpty("1 year > 1 'a'",
            "year and UCUM 'a' are not comparable (calendar unit > seconds)")
        .testEmpty("1 month < 1 'mo'",
            "month and UCUM 'mo' are not comparable (calendar unit > seconds)")
        .testEmpty("1 week <= 1 'wk'",
            "week and UCUM 'wk' are not comparable (calendar unit > seconds)")
        .testEmpty("1 day >= 1 'd'",
            "day and UCUM 'd' are not comparable (calendar unit > seconds)")
        .testEmpty("1 hour < 1 'h'",
            "hour and UCUM 'h' are not comparable (calendar unit > seconds)")
        .testEmpty("1 minute > 1 'min'",
            "minute and UCUM 'min' are not comparable (calendar unit > seconds)")
        .group("Calendar duration and non-time UCUM cross-comparisons (not comparable)")
        .testEmpty("1 year > 1 'mg'",
            "calendar year and UCUM mg are not comparable (non-time UCUM)")
        .testEmpty("10 hours < 1 'cm'",
            "calendar hour and UCUM cm are not comparable (non-time UCUM)")
        .testEmpty("5 seconds >= 1 'kg'",
            "calendar second and UCUM kg are not comparable (non-time UCUM)")
        .testEmpty("100 milliseconds <= 1 'L'",
            "calendar millisecond and UCUM L are not comparable (non-time UCUM)")
        .build();
  }
}
