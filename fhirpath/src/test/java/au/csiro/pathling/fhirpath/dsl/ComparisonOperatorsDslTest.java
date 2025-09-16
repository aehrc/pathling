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
  public Stream<DynamicTest> testBooleanComparison() {
    return builder()
        .withSubject(sb -> sb
            .stringEmpty("emptyString")
            .boolArray("allTrue", true, true)
            .boolArray("allFalse", false, false)
        )
        .group("Boolean comparison")
        // booleans are not comparable (orderable) despite having equality
        .testError("true > false", "Boolean comparison is not supported")
        .testEmpty("true > {}", "Boolean comparison with empty literal")
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
        .testFalse("int2 >= dec1", "Integer greater that or equal with variables false case")
        .testEmpty("{} > int1", "Empty greater than with variable")
        .testError("int1 <= intArray", "Integer less than or equal with array")
        .group("Numeric equality operations")
        .testTrue("int1 = 5.0", "Integer equals decimal with variables")
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
}
