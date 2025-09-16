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
}
