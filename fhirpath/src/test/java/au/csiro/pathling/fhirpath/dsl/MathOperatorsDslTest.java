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

public class MathOperatorsDslTest extends FhirPathDslTestBase {

  @FhirPathTest
  public Stream<DynamicTest> testNumberMathOperations() {
    return builder()
        .withSubject(
            sb ->
                sb.integer("int1", 5)
                    .integer("int2", 2)
                    .decimal("dec1", 5.5)
                    .decimal("dec2", 2.5)
                    .integerArray("intArray", 1, 2, 3)
                    .decimalArray("decArray", 1.1, 2.2, 3.3))
        .group("Integer math operations")

        // Integer negation
        .testEquals(-5, "-int1", "Integer negation with variables")
        .testEquals(-2, "-2", "Integer negation with literals")
        .testEquals(7, "--7", "Double integer negation")

        // Integer addition
        .testEquals(7, "int1 + int2", "Integer addition with variables")
        .testEquals(7, "5 + 2", "Integer addition with literals")
        .testEquals(7, "int1 + 2", "Integer addition with variable and literal")

        // Integer subtraction
        .testEquals(3, "int1 - int2", "Integer subtraction with variables")
        .testEquals(3, "5 - 2", "Integer subtraction with literals")
        .testEquals(3, "int1 - 2", "Integer subtraction with variable and literal")

        // Integer multiplication
        .testEquals(10, "int1 * int2", "Integer multiplication with variables")
        .testEquals(10, "5 * 2", "Integer multiplication with literals")
        .testEquals(10, "int1 * 2", "Integer multiplication with variable and literal")

        // Integer division
        .testEquals(2.5, "int1 / int2", "Integer division with variables")
        .testEquals(2.5, "5 / 2", "Integer division with literals")
        .testEquals(2.5, "int1 / 2", "Integer division with variable and literal")

        // Integer mod
        .testEquals(1, "int1 mod int2", "Integer mod with variables")
        .testEquals(1, "5 mod 2", "Integer mod with literals")
        .testEquals(1, "int1 mod 2", "Integer mod with variable and literal")
        .group("Decimal math operations")

        // Decimal negation
        .testEquals(-5.5, "-int1", "Decimal negation with variables")
        .testEquals(-2.5, "-2.5", "Decimal negation with literals")
        .testEquals(7.3, "--7.3", "Double decimal negation")

        // Decimal addition
        .testEquals(8.0, "dec1 + dec2", "Decimal addition with variables")
        .testEquals(8.0, "5.5 + 2.5", "Decimal addition with literals")
        .testEquals(8.0, "dec1 + 2.5", "Decimal addition with variable and literal")

        // Decimal subtraction
        .testEquals(3.0, "dec1 - dec2", "Decimal subtraction with variables")
        .testEquals(3.0, "5.5 - 2.5", "Decimal subtraction with literals")
        .testEquals(3.0, "dec1 - 2.5", "Decimal subtraction with variable and literal")

        // Decimal multiplication
        .testEquals(13.75, "dec1 * dec2", "Decimal multiplication with variables")
        .testEquals(13.75, "5.5 * 2.5", "Decimal multiplication with literals")
        .testEquals(13.75, "dec1 * 2.5", "Decimal multiplication with variable and literal")

        // Decimal division
        .testEquals(2.2, "dec1 / dec2", "Decimal division with variables")
        .testEquals(2.2, "5.5 / 2.5", "Decimal division with literals")
        .testEquals(2.2, "dec1 / 2.5", "Decimal division with variable and literal")
        .group("Mixed type math operations")
        // Integer and decimal operations
        .testEquals(7.5, "int1 + dec2", "Integer + Decimal")
        .testEquals(10.5, "5 + 5.5", "Integer literal + Decimal literal")
        .testEquals(2.5, "int1 - dec2", "Integer - Decimal")
        .testEquals(12.5, "int1 * dec2", "Integer * Decimal")
        .testEquals(2.0, "int1 / dec2", "Integer / Decimal")
        .group("Error cases with collections")
        .testError(
            "Polarity operator (-) requires a singular operand.",
            "-intArray",
            "Polarity with array")
        .testError(
            "Math operator (+) requires the left operand to be singular.",
            "intArray + 1",
            "Addition with array")
        .testError(
            "Math operator (+) requires the right operand to be singular.",
            "1 + intArray",
            "Addition with array")
        .testError(
            "Math operator (*) requires the left operand to be singular.",
            "intArray * 2",
            "Multiplication with array")
        .testError(
            "Math operator (/) requires the left operand to be singular.",
            "decArray / 2",
            "Division with array")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testMathWithEmptyArguments() {
    return builder()
        .withSubject(
            sb ->
                sb.integer("integerValue", 10)
                    .integerEmpty("emptyInteger")
                    .decimal("decimalValue", 11.5)
                    .bool("active", true)
                    .string("stringValue", "hello")
                    .stringEmpty("emptyStr"))
        .group("Math with empty arguments")
        // unary operations
        .testEmpty("-{}", "Polarity with empty literal")
        .testEmpty("-emptyInteger", "Polarity with empty integer collection")

        // binary operations
        .testEmpty("integerValue + {}", "Addition with empty literal")
        .testEmpty("{} - decimalValue", "Subtraction with empty literal")
        .testEmpty("1.5 * {}", "Multiplication with empty literal")
        .testEmpty("{}.where(true) / 2", "Division with empty literal where true")
        .testEmpty("2 + foo", "Addition with empty literal where foo is not defined")
        .testEmpty("foo - {}", "Subtraction with of two empty collections")

        // Empty collections in string concatenation
        .testEmpty("stringValue + {}", "String concatenation with empty literal using +")
        .testEmpty("{} + stringValue", "String concatenation with empty literal using +")
        .testEmpty("emptyStr + stringValue", "String concatenation with empty string using +")
        .testEmpty("stringValue + emptyStr", "String concatenation with empty string using +")
        // error cases
        .testError("2 * true", "Multiplication with boolean literal")
        .testError("2 / active", "Division with boolean literal where active is boolean")
        .testError(
            "2 + true.where(%resource.active)",
            "Addition with boolean literal where active is boolean and true")
        .testError("2 - true.where(false)", "Subtraction with boolean literal where false")
        .testError(
            "2 * true.where({})", "Multiplication with boolean literal where empty collection")
        .testError(
            "2 / true.where(1 = 2)", "Division with boolean literal where condition is false")
        .testError(
            "Negation is not supported on Strings",
            "-emptyStr",
            "Polarity with empty string collection")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testStringMathOperations() {
    return builder()
        .withSubject(
            sb ->
                sb.string("str1", "hello")
                    .string("str2", "world")
                    .stringArray("strArray", "one", "two", "three")
                    .stringEmpty("emptyStr")
                    .integer("intValue", 42))
        .group("String addition (concatenation)")
        // String concatenation with + operator
        .testEquals("helloworld", "str1 + str2", "String concatenation with variables")
        .testEquals("hello world", "'hello' + ' world'", "String concatenation with literals")
        .testEquals(
            "hello world", "str1 + ' world'", "String concatenation with variable and literal")
        .group("Unsupported string math operations")
        // Other math operations not supported for strings
        .testError("str1 - str2", "String subtraction not supported")
        .testError("str1 * 2", "String multiplication not supported")
        .testError("2 * str1", "String multiplication not supported")
        .testError("str1 / str2", "String division not supported")
        .testError("str1 mod str2", "String modulo not supported")
        .testError("str1 div str2", "String integer division not supported")
        .testError("-str1", "Unary minus not supported for strings")
        .group("String array math operations")
        // Operations with string arrays
        .testError("strArray + str1", "String array addition not supported")
        .testError("str1 + strArray", "String array addition not supported")
        .testError("strArray - str1", "String array subtraction not supported")
        .build();
  }
}
