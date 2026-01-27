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
 * Tests for FHIRPath boolean logic functions required by SQL on FHIR sharable view profile: - not()
 * function
 */
public class BooleanLogicFunctionsDslTest extends FhirPathDslTestBase {

  @FhirPathTest
  public Stream<DynamicTest> testNot() {
    return builder()
        .withSubject(
            sb ->
                sb
                    // Boolean values
                    .bool("trueValue", true)
                    .bool("falseValue", false)
                    .boolEmpty("emptyBoolean")
                    // String values
                    .string("stringValue", "value")
                    .stringEmpty("emptyString")
                    .stringArray("stringArray", "value1", "value2")
                    // Boolean arrays
                    .boolArray("boolArray", true, false, true)
                    // Complex types with boolean properties
                    .element("person", person -> person.string("name", "John").bool("active", true))
                    .elementArray(
                        "people",
                        person1 -> person1.string("name", "Alice").bool("active", true),
                        person2 -> person2.string("name", "Bob").bool("active", false))
                    .element(
                        "choiceField",
                        val1 ->
                            val1.choice("value")
                                .string("valueString", "123")
                                .integerEmpty("valueInteger")))
        .group("not() function")
        // Basic not() tests
        .testEquals(false, "trueValue.not()", "negates true to false")
        .testEquals(true, "falseValue.not()", "negates false to true")
        // empty collections
        .testEmpty("emptyBoolean.not()", "returns empty for empty boolean")
        .testEmpty("{}.not()", "returns empty for empty collection")
        .testEmpty("undefined.not()", "returns empty for empty collection")
        .testEmpty("emptyString.not()", "returns empty for empty String collection")
        .testEmpty(
            "stringValue.where($this.empty()).not()",
            "returns empty for calculated empty String collection")
        .testEmpty(
            "stringArray.where($this.empty()).not()",
            "returns empty for calculated empty String collection")
        .testEmpty(
            "%resource.ofType(Condition).not()", "returns empty for empty Resource collection")
        // boolean evaluation of collections
        .testFalse("%resource.not()", "false for root resource collection")
        .testFalse("stringValue.not()", "false for singular string element")
        .testFalse(
            "stringArray.where($this='value1').not()", "false for calculated singular  element")
        .testFalse("choiceField.value.not()", "false for singular choice element")
        .testFalse(
            "choiceField.value.ofType(string).not()", "false for singular resolved choice element")
        .testEmpty(
            "choiceField.value.ofType(integer).not()", "empty for empty resolved choice element")
        .testError("stringArray.not()", "fails on non-boolean non-singular collection")

        // not() with boolean arrays
        .testError("boolArray.not()", "fails on non-singular boolean collection")
        // not() with complex types
        .testEquals(false, "person.active.not()", "negates boolean property of complex type")
        .testError("people.active.not()", "fails on non-singular boolean computed collection")

        // Chained not() tests
        .testEquals(true, "falseValue.not().not().not()", "can be chained multiple times")
        .testTrue("trueValue.not() = false", "result can be compared with boolean literal")

        // not() with boolean expressions
        .testEquals(false, "(trueValue and trueValue).not()", "negates result of 'and' operation")
        .testEquals(false, "(falseValue or trueValue).not()", "negates result of 'or' operation")
        .testEquals(
            true,
            "(falseValue and trueValue).not()",
            "negates result of 'and' operation with false")

        // not() with boolean conditions
        .testTrue("people.where(active.not()).name = 'Bob'", "can be used in where() conditions")
        .build();
  }
}
