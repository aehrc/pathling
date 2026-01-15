/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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
 * Tests for FHIRPath existence functions as defined in supported.md: - exists() - empty() - count()
 * - allTrue() - allFalse() - anyTrue() - anyFalse()
 */
public class ExistenceFunctionsDslTest extends FhirPathDslTestBase {

  @FhirPathTest
  public Stream<DynamicTest> testExists() {
    return builder()
        .withSubject(
            sb ->
                sb
                    // Empty values
                    .stringEmpty("emptyString")
                    .elementEmpty("emptyComplex")
                    // Single values
                    .string("singleString", "test")
                    .integer("singleInteger", 42)
                    .bool("singleBoolean", true)
                    // Arrays
                    .stringArray("stringArray", "one", "two", "three")
                    // Complex types
                    .element(
                        "person",
                        person ->
                            person.string("name", "John").integer("age", 30).bool("active", true))
                    .elementArray(
                        "people",
                        person1 ->
                            person1
                                .string("name", "Alice")
                                .integer("age", 25)
                                .bool("active", true)
                                .stringArray("alias", "Alias2", "Alias1"),
                        person2 ->
                            person2
                                .string("name", "Bob")
                                .integer("age", 40)
                                .bool("active", false)
                                .stringArray("alias", "Alias4", "Alias5")))
        .group("exists() function")
        // Basic exists() tests
        .testTrue("singleString.exists()", "returns true for a single string")
        .testTrue("stringArray.exists()", "returns true for a non-empty array")
        .testFalse("emptyString.exists()", "returns false for an empty value")
        .testTrue("singleInteger.exists()", "returns true for a single integer")
        .testTrue("singleBoolean.exists()", "returns true for a single boolean")
        .testTrue("person.exists()", "returns true for a complex type")
        .testTrue("people.exists()", "returns true for an array of complex types")
        .testFalse("emptyComplex.exists()", "returns false for an empty complex type")
        .testFalse("{}.exists()", "returns false for an empty literal")

        // exists() with criteria
        .testTrue(
            "stringArray.exists($this = 'one')", "with criteria returns true when criteria matches")
        .testFalse(
            "stringArray.exists($this = 'four')",
            "with criteria returns false when criteria doesn't match")
        .testTrue(
            "people.exists(name = 'Alice')",
            "with criteria on complex type returns true when criteria matches")
        .testFalse(
            "people.exists(name = 'David')",
            "with criteria on complex type returns false when criteria doesn't match")
        .testTrue(
            "people.exists(active = true)",
            "with criteria on complex type returns true when criteria matches multiple items")
        .testTrue(
            "people.exists(name)",
            "with criteria on complex type returns true with boolean eval of singletons for"
                + " singular element")
        .testError(
            "people.exists(alias)",
            "with criteria on complex type fails with boolean eval of non-singleton")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testEmpty() {
    return builder()
        .withSubject(
            sb ->
                sb
                    // Empty values of different types
                    .stringEmpty("emptyString")
                    .integerEmpty("emptyInteger")
                    .decimalEmpty("emptyDecimal")
                    .boolEmpty("emptyBoolean")
                    .elementEmpty("emptyComplex")
                    // Single values
                    .string("singleString", "test")
                    .integer("singleInteger", 42)
                    .bool("singleBoolean", true)
                    // Arrays
                    .stringArray("stringArray", "one", "two", "three")
                    // Complex types
                    .element(
                        "person",
                        person ->
                            person.string("name", "John").integer("age", 30).bool("active", true)))
        .group("empty() function")
        // empty() tests
        .testTrue("emptyString.empty()", "returns true for an empty string")
        .testTrue("emptyInteger.empty()", "returns true for an empty integer")
        .testTrue("emptyDecimal.empty()", "returns true for an empty decimal")
        .testTrue("emptyBoolean.empty()", "returns true for an empty boolean")
        .testTrue("emptyComplex.empty()", "returns true for an empty complex type")
        .testTrue("{}.empty()", "returns true for an empty literal")
        .testFalse("singleString.empty()", "returns false for a single string")
        .testFalse("stringArray.empty()", "returns false for a non-empty array")
        .testFalse("singleInteger.empty()", "returns false for a single integer")
        .testFalse("singleBoolean.empty()", "returns false for a single boolean")
        .testFalse("person.empty()", "returns false for a complex type")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testChainedFunctions() {
    return builder()
        .withSubject(
            sb ->
                sb
                    // Arrays
                    .stringArray("stringArray", "one", "two", "three")
                    .integerArray("integerArray", 1, 2, 3, 4, 5)
                    // Complex types
                    .elementArray(
                        "people",
                        person1 ->
                            person1.string("name", "Alice").integer("age", 25).bool("active", true),
                        person2 ->
                            person2.string("name", "Bob").integer("age", 40).bool("active", false),
                        person3 ->
                            person3
                                .string("name", "Charlie")
                                .integer("age", 35)
                                .bool("active", true)))
        .group("Chained function tests")
        // Chained function tests
        .testFalse(
            "stringArray.exists().not().empty()",
            "Chained exists() and empty() functions work correctly")
        .build();
  }
}
