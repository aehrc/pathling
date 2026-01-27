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
import org.hl7.fhir.r4.model.ContactPoint;
import org.hl7.fhir.r4.model.HumanName;
import org.hl7.fhir.r4.model.Patient;
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

  @FhirPathTest
  public Stream<DynamicTest> testCount() {
    return builder()
        .withSubject(
            sb ->
                sb
                    // Empty values of different types
                    .stringEmpty("emptyString")
                    .integerEmpty("emptyInteger")
                    .elementEmpty("emptyComplex")
                    // Single values
                    .string("singleString", "test")
                    .integer("singleInteger", 42)
                    // Arrays
                    .stringArray("stringArray", "one", "two", "three")
                    .integerArray("integerArray", 1, 2, 3, 4, 5)
                    // Complex types
                    .element(
                        "person",
                        person ->
                            person.string("name", "John").integer("age", 30).bool("active", true))
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
        .group("count() function - Core tests for basic types")
        // Empty collections - should return 0
        .testEquals(0, "emptyString.count()", "returns 0 for an empty string")
        .testEquals(0, "emptyInteger.count()", "returns 0 for an empty integer")
        .testEquals(0, "emptyComplex.count()", "returns 0 for an empty complex type")
        .testEquals(0, "{}.count()", "returns 0 for an empty literal")
        // Single values - should return 1
        .testEquals(1, "singleString.count()", "returns 1 for a single string")
        .testEquals(1, "singleInteger.count()", "returns 1 for a single integer")
        // Arrays - should return actual count
        .testEquals(3, "stringArray.count()", "returns 3 for a string array with 3 elements")
        .testEquals(5, "integerArray.count()", "returns 5 for an integer array with 5 elements")
        .group("count() function - Composite types")
        // Single complex element
        .testEquals(1, "person.count()", "returns 1 for a single complex element")
        // Complex element array
        .testEquals(3, "people.count()", "returns 3 for an array of 3 complex types")
        // Nested property access
        .testEquals(3, "people.name.count()", "returns 3 for nested property on array")
        .group("count() function - Function composition with where()")
        // Filtered string array
        .testEquals(
            1,
            "stringArray.where($this = 'one').count()",
            "returns 1 after filtering to single match")
        .testEquals(
            0,
            "stringArray.where($this = 'four').count()",
            "returns 0 after filtering to no matches")
        // Filtered complex array
        .testEquals(
            2, "people.where(active = true).count()", "returns 2 for filtered complex type array")
        .group("count() function - Comparisons")
        // Equality and comparison tests
        .testTrue("stringArray.count() = 3", "count result can be compared for equality")
        .testTrue("people.count() > 0", "count result can be used in greater than comparison")
        .testFalse("emptyString.count() > 0", "count of empty collection is not greater than 0")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testCountOnFhirResource() {
    final Patient patient = new Patient();

    // Add names
    patient.addName(new HumanName().setFamily("Smith").addGiven("John").addGiven("David"));

    // Add telecoms
    patient.addTelecom(
        new ContactPoint().setSystem(ContactPoint.ContactPointSystem.PHONE).setValue("555-1234"));
    patient.addTelecom(
        new ContactPoint()
            .setSystem(ContactPoint.ContactPointSystem.EMAIL)
            .setValue("john@example.com"));

    return builder()
        .withResource(patient)
        .group("count() function - HAPI FHIR resource collections")
        .testEquals(1, "Patient.name.count()", "returns 1 for Patient.name")
        .testEquals(2, "Patient.telecom.count()", "returns 2 for Patient.telecom")
        .testEquals(
            2, "Patient.name.given.count()", "returns 2 for Patient.name.given (nested array)")
        .testEquals(0, "Patient.address.count()", "returns 0 for empty Patient.address")
        .group("count() function - on the resource itself")
        .testEquals(1, "Patient.count()", "returns 1 for Patient resource")
        .testEquals(1, "%resource.count()", "returns 1 for %resource variable")
        .group("count() function - FHIR resource with where()")
        .testEquals(
            1,
            "Patient.telecom.where(system = 'phone').count()",
            "returns 1 for filtered Patient.telecom by phone")
        .testEquals(
            1,
            "Patient.telecom.where(system = 'email').count()",
            "returns 1 for filtered Patient.telecom by email")
        .testEquals(
            0,
            "Patient.telecom.where(system = 'fax').count()",
            "returns 0 for filtered Patient.telecom with no matches")
        .group("count() function - FHIR resource comparisons")
        .testTrue("Patient.telecom.count() = 2", "count of Patient.telecom can be compared")
        .testTrue(
            "Patient.telecom.count() > 1", "count of Patient.telecom in greater than comparison")
        .testFalse(
            "Patient.address.count() > 0", "count of empty Patient.address is not greater than 0")
        .build();
  }
}
