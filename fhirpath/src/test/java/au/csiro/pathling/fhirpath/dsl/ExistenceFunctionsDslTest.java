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

import static au.csiro.pathling.test.yaml.FhirTypedLiteral.toCoding;
import static au.csiro.pathling.test.yaml.FhirTypedLiteral.toDate;
import static au.csiro.pathling.test.yaml.FhirTypedLiteral.toDateTime;
import static au.csiro.pathling.test.yaml.FhirTypedLiteral.toQuantity;
import static au.csiro.pathling.test.yaml.FhirTypedLiteral.toTime;

import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import java.util.List;
import java.util.stream.Stream;
import org.hl7.fhir.r4.model.ContactPoint;
import org.hl7.fhir.r4.model.HumanName;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.DynamicTest;

/**
 * Tests for FHIRPath existence functions as defined in supported.md: - exists() - empty() - count()
 * - all() - allTrue() - allFalse() - anyTrue() - anyFalse() - isDistinct() - distinct()
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

  @FhirPathTest
  public Stream<DynamicTest> testAll() {
    return builder()
        .withSubject(
            sb ->
                sb.integerEmpty("emptyInteger")
                    .integer("singleInteger", 42)
                    .integerArray("integerArray", 1, 2, 3)
                    .integerArray("indexMatch", 0, 1, 2, 3)
                    .elementArray(
                        "people",
                        person1 ->
                            person1
                                .integer("age", 25)
                                .bool("active", true)
                                .stringArray("alias", "Alice", "Al"),
                        person2 ->
                            person2
                                .integer("age", 40)
                                .bool("active", false)
                                .stringArray("alias", "Bob", "Bobby")))
        .group("all() empty propagation")
        .testTrue("{}.all($this > 0)", "Empty literal returns true (vacuous truth)")
        .testTrue("emptyInteger.all($this > 0)", "Typed-empty field returns true")
        .testTrue(
            "integerArray.where($this > 100).all($this > 0)",
            "Computed-empty array (filtered to nothing) returns true")
        .group("all() core semantics")
        .testTrue("singleInteger.all($this > 0)", "Singleton matching criteria returns true")
        .testFalse("singleInteger.all($this < 0)", "Singleton failing criteria returns false")
        .testTrue("integerArray.all($this > 0)", "Array where every element matches returns true")
        .testFalse(
            "integerArray.all($this = 1)", "Array where only some elements match returns false")
        .testFalse("integerArray.all($this < 0)", "Array where no elements match returns false")
        .testTrue(
            "indexMatch.all($this = $index)", "$index is available within the criteria expression")
        .group("all() on complex elements")
        .testTrue("people.all(age > 0)", "Criteria on complex type matching every element")
        .testFalse(
            "people.all(active = true)",
            "Criteria on complex type failing for one element returns false")
        .testError(
            "people.all(alias)",
            "Criteria evaluating to a non-singleton boolean (inherited from where()) raises an"
                + " error")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testAllTrueAnyTrueAllFalseAnyFalse() {
    return builder()
        .withSubject(
            sb ->
                sb.boolEmpty("emptyBoolean")
                    .bool("singleTrue", true)
                    .bool("singleFalse", false)
                    .boolArray("allTrueArray", true, true)
                    .boolArray("allFalseArray", false, false)
                    .boolArray("mixedArray", true, false)
                    .stringArray("stringArray", "one", "two"))
        .group("allTrue()/anyTrue()/allFalse()/anyFalse() empty propagation")
        .testTrue("{}.allTrue()", "allTrue() of empty literal is true")
        .testFalse("{}.anyTrue()", "anyTrue() of empty literal is false")
        .testTrue("{}.allFalse()", "allFalse() of empty literal is true")
        .testFalse("{}.anyFalse()", "anyFalse() of empty literal is false")
        .testTrue("emptyBoolean.allTrue()", "allTrue() of typed-empty field is true")
        .testFalse("emptyBoolean.anyTrue()", "anyTrue() of typed-empty field is false")
        .testTrue("emptyBoolean.allFalse()", "allFalse() of typed-empty field is true")
        .testFalse("emptyBoolean.anyFalse()", "anyFalse() of typed-empty field is false")
        .testTrue(
            "allTrueArray.where($this = false).allTrue()",
            "allTrue() of a computed-empty array (filtered to nothing) is true")
        .testFalse(
            "allTrueArray.where($this = false).anyTrue()",
            "anyTrue() of a computed-empty array (filtered to nothing) is false")
        .group("allTrue()/anyTrue()/allFalse()/anyFalse() singleton")
        .testTrue("singleTrue.allTrue()", "allTrue() of a single true value is true")
        .testTrue("singleTrue.anyTrue()", "anyTrue() of a single true value is true")
        .testFalse("singleTrue.allFalse()", "allFalse() of a single true value is false")
        .testFalse("singleTrue.anyFalse()", "anyFalse() of a single true value is false")
        .testFalse("singleFalse.allTrue()", "allTrue() of a single false value is false")
        .testFalse("singleFalse.anyTrue()", "anyTrue() of a single false value is false")
        .testTrue("singleFalse.allFalse()", "allFalse() of a single false value is true")
        .testTrue("singleFalse.anyFalse()", "anyFalse() of a single false value is true")
        .group("allTrue()/anyTrue()/allFalse()/anyFalse() over arrays")
        .testTrue("allTrueArray.allTrue()", "allTrue() of an all-true array is true")
        .testTrue("allTrueArray.anyTrue()", "anyTrue() of an all-true array is true")
        .testFalse("allTrueArray.allFalse()", "allFalse() of an all-true array is false")
        .testFalse("allTrueArray.anyFalse()", "anyFalse() of an all-true array is false")
        .testFalse("allFalseArray.allTrue()", "allTrue() of an all-false array is false")
        .testFalse("allFalseArray.anyTrue()", "anyTrue() of an all-false array is false")
        .testTrue("allFalseArray.allFalse()", "allFalse() of an all-false array is true")
        .testTrue("allFalseArray.anyFalse()", "anyFalse() of an all-false array is true")
        .testFalse("mixedArray.allTrue()", "allTrue() of a mixed array is false")
        .testTrue("mixedArray.anyTrue()", "anyTrue() of a mixed array is true")
        .testFalse("mixedArray.allFalse()", "allFalse() of a mixed array is false")
        .testTrue("mixedArray.anyFalse()", "anyFalse() of a mixed array is true")
        .group("allTrue()/anyTrue()/allFalse()/anyFalse() reject non-Boolean input")
        .testError("stringArray.allTrue()", "allTrue() on a non-Boolean collection raises an error")
        .testError("stringArray.anyTrue()", "anyTrue() on a non-Boolean collection raises an error")
        .testError(
            "stringArray.allFalse()", "allFalse() on a non-Boolean collection raises an error")
        .testError(
            "stringArray.anyFalse()", "anyFalse() on a non-Boolean collection raises an error")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testIsDistinctAndDistinctCardinality() {
    return builder()
        .withSubject(
            sb ->
                sb.stringEmpty("emptyString")
                    .string("singleString", "test")
                    .integerArray("distinctArray", 1, 2, 3)
                    .integerArray("duplicateArray", 1, 1, 2))
        .group("isDistinct()/distinct() empty propagation")
        .testTrue("{}.isDistinct()", "isDistinct() of empty literal is true")
        .testEmpty("{}.distinct()", "distinct() of empty literal is empty")
        .testTrue(
            "distinctArray.where($this > 100).isDistinct()",
            "isDistinct() of a computed-empty array (filtered to nothing) is true")
        .testEmpty(
            "distinctArray.where($this > 100).distinct()",
            "distinct() of a computed-empty array (filtered to nothing) is empty")
        .testTrue("emptyString.isDistinct()", "isDistinct() of typed-empty field is true")
        .testEmpty("emptyString.distinct()", "distinct() of typed-empty field is empty")
        .group("isDistinct()/distinct() singleton")
        .testTrue("singleString.isDistinct()", "isDistinct() of a singleton is true")
        .testEquals(
            "test", "singleString.distinct()", "distinct() of a singleton returns that value")
        .group("isDistinct()/distinct() over arrays")
        .testTrue("distinctArray.isDistinct()", "isDistinct() of an all-distinct array is true")
        .testEquals(
            List.of(1, 2, 3),
            "distinctArray.distinct()",
            "distinct() of an all-distinct array returns it unchanged")
        .testFalse(
            "duplicateArray.isDistinct()", "isDistinct() of an array with a duplicate is false")
        .testEquals(
            List.of(1, 2),
            "duplicateArray.distinct()",
            "distinct() of an array with a duplicate removes it")
        .build();
  }

  /**
   * Mirrors the type matrix in {@link CombiningFunctionsDslTest}, since {@code isDistinct()} and
   * {@code distinct()} reuse the same equals-based deduplication as {@code union()}/{@code
   * combine()}. {@code combine()} is used here to construct arrays with controlled duplicates for
   * types the map-based builder cannot express directly (equal Quantity values in different units,
   * identical Codings).
   */
  @FhirPathTest
  public Stream<DynamicTest> testIsDistinctAndDistinctTypeMatrix() {
    return builder()
        .withSubject(sb -> sb)
        .group("isDistinct()/distinct() — Boolean")
        .testFalse("true.combine(true).isDistinct()", "Boolean duplicate is not distinct")
        .testTrue("true.combine(true).distinct()", "Boolean distinct() deduplicates")
        .group("isDistinct()/distinct() — Integer")
        .testFalse("1.combine(1).isDistinct()", "Integer duplicate is not distinct")
        .testEquals(1, "1.combine(1).distinct()", "Integer distinct() deduplicates")
        .group("isDistinct()/distinct() — Decimal")
        .testFalse("1.1.combine(1.1).isDistinct()", "Decimal duplicate is not distinct")
        .testEquals(1.1, "1.1.combine(1.1).distinct()", "Decimal distinct() deduplicates")
        .group("isDistinct()/distinct() — String")
        .testFalse("'a'.combine('a').isDistinct()", "String duplicate is not distinct")
        .testEquals("a", "'a'.combine('a').distinct()", "String distinct() deduplicates")
        .group("isDistinct()/distinct() — Date")
        .testFalse(
            "@2020-01-01.combine(@2020-01-01).isDistinct()", "Date duplicate is not distinct")
        .testEquals(
            toDate("2020-01-01"),
            "@2020-01-01.combine(@2020-01-01).distinct()",
            "Date distinct() deduplicates")
        .group("isDistinct()/distinct() — DateTime")
        .testFalse(
            "@2020-01-01T10:00:00.combine(@2020-01-01T10:00:00).isDistinct()",
            "DateTime duplicate is not distinct")
        .testEquals(
            toDateTime("2020-01-01T10:00:00"),
            "@2020-01-01T10:00:00.combine(@2020-01-01T10:00:00).distinct()",
            "DateTime distinct() deduplicates")
        .group("isDistinct()/distinct() — Time")
        .testFalse("@T12:00.combine(@T12:00).isDistinct()", "Time duplicate is not distinct")
        .testEquals(
            toTime("12:00"), "@T12:00.combine(@T12:00).distinct()", "Time distinct() deduplicates")
        .group("isDistinct()/distinct() — Quantity (equal under Quantity equality)")
        .testFalse(
            "1000 'mg'.combine(1 'g').isDistinct()",
            "Quantity values equal under Quantity equality are not distinct")
        .testEquals(
            toQuantity("1000 'mg'"),
            "1000 'mg'.combine(1 'g').distinct()",
            "Quantity distinct() deduplicates equal values")
        .group("isDistinct()/distinct() — Coding")
        .testFalse(
            "http://loinc.org|8867-4||'Heart rate'.combine("
                + "http://loinc.org|8867-4||'Heart rate').isDistinct()",
            "Identical Codings are not distinct")
        .testEquals(
            toCoding("http://loinc.org|8867-4||'Heart rate'"),
            "http://loinc.org|8867-4||'Heart rate'.combine("
                + "http://loinc.org|8867-4||'Heart rate').distinct()",
            "Coding distinct() deduplicates identical codings")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testIsDistinctAndDistinctComplexTypeError() {
    return builder()
        .withSubject(
            sb ->
                sb.elementArray(
                    "people",
                    person1 -> person1.string("name", "Alice"),
                    person2 -> person2.string("name", "Bob")))
        .group("isDistinct()/distinct() reject complex, non-equatable types")
        .testError("people.isDistinct()", "isDistinct() on a complex/backbone type raises an error")
        .testError("people.distinct()", "distinct() on a complex/backbone type raises an error")
        .build();
  }
}
