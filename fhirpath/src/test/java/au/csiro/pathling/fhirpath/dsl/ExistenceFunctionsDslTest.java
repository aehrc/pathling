package au.csiro.pathling.fhirpath.dsl;

import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;

/**
 * Tests for FHIRPath existence functions as defined in supported.md:
 * - exists()
 * - empty()
 * - count()
 * - allTrue()
 * - allFalse()
 * - anyTrue()
 * - anyFalse()
 */
@Tag("UnitTest")
public class ExistenceFunctionsDslTest extends FhirPathDslTestBase {

  @FhirPathTest
  public Stream<DynamicTest> testExists() {
    return builder()
        .withSubject(sb -> sb
            // Empty values
            .stringEmpty("emptyString")
            .complexEmpty("emptyComplex")
            // Single values
            .string("singleString", "test")
            .integer("singleInteger", 42)
            .bool("singleBoolean", true)
            .quantity("singleQuantity", "10 'mg'")
            // Arrays
            .stringArray("stringArray", "one", "two", "three")
            // Complex types
            .complex("person", person -> person
                .string("name", "John")
                .integer("age", 30)
                .bool("active", true))
            .complexArray("people",
                person1 -> person1
                    .string("name", "Alice")
                    .integer("age", 25)
                    .bool("active", true),
                person2 -> person2
                    .string("name", "Bob")
                    .integer("age", 40)
                    .bool("active", false))
        )
        .group("exists() function")
        // Basic exists() tests
        .testTrue("singleString.exists()", "exists() returns true for a single string")
        .testTrue("stringArray.exists()", "exists() returns true for a non-empty array")
        .testFalse("emptyString.exists()", "exists() returns false for an empty value")
        .testTrue("singleInteger.exists()", "exists() returns true for a single integer")
        .testTrue("singleBoolean.exists()", "exists() returns true for a single boolean")
        .testTrue("singleQuantity.exists()", "exists() returns true for a single quantity")
        .testTrue("person.exists()", "exists() returns true for a complex type")
        .testTrue("people.exists()", "exists() returns true for an array of complex types")
        .testFalse("emptyComplex.exists()", "exists() returns false for an empty complex type")
        
        // exists() with criteria
        .testTrue("stringArray.exists($this = 'one')", "exists() with criteria returns true when criteria matches")
        .testFalse("stringArray.exists($this = 'four')", "exists() with criteria returns false when criteria doesn't match")
        .testTrue("people.exists(name = 'Alice')", "exists() with criteria on complex type returns true when criteria matches")
        .testFalse("people.exists(name = 'David')", "exists() with criteria on complex type returns false when criteria doesn't match")
        .testTrue("people.exists(active = true)", "exists() with criteria on complex type returns true when criteria matches multiple items")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testEmpty() {
    return builder()
        .withSubject(sb -> sb
            // Empty values of different types
            .stringEmpty("emptyString")
            .integerEmpty("emptyInteger")
            .decimalEmpty("emptyDecimal")
            .quantityEmpty("emptyQuantity")
            .boolEmpty("emptyBoolean")
            .complexEmpty("emptyComplex")
            // Single values
            .string("singleString", "test")
            .integer("singleInteger", 42)
            .bool("singleBoolean", true)
            .quantity("singleQuantity", "10 'mg'")
            // Arrays
            .stringArray("stringArray", "one", "two", "three")
            // Complex types
            .complex("person", person -> person
                .string("name", "John")
                .integer("age", 30)
                .bool("active", true))
        )
        .group("empty() function")
        // empty() tests
        .testTrue("emptyString.empty()", "empty() returns true for an empty string")
        .testTrue("emptyInteger.empty()", "empty() returns true for an empty integer")
        .testTrue("emptyDecimal.empty()", "empty() returns true for an empty decimal")
        .testTrue("emptyQuantity.empty()", "empty() returns true for an empty quantity")
        .testTrue("emptyBoolean.empty()", "empty() returns true for an empty boolean")
        .testTrue("emptyComplex.empty()", "empty() returns true for an empty complex type")
        .testFalse("singleString.empty()", "empty() returns false for a single string")
        .testFalse("stringArray.empty()", "empty() returns false for a non-empty array")
        .testFalse("singleInteger.empty()", "empty() returns false for a single integer")
        .testFalse("singleBoolean.empty()", "empty() returns false for a single boolean")
        .testFalse("singleQuantity.empty()", "empty() returns false for a single quantity")
        .testFalse("person.empty()", "empty() returns false for a complex type")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testCount() {
    return builder()
        .withSubject(sb -> sb
            // Empty values
            .stringEmpty("emptyString")
            .integerEmpty("emptyInteger")
            .complexEmpty("emptyComplex")
            // Single values
            .string("singleString", "test")
            .integer("singleInteger", 42)
            .bool("singleBoolean", true)
            .quantity("singleQuantity", "10 'mg'")
            // Arrays
            .stringArray("stringArray", "one", "two", "three")
            .integerArray("integerArray", 1, 2, 3, 4, 5)
            .boolArray("booleanArray", true, false, true)
            .quantityArray("quantityArray", "5 'mg'", "10 'mg'", "15 'mg'")
            // Complex types
            .complex("person", person -> person
                .string("name", "John")
                .integer("age", 30)
                .bool("active", true))
            .complexArray("people",
                person1 -> person1
                    .string("name", "Alice")
                    .integer("age", 25)
                    .bool("active", true),
                person2 -> person2
                    .string("name", "Bob")
                    .integer("age", 40)
                    .bool("active", false),
                person3 -> person3
                    .string("name", "Charlie")
                    .integer("age", 35)
                    .bool("active", true))
        )
        .group("count() function")
        // count() tests
        .testEquals(0, "emptyString.count()", "count() returns 0 for an empty string")
        .testEquals(0, "emptyInteger.count()", "count() returns 0 for an empty integer")
        .testEquals(0, "emptyComplex.count()", "count() returns 0 for an empty complex type")
        .testEquals(1, "singleString.count()", "count() returns 1 for a single string")
        .testEquals(3, "stringArray.count()", "count() returns the correct count for a string array")
        .testEquals(1, "singleInteger.count()", "count() returns 1 for a single integer")
        .testEquals(5, "integerArray.count()", "count() returns the correct count for an integer array")
        .testEquals(1, "singleBoolean.count()", "count() returns 1 for a single boolean")
        .testEquals(3, "booleanArray.count()", "count() returns the correct count for a boolean array")
        .testEquals(1, "singleQuantity.count()", "count() returns 1 for a single quantity")
        .testEquals(3, "quantityArray.count()", "count() returns the correct count for a quantity array")
        .testEquals(1, "person.count()", "count() returns 1 for a complex type")
        .testEquals(3, "people.count()", "count() returns the correct count for an array of complex types")
        .build();
  }
  
  @FhirPathTest
  public Stream<DynamicTest> testAllTrue() {
    return builder()
        .withSubject(sb -> sb
            // Empty values
            .boolEmpty("emptyBoolean")
            // Single values
            .bool("trueBoolean", true)
            .bool("falseBoolean", false)
            .string("singleString", "test")
            // Arrays
            .boolArray("allTrueArray", true, true, true)
            .boolArray("allFalseArray", false, false, false)
            .boolArray("mixedBoolArray", true, false, true)
        )
        .group("allTrue() function")
        // allTrue() tests
        .testTrue("allTrueArray.allTrue()", "allTrue() returns true when all values are true")
        .testFalse("allFalseArray.allTrue()", "allTrue() returns false when all values are false")
        .testFalse("mixedBoolArray.allTrue()", "allTrue() returns false when some values are false")
        .testTrue("trueBoolean.allTrue()", "allTrue() returns true for a single true boolean")
        .testFalse("falseBoolean.allTrue()", "allTrue() returns false for a single false boolean")
        .testTrue("emptyBoolean.allTrue()", "allTrue() returns true for an empty collection")
        .testError("'string'.allTrue()", "allTrue() errors on non-boolean values")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testAllFalse() {
    return builder()
        .withSubject(sb -> sb
            // Empty values
            .boolEmpty("emptyBoolean")
            // Single values
            .bool("trueBoolean", true)
            .bool("falseBoolean", false)
            .string("singleString", "test")
            // Arrays
            .boolArray("allTrueArray", true, true, true)
            .boolArray("allFalseArray", false, false, false)
            .boolArray("mixedBoolArray", true, false, true)
        )
        .group("allFalse() function")
        // allFalse() tests
        .testFalse("allTrueArray.allFalse()", "allFalse() returns false when all values are true")
        .testTrue("allFalseArray.allFalse()", "allFalse() returns true when all values are false")
        .testFalse("mixedBoolArray.allFalse()", "allFalse() returns false when some values are true")
        .testFalse("trueBoolean.allFalse()", "allFalse() returns false for a single true boolean")
        .testTrue("falseBoolean.allFalse()", "allFalse() returns true for a single false boolean")
        .testTrue("emptyBoolean.allFalse()", "allFalse() returns true for an empty collection")
        .testError("'string'.allFalse()", "allFalse() errors on non-boolean values")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testAnyTrue() {
    return builder()
        .withSubject(sb -> sb
            // Empty values
            .boolEmpty("emptyBoolean")
            // Single values
            .bool("trueBoolean", true)
            .bool("falseBoolean", false)
            .string("singleString", "test")
            // Arrays
            .boolArray("allTrueArray", true, true, true)
            .boolArray("allFalseArray", false, false, false)
            .boolArray("mixedBoolArray", true, false, true)
        )
        .group("anyTrue() function")
        // anyTrue() tests
        .testTrue("allTrueArray.anyTrue()", "anyTrue() returns true when all values are true")
        .testFalse("allFalseArray.anyTrue()", "anyTrue() returns false when all values are false")
        .testTrue("mixedBoolArray.anyTrue()", "anyTrue() returns true when some values are true")
        .testTrue("trueBoolean.anyTrue()", "anyTrue() returns true for a single true boolean")
        .testFalse("falseBoolean.anyTrue()", "anyTrue() returns false for a single false boolean")
        .testFalse("emptyBoolean.anyTrue()", "anyTrue() returns false for an empty collection")
        .testError("'string'.anyTrue()", "anyTrue() errors on non-boolean values")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testAnyFalse() {
    return builder()
        .withSubject(sb -> sb
            // Empty values
            .boolEmpty("emptyBoolean")
            // Single values
            .bool("trueBoolean", true)
            .bool("falseBoolean", false)
            .string("singleString", "test")
            // Arrays
            .boolArray("allTrueArray", true, true, true)
            .boolArray("allFalseArray", false, false, false)
            .boolArray("mixedBoolArray", true, false, true)
        )
        .group("anyFalse() function")
        // anyFalse() tests
        .testFalse("allTrueArray.anyFalse()", "anyFalse() returns false when all values are true")
        .testTrue("allFalseArray.anyFalse()", "anyFalse() returns true when all values are false")
        .testTrue("mixedBoolArray.anyFalse()", "anyFalse() returns true when some values are false")
        .testFalse("trueBoolean.anyFalse()", "anyFalse() returns false for a single true boolean")
        .testTrue("falseBoolean.anyFalse()", "anyFalse() returns true for a single false boolean")
        .testFalse("emptyBoolean.anyFalse()", "anyFalse() returns false for an empty collection")
        .testError("'string'.anyFalse()", "anyFalse() errors on non-boolean values")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testChainedFunctions() {
    return builder()
        .withSubject(sb -> sb
            // Arrays
            .stringArray("stringArray", "one", "two", "three")
            .integerArray("integerArray", 1, 2, 3, 4, 5)
            // Complex types
            .complexArray("people",
                person1 -> person1
                    .string("name", "Alice")
                    .integer("age", 25)
                    .bool("active", true),
                person2 -> person2
                    .string("name", "Bob")
                    .integer("age", 40)
                    .bool("active", false),
                person3 -> person3
                    .string("name", "Charlie")
                    .integer("age", 35)
                    .bool("active", true))
        )
        .group("Chained function tests")
        // Chained function tests
        .testFalse("stringArray.exists().not().empty()", "Chained exists() and empty() functions work correctly")
        .testEquals(2, "people.where(active = true).count()", "Filtering with where() before count() works correctly")
        .testTrue("people.select(active).allTrue().not()", "Selecting and checking with allTrue() works correctly")
        .testTrue("people.select(active).anyTrue()", "Selecting and checking with anyTrue() works correctly")
        .testEquals(60, "people.where(active = true).select(age).sum()", "Chaining where(), select(), and sum() works correctly")
        .build();
  }
}
