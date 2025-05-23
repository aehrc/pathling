package au.csiro.pathling.fhirpath.dsl;

import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;

/**
 * Tests for FHIRPath existence functions as defined in supported.md: - exists() - empty() - count()
 * - allTrue() - allFalse() - anyTrue() - anyFalse()
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
        .testFalse("{}.exists()", "exists() returns false for an empty literal")

        // exists() with criteria
        .testTrue("stringArray.exists($this = 'one')",
            "exists() with criteria returns true when criteria matches")
        .testFalse("stringArray.exists($this = 'four')",
            "exists() with criteria returns false when criteria doesn't match")
        .testTrue("people.exists(name = 'Alice')",
            "exists() with criteria on complex type returns true when criteria matches")
        .testFalse("people.exists(name = 'David')",
            "exists() with criteria on complex type returns false when criteria doesn't match")
        .testTrue("people.exists(active = true)",
            "exists() with criteria on complex type returns true when criteria matches multiple items")
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
        .testTrue("{}.empty()", "empty() returns true for an empty literal")
        .testFalse("singleString.empty()", "empty() returns false for a single string")
        .testFalse("stringArray.empty()", "empty() returns false for a non-empty array")
        .testFalse("singleInteger.empty()", "empty() returns false for a single integer")
        .testFalse("singleBoolean.empty()", "empty() returns false for a single boolean")
        .testFalse("singleQuantity.empty()", "empty() returns false for a single quantity")
        .testFalse("person.empty()", "empty() returns false for a complex type")
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
        .testFalse("stringArray.exists().not().empty()",
            "Chained exists() and empty() functions work correctly")
        .build();
  }
}
