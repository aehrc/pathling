package au.csiro.pathling.fhirpath.dsl;

import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;

/**
 * Tests for FHIRPath subsetting functions required by SQL on FHIR sharable view profile:
 * - first() function
 */
@Tag("UnitTest")
public class SubsettingFunctionsDslTest extends FhirPathDslTestBase {

  @FhirPathTest
  public Stream<DynamicTest> testFirst() {
    return builder()
        .withSubject(sb -> sb
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
            .element("person", person -> person
                .string("name", "John")
                .integer("age", 30)
                .bool("active", true))
            .elementArray("people",
                person1 -> person1
                    .string("name", "Alice")
                    .integer("age", 25)
                    .bool("active", true),
                person2 -> person2
                    .string("name", "Bob")
                    .integer("age", 40)
                    .bool("active", false))
        )
        .group("first() function")
        // Basic first() tests
        .testEquals("test", "singleString.first()", 
            "first() returns the single string value")
        .testEquals("one", "stringArray.first()", 
            "first() returns the first item in a string array")
        .testEmpty("emptyString.first()", 
            "first() returns empty for an empty string")
        .testEquals(42, "singleInteger.first()", 
            "first() returns the single integer value")
        .testEquals(true, "singleBoolean.first()", 
            "first() returns the single boolean value")
        
        // Complex type first() tests
        .testTrue("person.first().name = 'John'", 
            "first() returns the single complex type with expected name")
        .testTrue("people.first().name = 'Alice'", 
            "first() returns the first item in a complex type array with expected name")
        .testEmpty("emptyComplex.first()", 
            "first() returns empty for an empty complex type")
        .testEmpty("{}.first()", 
            "first() returns empty for an empty literal")
        
        // Chained first() tests
        .testEquals("Alice", "people.first().name", 
            "first() can be chained with property access")
        .testEquals(25, "people.first().age", 
            "first() can be chained with property access for integer")
        .build();
  }
}
