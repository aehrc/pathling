package au.csiro.pathling.fhirpath.dsl;

import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;

/**
 * Tests for FHIRPath string functions as defined in supported.md: - join([separator: String]) :
 * String
 */
public class StringFunctionsDslTest extends FhirPathDslTestBase {

  @FhirPathTest
  public Stream<DynamicTest> testJoin() {
    return builder()
        .withSubject(sb -> sb
            // Empty values
            .stringEmpty("emptyString")
            // Single values
            .string("singleString", "test")
            // Arrays of strings
            .stringArray("stringArray", "one", "two", "three")
            // Arrays of other types
            .integerArray("integerArray", 1, 2, 3)
            .boolArray("booleanArray", true, false, true)
            // Complex types
            .element("person", person -> person
                .string("firstName", "John")
                .string("lastName", "Doe"))
            .elementArray("people",
                person1 -> person1
                    .string("firstName", "Alice")
                    .string("lastName", "Smith"),
                person2 -> person2
                    .string("firstName", "Bob")
                    .string("lastName", "Jones"))
        )
        .group("join() function with string arrays")
        // Basic join() tests with string arrays
        .testEquals("onetwothree", "stringArray.join()",
            "join() without separator concatenates strings")
        .testEquals("one,two,three", "stringArray.join(',')",
            "join() with comma separator works correctly")
        .testEquals("one | two | three", "stringArray.join(' | ')",
            "join() with complex separator works correctly")
        .testEmpty("emptyString.join(',')", "join() on empty string returns empty")

        .group("join() function with single values")
        .testEquals("test", "singleString.join()", "join() on single string returns the string")
        .testEquals("test", "singleString.join(',')",
            "join() on single string with separator returns the string")

        .group("join() function with complex expressions")
        .testEquals("Alice,Bob", "people.where(lastName.exists()).firstName.join(',')",
            "join() works with selected properties")
        .group("join() function error cases")
        .testError("integerArray.join()", "join() error non-string type")
        .testError("person.join(',')", "join() errors on non-collection types")

        .build();
  }
}
