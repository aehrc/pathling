package au.csiro.pathling.fhirpath.dsl;

import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;

/**
 * Tests for FHIRPath boolean logic functions required by SQL on FHIR sharable view profile: - not()
 * function
 */
@Tag("UnitTest")
public class BooleanLogicFunctionsDslTest extends FhirPathDslTestBase {

  @FhirPathTest
  public Stream<DynamicTest> testNot() {
    return builder()
        .withSubject(sb -> sb
            // Boolean values
            .bool("trueValue", true)
            .bool("falseValue", false)
            .boolEmpty("emptyBoolean")
            // String values
            .string("stringValue", "value")
            .stringEmpty("emptyString")
            .stringArray("stringArray", "value1", "valu2")
            // Boolean arrays
            .boolArray("boolArray", true, false, true)
            // Complex types with boolean properties
            .element("person", person -> person
                .string("name", "John")
                .bool("active", true))
            .elementArray("people",
                person1 -> person1
                    .string("name", "Alice")
                    .bool("active", true),
                person2 -> person2
                    .string("name", "Bob")
                    .bool("active", false))
            .element("choiceField",
                val1 -> val1.choice("value")
                    .string("valueString", "123")
                    .integerEmpty("valueInteger")
            )
        )
        .group("not() function")
        // Basic not() tests
        .testEquals(false, "trueValue.not()",
            "not() negates true to false")
        .testEquals(true, "falseValue.not()",
            "not() negates false to true")
        // empty collections
        .testEmpty("emptyBoolean.not()",
            "not() returns empty for empty boolean")
        .testEmpty("{}.not()",
            "not() returns empty for empty collection")
        .testEmpty("undefined.not()",
            "not() returns empty for empty collection")
        .testEmpty("emptyString.not()",
            "not() returns empty for empty String collection")
        .testEmpty("stringValue.where($this.empty()).not()",
            "not() returns empty for calculated empty String collection")
        .testEmpty("stringArray.where($this.empty()).not()",
            "not() returns empty for calculated empty String collection")
        .testEmpty("%resource.ofType(Condition).not()",
            "not() returns empty for empty Resource collection")
        // boolean evaluation of collections
        .testFalse("%resource.not()",
            "not() false for root resource collection")
        .testFalse("stringValue.not()",
            "not() false for singular string element")
        .testFalse("stringArray.where($this='value1').not()",
            "not() false for calculated singular  element")
        .testFalse("choiceField.value.not()",
            "not() false for singular choice element")
        .testFalse("choiceField.value.ofType(string).not()",
            "not() false for singular resolved choice element")
        .testEmpty("choiceField.value.ofType(integer).not()",
            "not() empty for empty resolved choice element")
        .testError("stringArray.not()",
            "not() fails on non-boolean non-singular collection")
        
        // not() with boolean arrays
        .testEquals(List.of(false, true, false), "boolArray.not()",
            "not() negates each value in a boolean array")
        // not() with complex types
        .testEquals(false, "person.active.not()",
            "not() negates boolean property of complex type")
        .testEquals(List.of(false, true), "people.active.not()",
            "not() negates boolean property of each item in complex type array")

        // Chained not() tests
        .testEquals(true, "falseValue.not().not().not()",
            "not() can be chained multiple times")
        .testTrue("trueValue.not() = false",
            "not() result can be compared with boolean literal")

        // not() with boolean expressions
        .testEquals(false, "(trueValue and trueValue).not()",
            "not() negates result of 'and' operation")
        .testEquals(false, "(falseValue or trueValue).not()",
            "not() negates result of 'or' operation")
        .testEquals(true, "(falseValue and trueValue).not()",
            "not() negates result of 'and' operation with false")

        // not() with boolean conditions
        .testTrue("people.where(active.not()).name = 'Bob'",
            "not() can be used in where() conditions")
        .build();
  }
}
