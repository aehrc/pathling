package au.csiro.pathling.fhirpath.dsl;

import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import java.util.List;
import java.util.stream.Stream;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Resource;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;

/**
 * Tests for FHIRPath path traversal related operations.
 */
@Tag("UnitTest")
public class SystemDslTest extends FhirPathDslTestBase {

  @FhirPathTest
  public Stream<DynamicTest> testLiterals() {
    return builder()
        .withSubject(sb -> sb
            .string("stringValue", "test")
        )
        .group("Supported literal types")
        // String literals
        .testEquals("test", "'test'", "Simple string literal")
        .testEquals("test with spaces", "'test with spaces'", "String literal with spaces")
        .testEquals("", "''", "Empty string literal")
        // Integer literals
        .testEquals(42, "42", "Positive integer literal")
        .testEquals(-42, "-42", "Negative integer literal")
        .testEquals(0, "0", "Zero integer literal")
        // Decimal literals
        .testEquals(3.14, "3.14", "Positive decimal literal")
        .testEquals(-3.14, "-3.14", "Negative decimal literal")
        .testEquals(0.0, "0.0", "Zero decimal literal")
        // Boolean literals
        .testEquals(true, "true", "True boolean literal")
        .testEquals(false, "false", "False boolean literal")
        // Empty literal
        .testEmpty("{}", "Empty literal")
        // Coding literals
        .testTrue("(http://example.org|code).exists()", "Simple Coding literal exists")
        .testEquals("http://example.org", "(http://example.org|code).system",
            "Coding literal system")
        .testEquals("code", "(http://example.org|code).code", "Coding literal code")
        .testEquals("display", "(http://example.org|code||display).display",
            "Coding literal display")
        .testEquals(true, "(http://example.org|code|v1|display|true).userSelected",
            "Coding literal userSelected")

        .group("String literal escape sequences")
        .testEquals("line1\nline2", "'line1\\nline2'", "String literal with newline")
        .testEquals("tab\tcharacter", "'tab\\tcharacter'", "String literal with tab")
        .testEquals("carriage\rreturn", "'carriage\\rreturn'",
            "String literal with carriage return")
        .testEquals("form\ffeed", "'form\\ffeed'", "String literal with form feed")
        .testEquals("back`tick", "'back\\`tick'", "String literal with backtick")
        .testEquals("Unicode: Peter", "'Unicode: P\\u0065ter'",
            "String literal with Unicode escape")
        .testEquals("All escapes: \\\r\n\t\f\"`'",
            "'All escapes: \\\\\\r\\n\\t\\f\\\"`\\''",
            "String literal with all escape sequences")
        .group("Unsupported literal types")
        .testError("@2019-02-04", "DateTime literal is not supported")
        .testError("@2019-02-04T14:34:28+09:00", "DateTime with time literal is not supported")
        .testError("@T14:34:28", "Time literal is not supported")
        .testError("10 'mg'", "Quantity literal is not supported")
        .testError("4 days", "Time quantity literal is not supported")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testBooleanEvaluation() {
    return builder()
        .withSubject(sb -> sb
            // Boolean values
            .bool("trueValue", true)
            .bool("falseValue", false)
            .boolEmpty("emptyBoolean")
            .boolArray("booleanArray", true, false, true)
            .string("stringValue", "value")
            .stringEmpty("emptyString")
            .stringArray("stringArrayOne", "value1")
            .stringArray("stringArray", "value1", "value2")
            .codingEmpty("emptyCoding")
            .coding("codingValue", "http://example.org|code|display")
            .element("choiceField",
                val1 -> val1.choice("value")
                    .string("valueString", "123")
                    .integerEmpty("valueInteger")
            )
            .elementArray("complex",
                person1 -> person1
                    .string("id", "1")
                    .stringEmpty("singularString")
                    .stringArray("oneString", "Alias2")
                    .stringArray("manyStrings", "Alias2", "Alias1"),
                person2 -> person2
                    .string("id", "2")
                    .string("singularString", "Alice")
                    .stringArray("oneString", "Alias1")
                    .stringArray("manyStrings", "Alias2", "Alias1")
            )
        )
        .group("empty evaluation for functions")
        .testEmpty("booleanEmpty.not()", "empty boolean evaluates to empty in function")
        .testEmpty("{}.not()", "empty literal {} evaluates to empty in function")
        .testEmpty("unknown.not()", "undefined field evaluates to empty in function")
        .testEmpty("emptyString.not()", "empty string evaluates to empty in function")
        .testEmpty("stringArray.where($this.empty()).not()",
            "computed empty string collection evaluates to empty in function")
        .testEmpty("emptyCoding.not()", "empty Coding evaluates to empty in function")
        .testEmpty("emptyCoding.where($this.empty()).not()",
            "computed empty Coding evaluates to empty in function")
        .testEmpty("choiceField.value.ofType(integer).not()",
            "empty choice evaluates to empty in function")
        .group("empty evaluation for operators")
        .testEmpty("booleanEmpty and true", "empty boolean evaluates to empty in boolean operator")
        .testEmpty("false or {}", "empty literal {} evaluates to empty in boolean operator")
        .testEmpty("unknown xor true", "undefined field evaluates to empty in boolean operator")
        .testEmpty("true and emptyString", "empty string evaluates to empty in boolean operator")
        .testEmpty("stringArray.where($this.empty()) or false",
            "computed empty string collection evaluates to empty in boolean operator")
        .testEmpty("false xor emptyCoding", "empty Coding evaluates to empty in boolean operator")
        .testEmpty("emptyCoding.where($this.empty()) or false",
            "computed empty Coding evaluates to empty in boolean operator")
        .testEmpty("true and choiceField.value.ofType(integer)",
            "empty choice evaluates to empty in boolean operator")
        .group("single element collection evaluate to true in functions")
        .testFalse("trueValue.not()", "single true boolean evaluates to true in function")
        .testTrue("falseValue.not()", "single true boolean evaluates to true in function")
        .testFalse("stringValue.not()", "single string evaluates to true in function")
        .testFalse("codingValue.not()", "single Coding evaluates to true in function")
        .testFalse("stringArrayOne.not()", "array of one String evaluates to true in function")
        .testFalse("stringArray.where($this='value1').not()",
            "computed array of one String evaluates to true in function")
        .testFalse("%resource.not()", "single resource evaluates to true in function")
        .testFalse("choiceField.value.not()",
            "single polymorphic choice evaluates to true in function")
        .testFalse("choiceField.value.ofType(string).not()",
            "single resolved choice evaluates to true in function")
        .group("single element collection evaluate to true in boolean operators")
        .testTrue("trueValue and true",
            "single true boolean evaluates to true in boolean operators")
        .testFalse("falseValue or false",
            "single true boolean evaluates to true in boolean operators")
        .testTrue("stringValue or {}", "single string evaluates to true in boolean operators")
        .testTrue("true and codingValue", "single Coding evaluates to true in boolean operators")
        .testTrue("{} or stringArrayOne",
            "array of one String evaluates to true in boolean operators")
        .testTrue("stringArray.where($this='value1') and true",
            "computed array of one String evaluates to true in boolean operators")
        .testTrue("choiceField.value and true",
            "single polymorphic choice evaluates to true in boolean operators")
        .testTrue("choiceField.value.ofType(string) or false",
            "single resolved choice evaluates to true in boolean operators")
        .group("collections with many elements fail to evaluate as boolean singleton")
        .testError("booleanArray.not()",
            "array of booleans evaluates to error as boolean singleton")
        .testError("stringArray.not()",
            "array of many strings evaluates to error as boolean singleton")
        .testError("stringArray.where($this.exists()).not()",
            "computed array many of strings evaluates to error as boolean singleton")
        .group("collections with many elements fail to evaluate in boolean operators")
        .testError(
            "booleanArray and true",
            "array of booleans evaluates to error in boolean operators")
        .testError("false or stringArray",
            "array of many strings evaluates to error in  boolean operators")
        .testError("true and stringArray.where($this.exists())",
            "computed array many of strings evaluates to error in  boolean operators")
        .group("boolean evaluation in boolean expressions")
        .testEquals("2", "complex.where($this.singularString).id",
            "boolean expression evaluates for non-empty singular string")
        .testEquals(List.of("1", "2"), "complex.where($this.oneString).id",
            "boolean expression evaluates to true for one ")
        .testError("Expecting a collection with a single element but it has many.",
            "complex.where($this.manyStrings).id",
            "boolean expression fails collection with many elements")
        .build();
  }


  @FhirPathTest
  public Stream<DynamicTest> testPathTraversal() {
    return builder()
        .withSubject(sb -> sb
            // Boolean values
            .bool("trueValue", true)
            .bool("falseValue", false)
        )
        .group("basic traversal")
        .testEmpty("unknown",
            "traversal to undefined property returns {}")
        .testTrue("trueValue",
            "correct traversal to a 'true' boolean property")
        .testFalse("falseValue",
            "correct traversal to a 'false' boolean property")
        .group("travrersal to complex literal types")
        .testEquals("urn:system1", "(urn:system1|code1).system",
            "correct traversal to a Coding literal system")
        .testEquals("code2", "(urn:system2|code2).code",
            "correct traversal to a Coding literal code")
        .testEquals("display3", "(urn:system2|code2||display3).display",
            "correct traversal to a Coding literal display")
        .testEquals(true, "(urn:system2|code2|v1|display|true).userSelected",
            "correct traversal to a Coding literal userSelected")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testPathTraversalOnFhirResource() {

    final Resource observation = new Observation()
        .setId("1");

    return builder()
        .withResource(observation)
        .group("basic traversal of FHIR resource")
        .testEmpty("unknown",
            "traversal to undefined property returns {}")
        .testEquals("1", "id",
            "correct traversal an ID property")
        .build();
  }


}
