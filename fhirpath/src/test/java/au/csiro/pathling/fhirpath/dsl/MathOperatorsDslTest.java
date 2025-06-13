package au.csiro.pathling.fhirpath.dsl;

import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;

@Tag("UnitTest")
public class MathOperatorsDslTest extends FhirPathDslTestBase {

  @FhirPathTest
  public Stream<DynamicTest> testBasicMathOperations() {
    return builder()
        .withSubject(sb -> sb
            .integer("int1", 5)
            .integer("int2", 2)
            .decimal("dec1", 5.5)
            .decimal("dec2", 2.5)
            .integerArray("intArray", 1, 2, 3)
            .decimalArray("decArray", 1.1, 2.2, 3.3)
        )
        .group("Integer math operations")
        // Integer addition
        .testEquals(7, "int1 + int2", "Integer addition with variables")
        .testEquals(7, "5 + 2", "Integer addition with literals")
        .testEquals(7, "int1 + 2", "Integer addition with variable and literal")

        // Integer subtraction
        .testEquals(3, "int1 - int2", "Integer subtraction with variables")
        .testEquals(3, "5 - 2", "Integer subtraction with literals")
        .testEquals(3, "int1 - 2", "Integer subtraction with variable and literal")

        // Integer multiplication
        .testEquals(10, "int1 * int2", "Integer multiplication with variables")
        .testEquals(10, "5 * 2", "Integer multiplication with literals")
        .testEquals(10, "int1 * 2", "Integer multiplication with variable and literal")

        // Integer division
        .testEquals(2.5, "int1 / int2", "Integer division with variables")
        .testEquals(2.5, "5 / 2", "Integer division with literals")
        .testEquals(2.5, "int1 / 2", "Integer division with variable and literal")

        // Integer mod
        .testEquals(1, "int1 mod int2", "Integer mod with variables")
        .testEquals(1, "5 mod 2", "Integer mod with literals")
        .testEquals(1, "int1 mod 2", "Integer mod with variable and literal")

        .group("Decimal math operations")
        // Decimal addition
        .testEquals(8.0, "dec1 + dec2", "Decimal addition with variables")
        .testEquals(8.0, "5.5 + 2.5", "Decimal addition with literals")
        .testEquals(8.0, "dec1 + 2.5", "Decimal addition with variable and literal")

        // Decimal subtraction
        .testEquals(3.0, "dec1 - dec2", "Decimal subtraction with variables")
        .testEquals(3.0, "5.5 - 2.5", "Decimal subtraction with literals")
        .testEquals(3.0, "dec1 - 2.5", "Decimal subtraction with variable and literal")

        // Decimal multiplication
        .testEquals(13.75, "dec1 * dec2", "Decimal multiplication with variables")
        .testEquals(13.75, "5.5 * 2.5", "Decimal multiplication with literals")
        .testEquals(13.75, "dec1 * 2.5", "Decimal multiplication with variable and literal")

        // Decimal division
        .testEquals(2.2, "dec1 / dec2", "Decimal division with variables")
        .testEquals(2.2, "5.5 / 2.5", "Decimal division with literals")
        .testEquals(2.2, "dec1 / 2.5", "Decimal division with variable and literal")

        .group("Mixed type math operations")
        // Integer and decimal operations
        .testEquals(7.5, "int1 + dec2", "Integer + Decimal")
        .testEquals(10.5, "5 + 5.5", "Integer literal + Decimal literal")
        .testEquals(2.5, "int1 - dec2", "Integer - Decimal")
        .testEquals(12.5, "int1 * dec2", "Integer * Decimal")
        .testEquals(2.0, "int1 / dec2", "Integer / Decimal")

        .group("Error cases with collections")
        .testError("intArray + 1", "Addition with array")
        .testError("1 + intArray", "Addition with array")
        .testError("intArray * 2", "Multiplication with array")
        .testError("decArray / 2", "Division with array")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testMathWithEmptyArguments() {
    return builder()
        .withSubject(sb -> sb
            .integer("integerValue", 10)
            .decimal("decimalValue", 11.5)
            .bool("active", true)
        )
        .group("Math with empty arguments")
        .testEmpty("integerValue + {}", "Addition with empty literal")
        .testEmpty("{} - decimalValue", "Subtraction with empty literal")
        .testEmpty("1.5 * {}", "Multiplication with empty literal")
        .testEmpty("{}.where(true) / 2", "Division with empty literal where true")
        .testEmpty("2 + foo", "Addition with empty literal where foo is not defined")
        .testEmpty("foo - {}", "Subtraction with of two empty collections")
        .testError("2 * true", "Multiplication with boolean literal")
        .testError("2 / active", "Division with boolean literal where active is boolean")
        .testError("2 + true.where(%resource.active)",
            "Addition with boolean literal where active is boolean and true")
        .testError("2 - true.where(false)", "Subtraction with boolean literal where false")
        .testError("2 * true.where({})", "Multiplication with boolean literal where empty collection")
        .testError("2 / true.where(1 = 2)",
            "Division with boolean literal where condition is false")
        .build();
  }
}
