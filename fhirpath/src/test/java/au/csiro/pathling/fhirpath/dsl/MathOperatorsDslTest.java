package au.csiro.pathling.fhirpath.dsl;

import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;

import java.util.stream.Stream;

@Tag("UnitTest")
public class MathOperatorsDslTest extends FhirPathDslTestBase {

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
        .testEmpty("{} + decimalValue", "Addition with empty literal")
        // 2 + {} -> {}
        // 2 + {}.where(true) -> {}
        // 2 + foo -> {}
        .testEmpty("1.5 + {}", "Addition with empty literal")
        .testEmpty("{}.where(true) + 2", "Addition with empty literal where true")
        .testEmpty("2 + foo", "Addition with empty literal where foo is not defined")
        .testEmpty("foo + {}", "Addition with of two empty collections")

        // 2 + true -> error
        // 2 + active -> error
        // 2 + true.where(%resource.active) -> error
        // 2 + true.where(false) -> error
        // 2 + true.where({}) -> error  // because of boolean evaluation of collections
        // 2 + true.where(1 = 2) -> error

        .testError("2 + true", "Addition with boolean literal")
        .testError("2 + active", "Addition with boolean literal where active is boolean")
        .testError("2 + true.where(%resource.active)",
            "Addition with boolean literal where active is boolean and true")
        .testError("2 + true.where(false)", "Addition with boolean literal where false")
        .testError("2 + true.where({})", "Addition with boolean literal where empty collection")
        .testError("2 + true.where(1 = 2)",
            "Addition with boolean literal where condition is false")
        .build();
  }
}
