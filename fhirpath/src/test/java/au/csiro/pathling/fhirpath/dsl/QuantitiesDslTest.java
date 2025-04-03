package au.csiro.pathling.fhirpath.dsl;

import static au.csiro.pathling.test.dsl.FhirTestExpectations.*;

import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;

import java.util.stream.Stream;

@Tag("UnitTest")
public class QuantitiesDslTest extends FhirPathDslTestBase {

  @FhirPathTest
  public Stream<DynamicTest> testQuantities() {
    return builder()
        .test("Correct flooring of decimal weekly quantities", tc -> tc
            .expression("@2016-02-28 + 1.5 weeks")
            .expectResult("2016-03-06"))
        .test("Correct flooring of decimal second quantities", tc -> tc
            .expression("@2016-02-28T00:00:00.000 + 1.7006 seconds")
            .expectResult("2016-02-28T00:00:01.700+00:00"))
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testMathOperations() {
    return builder()
        .group("Polarity operator")
        .test("Integer negation", tc -> tc
            .expression("-10")
            .expectResult(-10))
        .test("Integer positive", tc -> tc
            .expression("+7")
            .expectResult(7))
        .test("Decimal negation", tc -> tc
            .expression("-10.23")
            .expectResult(-10.23))
        .test("Decimal positive", tc -> tc
            .expression("+7.3")
            .expectResult(7.3))
        .test("Quantity positive", tc -> tc
            .expression("+10.23 'm'= 10.23 'm'")
            .expectResult(true))
        .test("Quantity negation", tc -> tc
            .expression("0 'm' - 7.2 'm'= -7.2 'm'")
            .expectResult(true))
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testWithExpectations() {
    return builder()
        .test("Using expectEquals",
            expectEquals(10, "5 + 5"))
        .test("Using expectTrue",
            expectTrue("10 > 5"))
        .test("Using expectFalse",
            expectFalse("5 > 10"))
        .test("Using expectEmpty",
            expectEmpty("{}.where($this > 0)"))
        .test("Using expectError", expectError("1 / 0"))
        .build();
  }
}
