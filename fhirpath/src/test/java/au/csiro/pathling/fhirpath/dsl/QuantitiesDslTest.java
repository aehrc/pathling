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
        .test("Correct flooring of decimal weekly quantities", 
            expectEquals("2016-03-06", "@2016-02-28 + 1.5 weeks"))
        .test("Correct flooring of decimal second quantities", 
            expectEquals("2016-02-28T00:00:01.700+00:00", "@2016-02-28T00:00:00.000 + 1.7006 seconds"))
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testMathOperations() {
    return builder()
        .group("Polarity operator")
        .test("Integer negation", 
            expectEquals(-10, "-10"))
        .test("Integer positive", 
            expectEquals(7, "+7"))
        .test("Decimal negation", 
            expectEquals(-10.23, "-10.23"))
        .test("Decimal positive", 
            expectEquals(7.3, "+7.3"))
        .test("Quantity positive", 
            expectTrue("+10.23 'm'= 10.23 'm'"))
        .test("Quantity negation", 
            expectTrue("0 'm' - 7.2 'm'= -7.2 'm'"))
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
