package au.csiro.pathling.fhirpath.dsl;

import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;

@Tag("UnitTest")
public class QuantitiesDslTest extends FhirPathDslTestBase {

  @FhirPathTest
  public Stream<DynamicTest> testQuantities() {
    return builder()
        .testEquals("2016-03-06", "@2016-02-28 + 1.5 weeks", 
            "Correct flooring of decimal weekly quantities")
        .testEquals("2016-02-28T00:00:01.700+00:00", "@2016-02-28T00:00:00.000 + 1.7006 seconds", 
            "Correct flooring of decimal second quantities")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testMathOperations() {
    return builder()
        .group("Polarity operator")
        .testEquals(-10, "-10", "Integer negation")
        .testEquals(7, "+7", "Integer positive")
        .testEquals(-10.23, "-10.23", "Decimal negation")
        .testEquals(7.3, "+7.3", "Decimal positive")
        .testTrue("+10.23 'm'= 10.23 'm'", "Quantity positive")
        .testTrue("0 'm' - 7.2 'm'= -7.2 'm'", "Quantity negation")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testWithExpectations() {
    return builder()
        .testEquals(10, "5 + 5", "Using testEquals")
        .testTrue("10 > 5", "Using testTrue")
        .testFalse("5 > 10", "Using testFalse")
        .testEmpty("{}.where($this > 0)", "Using testEmpty")
        .testError("1 / 0", "Using testError")
        .build();
  }
}
