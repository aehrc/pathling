package au.csiro.pathling.fhirpath.dsl;

import static au.csiro.pathling.test.yaml.FhirTypedLiteral.toCoding;
import static au.csiro.pathling.test.yaml.FhirTypedLiteral.toQuantity;

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
        .withSubject(sb -> sb
            .quantity("q1", "1.5 'm'")
            .coding("c1", "http://loinc.org|8480-6||'Systolic blood pressure'")
        )
        .testEquals("2016-03-06", "@2016-02-28 + 1.5 weeks",
            "Correct flooring of decimal weekly quantities")
        .testEquals("2016-02-28T00:00:01.700+00:00", "@2016-02-28T00:00:00.000 + 1.7006 seconds",
            "Correct flooring of decimal second quantities")
        .testEquals("1.5 'm'", "q1.toString()",
            "Correct string representation of quantity")
        .testTrue("150 'cm' = q1",
            "Correct equality of quantities")
        .testTrue("3.0 'm' = (q1 + q1)",
            "Correct equality of quantities")
        .testEquals(toQuantity("1.5 'm'"), "q1",
            "Correct addition of quantities")
        .testEquals(toCoding("http://loinc.org|8480-6||'Systolic blood pressure'"),
            "c1.first()",
            "Correct comparison of coding")
        .build();
  }
}
