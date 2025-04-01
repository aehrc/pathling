package au.csiro.pathling.fhirpath.dsl;

import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import au.csiro.pathling.test.dsl.FhirPathTestBuilder;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;

import java.util.stream.Stream;

@Tag("UnitTest")
public class QuantitiesDslTest extends FhirPathDslTestBase {

    @FhirPathTest
    Stream<DynamicTest> testQuantities() {
        FhirPathTestBuilder builder = new FhirPathTestBuilder();
        builder.withSubject()
            .and()
            .test("Correct flooring of decimal weekly quantities")
                .expression("@2016-02-28 + 1.5 weeks")
                .expectResult("2016-03-06")
            .and()
            .test("Correct flooring of decimal second quantities")
                .expression("@2016-02-28T00:00:00.000 + 1.7006 seconds")
                .expectResult("2016-02-28T00:00:01.700+00:00");
                
        return builder.buildDynamicTests(this);
    }
    
    @FhirPathTest
    Stream<DynamicTest> testMathOperations() {
        FhirPathTestBuilder builder = new FhirPathTestBuilder();
        builder.withSubject()
            .and()
            .group("Polarity operator")
            .test("Integer negation")
                .expression("-10")
                .expectResult(-10)
            .and()
            .test("Integer positive")
                .expression("+7")
                .expectResult(7)
            .and()
            .test("Decimal negation")
                .expression("-10.23")
                .expectResult(-10.23)
            .and()
            .test("Decimal positive")
                .expression("+7.3")
                .expectResult(7.3)
            .and()
            .test("Quantity positive")
                .expression("+10.23 'm'= 10.23 'm'")
                .expectResult(true)
            .and()
            .test("Quantity negation")
                .expression("0 'm' - 7.2 'm'= -7.2 'm'")
                .expectResult(true);
                
        return builder.buildDynamicTests(this);
    }
}
