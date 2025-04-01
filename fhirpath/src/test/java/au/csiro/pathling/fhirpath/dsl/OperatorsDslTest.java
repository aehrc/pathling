package au.csiro.pathling.fhirpath.dsl;

import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import au.csiro.pathling.test.dsl.FhirPathTestBuilder;
import org.junit.jupiter.api.Tag;

import java.util.Collections;

@Tag("UnitTest")
public class OperatorsDslTest extends FhirPathDslTestBase {

    @FhirPathTest
    void testMembershipOperators(FhirPathTestBuilder builder) {
        builder.withSubject()
            .integer("oneInt", 1)
            .integerArray("manyInt", 1, 2, 3)
            .string("oneString", "test")
            .stringArray("manyString", "test1", "test2", "test3")
            .bool("oneBoolean", true)
            .boolArray("manyBoolean", true, false, true)
            .decimal("oneDecimal", 1.5)
            .decimalArray("manyDecimal", 1.5, 2.5, 3.5)
            .dateTime("oneDateTime", "@2023-10-15T09:45:00Z")
            .dateTimeArray("manyDateTime", 
                "@2023-01-02T14:00:00Z", 
                "@2023-01-03T09:30:00Z", 
                "@2023-01-03T15:45:00Z")
            .date("oneDate", "@2023-10-15")
            .dateTimeArray("manyDate", 
                "@2023-01-02", 
                "@2023-01-03", 
                "@2023-01-05")
            .time("oneTime", "@T09:45:00")
            .timeArray("manyTime", 
                "@T14:00:00", 
                "@T09:30:00", 
                "@T15:45:00")
            .coding("oneCoding", "http://loinc.org|8867-4||'Heart rate'")
            .codingArray("manyCoding", 
                "http://loinc.org|8480-6||'Systolic blood pressure'",
                "http://loinc.org|8867-4||'Heart rate'",
                "http://loinc.org|8462-4||'Diastolic blood pressure'")
            .and()
            .group("Integer type")
            .test("In for existing Integer in many")
                .expression("2 in manyInt")
                .expectResult(true)
            .and()
            .test("Contains for existing Integer in many")
                .expression("manyInt contains 2")
                .expectResult(true)
            .and()
            .test("In for non-existing Integer in many")
                .expression("5 in manyInt")
                .expectResult(false)
            .and()
            .test("Contains non-existing Integer in many")
                .expression("manyInt contains 5")
                .expectResult(false)
            .and()
            .test("In for empty Integer in many")
                .expression("{} in manyInt")
                .expectResult(Collections.emptyList())
            .and()
            .test("Contains empty Integer in many")
                .expression("manyInt contains {}")
                .expectResult(Collections.emptyList())
            .and()
            .group("String type")
            .test("In for existing String in many")
                .expression("'test2' in manyString")
                .expectResult(true)
            .and()
            .test("Contains for existing String in many")
                .expression("manyString contains 'test2'")
                .expectResult(true)
            .and()
            .test("In for non-existing String in many")
                .expression("'test4' in manyString")
                .expectResult(false);
    }
}
