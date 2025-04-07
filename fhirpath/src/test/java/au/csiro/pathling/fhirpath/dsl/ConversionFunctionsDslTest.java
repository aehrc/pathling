package au.csiro.pathling.fhirpath.dsl;

import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;

@Tag("UnitTest")
public class ConversionFunctionsDslTest extends FhirPathDslTestBase {

  @FhirPathTest
  public Stream<DynamicTest> testToStringFunction() {

    return builder()
        .withSubject(sb -> sb
            .decimal("decimal1", 1.10)
            .integer("decimal1_scale", 2)
            .integer("n1", 1)
            .integerArray("an1", 1)
            .integerArray("an2", 1, 2)
            .string("s1", "a")
            .stringArray("sn1", "a")
            .stringArray("sn2", "a", "b")
            .complex("e1", e -> e
                .complexArray("xy",
                    xy -> xy.integer("x", 1).integer("y", 2),
                    xy -> xy.integer("x", 3)
                )
            )
            .complexArray("e2",
                e -> e.integer("a", 1).integer("b", 2),
                e -> e.integer("a", 3).integer("b", 4)
            )
        )
        .group("Empty collections")
        .testEmpty("{}.toString()", "toString of empty literal collection is {}")
        .testEmpty("empty.toString()", "toString of empty collection is {}")

        .group("String collections")
        .testEquals("", "''.toString()", "toString of empty string literal is identity")
        .testEquals("'ala'", "'\\'ala\\''.toString()", "toString of string literal is identity")
        .testEquals("a", "s1.toString()", "toString of string value is identity")

        .group("Boolean collections")
        .testEquals("true", "true.toString()", "true literal is 'true'")
        .testEquals("false", "false.toString()", "false literal is 'false'")
        .testEmpty("false.where($this).toString()", "computed empty boolean collection is {}")

        .group("Integer collections")
        .testEquals("13", "13.toString()", "toString of integer literal is correct")
        .testEquals("1", "n1.toString()", "toString of integer value is correct")

        .group("Decimal collections")
        .testEquals("1.000", "(1.000).toString()", "toString of decimal literal retains scale")
        .testEquals("1.10", "decimal1.toString()", "toString of decimal value retains scale")

        .group("Quantity collections")
        .testEquals("1.10 'm'", "(1.10 'm').toString()", "ucum quantity literal toString")
        .testEquals("1 year", "(1 year).toString()", "time quantity literal toString")

        .group("Coding collections")
        .testEquals("http://snomed.info/sct|52101004",
            "(http://snomed.info/sct|52101004).toString()",
            "toString of coding literal (sys:code) is correct")
        .testEquals("http://snomed.info/sct|52101004||Present",
            "(http://snomed.info/sct|52101004||Present).toString()",
            "toString of coding literal (sys:code:disp) is correct")
        .testEquals("http://snomed.info/sct|52101004|2.0",
            "(http://snomed.info/sct|52101004|2.0).toString()",
            "toString of coding literal (sys:code:ver) is correct")
        .testEquals("http://snomed.info/sct|52101004|1.0|Present",
            "(http://snomed.info/sct|52101004|1.0|Present).toString()",
            "toString of coding literal (sys:code:ver:disp) is correct")

        .group("Date-time collections")
        .testEquals("2019-01-01T10:00:00Z",
            "(@2019-01-01T10:00:00Z).toString()",
            "toString of date-time literal is correct")
        .testEquals("2019-01-01",
            "(@2019-01-01).toString()",
            "toString of date literal is correct")
        .testEquals("10:00:00",
            "(@T10:00:00).toString()",
            "toString of time literal is correct")

        .group("Complex type collections")
        .testEmpty("e1.toString()", "toString of complex type is {}")

        .group("Non-singular collections")
        .testError("an2.toString()", "toString of non-singular Integer collection fails")
        .testError("sn2.toString()", "toString of non-singular String collection fails")
        .testError("e2.toString()", "toString of non-singular complex type fails")
        
        .build();
  }
}
