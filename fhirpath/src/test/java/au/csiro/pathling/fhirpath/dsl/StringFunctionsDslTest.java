package au.csiro.pathling.fhirpath.dsl;

import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import au.csiro.pathling.test.dsl.FhirPathTestBuilder;
import org.junit.jupiter.api.Tag;

import java.util.Collections;

@Tag("UnitTest")
public class StringFunctionsDslTest extends FhirPathDslTestBase {

    @FhirPathTest
    void testStringFunctions(FhirPathTestBuilder builder) {
        builder.withSubject()
            .string("s1", "hello")
            .stringArray("sn1", "hello", "world")
            .integer("n1", 42)
            .and()
            .group("String operations")
            .test("String concatenation")
                .expression("s1 + ' world'")
                .expectResult("hello world")
            .and()
            .test("String length")
                .expression("s1.length()")
                .expectResult(5)
            .and()
            .test("String with error")
                .expression("s1.nonExistentFunction()")
                .expectError();
    }
    
    @FhirPathTest
    void testExistenceFunctions(FhirPathTestBuilder builder) {
        builder.withSubject()
            .string("s1", "a")
            .stringArray("sn1", "a")
            .stringArray("sn2", "a", "b")
            .integer("n1", 1)
            .integerArray("an1", 1)
            .integerArray("an2", 1, 2)
            .complex("e1", e -> e
                .complexArray("xy", 
                    xy -> xy.property("x", 1).property("y", 2),
                    xy -> xy.property("x", 3)
                )
            )
            .and()
            .test("Empty literal should return true")
                .expression("{}.empty()")
                .expectResult(true)
            .and()
            .test("Singular integer is not empty")
                .expression("n1.empty()")
                .expectResult(false)
            .and()
            .test("Plural integer collection is not empty")
                .expression("an2.empty()")
                .expectResult(false);
    }
    
    @FhirPathTest
    void testToStringFunction(FhirPathTestBuilder builder) {
        builder.withSubject()
            .string("s1", "a")
            .integer("n1", 1)
            .decimal("decimal1", 1.10)
            .complex("e1", e -> e
                .complexArray("xy", 
                    xy -> xy.property("x", 1).property("y", 2),
                    xy -> xy.property("x", 3)
                )
            )
            .integerArray("an2", 1, 2)
            .stringArray("sn2", "a", "b")
            .complex("e2", e -> e
                .property("a", 1)
                .property("b", 2)
            )
            .and()
            .group("Empty collections")
            .test("toString of empty literal collection is {}")
                .expression("{}.toString()")
                .expectResult(Collections.emptyList())
            .and()
            .test("toString of empty collection is {}")
                .expression("empty.toString()")
                .expectResult(Collections.emptyList())
            .and()
            .group("String collections")
            .test("toString of empty string literal is identity")
                .expression("''.toString()")
                .expectResult("")
            .and()
            .test("toString of string literal is identity")
                .expression("'\\'ala\\''.toString()")
                .expectResult("'ala'")
            .and()
            .test("toString of string value is identity")
                .expression("s1.toString()")
                .expectResult("a")
            .and()
            .group("Boolean collections")
            .test("true literal is 'true'")
                .expression("true.toString()")
                .expectResult("true")
            .and()
            .test("false literal is 'false'")
                .expression("false.toString()")
                .expectResult("false")
            .and()
            .group("Integer collections")
            .test("toString of integer literal is correct")
                .expression("13.toString()")
                .expectResult("13")
            .and()
            .test("toString of integer value is correct")
                .expression("n1.toString()")
                .expectResult("1")
            .and()
            .group("Decimal collections")
            .test("toString of decimal literal retains scale")
                .expression("(1.000).toString()")
                .expectResult("1.000")
            .and()
            .test("toString of decimal value retains scale")
                .expression("decimal1.toString()")
                .expectResult("1.10")
            .and()
            .group("Non-singular collections")
            .test("toString of non-singular Integer collection fails")
                .expression("an2.toString()")
                .expectError()
            .and()
            .test("toString of non-singular String collection fails")
                .expression("sn2.toString()")
                .expectError()
            .and()
            .test("toString of non-singular complex type fails")
                .expression("e2.toString()")
                .expectError();
    }
}
