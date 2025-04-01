package au.csiro.pathling.fhirpath.dsl;

import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import au.csiro.pathling.test.dsl.FhirPathTestBuilder;
import org.junit.jupiter.api.Tag;

@Tag("UnitTest")
public class ExistenceFunctionsDslTest extends FhirPathDslTestBase {

    @FhirPathTest
    public void testEmptyFunction(FhirPathTestBuilder builder) {
        builder.withSubject()
            .integer("n1", 1)
            .integerArray("an1", 1)
            .integerArray("an2", 1, 2)
            .string("s1", "a")
            .stringArray("sn1", "a")
            .stringArray("sn2", "a", "b")
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
            .test("Empty collection should return true")
                .expression("nothing.empty()")
                .expectResult(true)
            .and()
            .test("Computed empty collection is empty")
                .expression("n1.where($this=0).empty()")
                .expectResult(true)
            .and()
            .test("Resource singular empty collection is empty")
                .expression("gender.empty()")
                //.inputFile("Patient-empty.json")
                .expectResult(true)
            .and()
            .test("Resource plural empty collection is empty")
                .expression("name.given.empty()")
                //.inputFile("Patient-empty.json")
                .expectResult(true)
            .and()
            .test("Singular integer is not empty")
                .expression("n1.empty()")
                .expectResult(false)
            .and()
            .test("Plural integer is not empty")
                .expression("an1.empty()")
                .expectResult(false)
            .and()
            .test("Plural integer collection is not empty")
                .expression("an2.empty()")
                .expectResult(false);
    }

    @FhirPathTest
    public void testCountFunction(FhirPathTestBuilder builder) {
        builder.withSubject()
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
            .test("Empty literal should return 0")
                .expression("{}.count()")
                .expectResult(0)
            .and()
            .test("Empty collection has count 0")
                .expression("nothing.count()")
                .expectResult(0)
            .and()
            .test("Computed empty collection has count 0")
                .expression("n1.where($this=0).count()")
                .expectResult(0)
            .and()
            .test("Resource singular empty collection has count 0")
                .expression("gender.count()")
                //.inputFile("Patient-empty.json")
                .expectResult(0)
            .and()
            .test("Resource plural empty collection has count 0")
                .expression("name.given.count()")
                //.inputFile("Patient-empty.json")
                .expectResult(0)
            .and()
            .test("Singular integer has count 1")
                .expression("n1.count()")
                .expectResult(1)
            .and()
            .test("Plural integer has count 1")
                .expression("an1.count()")
                .expectResult(1)
            .and()
            .test("Plural integer collection has count 2")
                .expression("an2.count()")
                .expectResult(2);
    }
}
