package au.csiro.pathling.fhirpath.dsl;

import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;

@Tag("UnitTest")
public class ExistenceFunctionsDslTest extends FhirPathDslTestBase {

  @FhirPathTest
  public Stream<DynamicTest> testEmptyFunction() {
    return builder()
        .withSubject(sb -> sb
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
            ))
        .test("Empty literal should return true", tc -> tc
            .expression("{}.empty()")
            .expectResult(true))
        .test("Empty collection should return true", tc -> tc
            .expression("nothing.empty()")
            .expectResult(true))
        .test("Computed empty collection is empty", tc -> tc
            .expression("n1.where($this=0).empty()")
            .expectResult(true))
        .test("Resource singular empty collection is empty", tc -> tc
            .expression("gender.empty()")
            //.inputFile("Patient-empty.json")
            .expectResult(true))
        .test("Resource plural empty collection is empty", tc -> tc
            .expression("name.given.empty()")
            //.inputFile("Patient-empty.json")
            .expectResult(true))
        .test("Singular integer is not empty", tc -> tc
            .expression("n1.empty()")
            .expectResult(false))
        .test("Plural integer is not empty", tc -> tc
            .expression("an1.empty()")
            .expectResult(false))
        .test("Plural integer collection is not empty", tc -> tc
            .expression("an2.empty()")
            .expectResult(false))
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testCountFunction() {
    return builder()
        .withSubject(sb -> sb
            .integer("n1", 1)
            .integerArray("an1", 1)
            .integerArray("an2", 1, 2)
            .complex("e1", e -> e
                .complexArray("xy",
                    xy -> xy.property("x", 1).property("y", 2),
                    xy -> xy.property("x", 3)
                )
            ))
        .test("Empty literal should return 0", tc -> tc
            .expression("{}.count()")
            .expectResult(0))
        .test("Empty collection has count 0", tc -> tc
            .expression("nothing.count()")
            .expectResult(0))
        .test("Computed empty collection has count 0", tc -> tc
            .expression("n1.where($this=0).count()")
            .expectResult(0))
        .test("Resource singular empty collection has count 0", tc -> tc
            .expression("gender.count()")
            //.inputFile("Patient-empty.json")
            .expectResult(0))
        .test("Resource plural empty collection has count 0", tc -> tc
            .expression("name.given.count()")
            //.inputFile("Patient-empty.json")
            .expectResult(0))
        .test("Singular integer has count 1", tc -> tc
            .expression("n1.count()")
            .expectResult(1))
        .test("Plural integer has count 1", tc -> tc
            .expression("an1.count()")
            .expectResult(1))
        .test("Plural integer collection has count 2", tc -> tc
            .expression("an2.count()")
            .expectResult(2))
        .build();
  }
}
