package au.csiro.pathling.fhirpath.dsl;

import static au.csiro.pathling.test.dsl.FhirTestExpectations.*;

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
        .test("Empty literal should return true", 
            expectTrue("{}.empty()"))
        .test("Empty collection should return true", 
            expectTrue("nothing.empty()"))
        .test("Computed empty collection is empty", 
            expectTrue("n1.where($this=0).empty()"))
        .test("Resource singular empty collection is empty", 
            expectTrue("gender.empty()"))
        .test("Resource plural empty collection is empty", 
            expectTrue("name.given.empty()"))
        .test("Singular integer is not empty", 
            expectFalse("n1.empty()"))
        .test("Plural integer is not empty", 
            expectFalse("an1.empty()"))
        .test("Plural integer collection is not empty", 
            expectFalse("an2.empty()"))
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
        .test("Empty literal should return 0", 
            expectEquals(0, "{}.count()"))
        .test("Empty collection has count 0", 
            expectEquals(0, "nothing.count()"))
        .test("Computed empty collection has count 0", 
            expectEquals(0, "n1.where($this=0).count()"))
        .test("Resource singular empty collection has count 0", 
            expectEquals(0, "gender.count()"))
        .test("Resource plural empty collection has count 0", 
            expectEquals(0, "name.given.count()"))
        .test("Singular integer has count 1", 
            expectEquals(1, "n1.count()"))
        .test("Plural integer has count 1", 
            expectEquals(1, "an1.count()"))
        .test("Plural integer collection has count 2", 
            expectEquals(2, "an2.count()"))
        .build();
  }
}
