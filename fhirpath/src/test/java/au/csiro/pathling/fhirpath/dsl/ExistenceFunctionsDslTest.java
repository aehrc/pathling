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
                    xy -> xy.integer("x", 1).integer("y", 2),
                    xy -> xy.integer("x", 3)
                )
            ))
        .testTrue("{}.empty()", "Empty literal should return true")
        .testTrue("nothing.empty()", "Empty collection should return true")
        .testTrue("n1.where($this=0).empty()", "Computed empty collection is empty")
        .testTrue("gender.empty()", "Resource singular empty collection is empty")
        .testTrue("name.given.empty()", "Resource plural empty collection is empty")
        .testFalse("n1.empty()", "Singular integer is not empty")
        .testFalse("an1.empty()", "Plural integer is not empty")
        .testFalse("an2.empty()", "Plural integer collection is not empty")
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
                    xy -> xy.integer("x", 1).integer("y", 2),
                    xy -> xy.integer("x", 3)
                )
            ))
        .testEquals(0, "{}.count()", "Empty literal should return 0")
        .testEquals(0, "nothing.count()", "Empty collection has count 0")
        .testEquals(0, "n1.where($this=0).count()", "Computed empty collection has count 0")
        .testEquals(0, "gender.count()", "Resource singular empty collection has count 0")
        .testEquals(0, "name.given.count()", "Resource plural empty collection has count 0")
        .testEquals(1, "n1.count()", "Singular integer has count 1")
        .testEquals(1, "an1.count()", "Plural integer has count 1")
        .testEquals(2, "an2.count()", "Plural integer collection has count 2")
        .build();
  }
}
