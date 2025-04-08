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
        )
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
  public Stream<DynamicTest> testExistsFunction() {
    return builder()
        .withSubject(sb -> sb
            .integer("n1", 1)
            .integerArray("an1", 1)
            .integerArray("an2", 1, 2)
            .string("s1", "a")
            .stringArray("sn1", "a")
            .stringArray("sn2", "a", "b")
            .complex("patient", p -> p
                .stringArray("name", "John", "Doe")
                .complexArray("telecom", 
                    t -> t.string("system", "phone").string("value", "555-1234"),
                    t -> t.string("system", "email").string("value", "john@example.com")
                )
            )
        )
        .group("exists() without criteria")
        .testFalse("{}.exists()", "Empty literal does not exist")
        .testFalse("nothing.exists()", "Empty collection does not exist")
        .testFalse("n1.where($this=0).exists()", "Computed empty collection does not exist")
        .testFalse("gender.exists()", "Resource singular empty collection does not exist")
        .testFalse("name.given.exists()", "Resource plural empty collection does not exist")
        .testTrue("n1.exists()", "Singular integer exists")
        .testTrue("an1.exists()", "Plural integer exists")
        .testTrue("an2.exists()", "Plural integer collection exists")
        .testTrue("s1.exists()", "Singular string exists")
        .testTrue("sn1.exists()", "Plural string exists")
        .testTrue("sn2.exists()", "Plural string collection exists")
        
        .group("exists() with criteria")
        .testTrue("an2.exists($this = 1)", "Exists with matching criteria")
        .testFalse("an2.exists($this = 3)", "Does not exist with non-matching criteria")
        .testTrue("sn2.exists($this = 'a')", "String exists with matching criteria")
        .testFalse("sn2.exists($this = 'c')", "String does not exist with non-matching criteria")
        .testTrue("patient.telecom.exists(system = 'phone')", "Complex exists with matching criteria")
        .testFalse("patient.telecom.exists(system = 'fax')", "Complex does not exist with non-matching criteria")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testCountFunction() {
    return builder()
        .withSubject(sb -> sb
            .integer("n1", 1)
            .integerArray("an1", 1)
            .integerArray("an2", 1, 2)
        )
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

  @FhirPathTest
  public Stream<DynamicTest> testAllTrueFunction() {
    return builder()
        .withSubject(sb -> sb
            .bool("b1", true)
            .boolArray("allTrue", true, true, true)
            .boolArray("someFalse", true, false, true)
            .boolArray("allFalse", false, false, false)
        )
        .group("allTrue() function")
        .testTrue("{}.allTrue()", "Empty collection returns true")
        .testTrue("b1.allTrue()", "Single true returns true")
        .testTrue("allTrue.allTrue()", "All true collection returns true")
        .testFalse("someFalse.allTrue()", "Collection with some false returns false")
        .testFalse("allFalse.allTrue()", "All false collection returns false")
        .testTrue("b1.where($this = false).allTrue()", "Empty computed collection returns true")
        .testTrue("(true and true and true).allTrue()", "Computed all true returns true")
        .testFalse("(true and false and true).allTrue()", "Computed with false returns false")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testAllFalseFunction() {
    return builder()
        .withSubject(sb -> sb
            .bool("b1", true)
            .bool("b2", false)
            .boolArray("allTrue", true, true, true)
            .boolArray("someFalse", true, false, true)
            .boolArray("allFalse", false, false, false)
        )
        .group("allFalse() function")
        .testTrue("{}.allFalse()", "Empty collection returns true")
        .testFalse("b1.allFalse()", "Single true returns false")
        .testTrue("b2.allFalse()", "Single false returns true")
        .testFalse("allTrue.allFalse()", "All true collection returns false")
        .testFalse("someFalse.allFalse()", "Collection with some false returns false")
        .testTrue("allFalse.allFalse()", "All false collection returns true")
        .testTrue("b1.where($this = false).allFalse()", "Empty computed collection returns true")
        .testFalse("(false or true or false).allFalse()", "Computed with true returns false")
        .testTrue("(false and false and false).allFalse()", "Computed all false returns true")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testAnyTrueFunction() {
    return builder()
        .withSubject(sb -> sb
            .bool("b1", true)
            .bool("b2", false)
            .boolArray("allTrue", true, true, true)
            .boolArray("someFalse", true, false, true)
            .boolArray("allFalse", false, false, false)
        )
        .group("anyTrue() function")
        .testFalse("{}.anyTrue()", "Empty collection returns false")
        .testTrue("b1.anyTrue()", "Single true returns true")
        .testFalse("b2.anyTrue()", "Single false returns false")
        .testTrue("allTrue.anyTrue()", "All true collection returns true")
        .testTrue("someFalse.anyTrue()", "Collection with some false returns true")
        .testFalse("allFalse.anyTrue()", "All false collection returns false")
        .testFalse("b1.where($this = false).anyTrue()", "Empty computed collection returns false")
        .testTrue("(true or false or false).anyTrue()", "Computed with true returns true")
        .testFalse("(false and false and false).anyTrue()", "Computed all false returns false")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testAnyFalseFunction() {
    return builder()
        .withSubject(sb -> sb
            .bool("b1", true)
            .bool("b2", false)
            .boolArray("allTrue", true, true, true)
            .boolArray("someFalse", true, false, true)
            .boolArray("allFalse", false, false, false)
        )
        .group("anyFalse() function")
        .testFalse("{}.anyFalse()", "Empty collection returns false")
        .testFalse("b1.anyFalse()", "Single true returns false")
        .testTrue("b2.anyFalse()", "Single false returns true")
        .testFalse("allTrue.anyFalse()", "All true collection returns false")
        .testTrue("someFalse.anyFalse()", "Collection with some false returns true")
        .testTrue("allFalse.anyFalse()", "All false collection returns true")
        .testFalse("b1.where($this = false).anyFalse()", "Empty computed collection returns false")
        .testTrue("(false or true or false).anyFalse()", "Computed with false returns true")
        .testFalse("(true and true and true).anyFalse()", "Computed all true returns false")
        .build();
  }
}
