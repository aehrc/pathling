package au.csiro.pathling.fhirpath.dsl;

import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;

@Tag("UnitTest")
public class MembershipOperatorsDslTest extends FhirPathDslTestBase {

  @FhirPathTest
  public Stream<DynamicTest> testQuantityMembership() {
    return builder()
        .withSubject(sb -> sb
            .quantity("noQuantity", null)
            .quantity("oneQuantity", "80 '/min'")
            .quantityArray("manyQuantity",
                "120 'mm[Hg]'",
                "80 'mm[Hg]'",
                "72 '/min'")
        )
        .group("Quantity type")
        .testTrue("80 'mm[Hg]' in manyQuantity", "In for existing quantity in many")
        .testTrue("manyQuantity contains 72 '/min'", "Contains for existing quantity in many")
        .testFalse("72 'mm[Hg]' in manyQuantity", "In for non-existing quantity in many")
        .testFalse("manyQuantity contains 80 '/min'", "Contains non-existing quantity in many")
        .testEmpty("{} in manyQuantity", "In for empty literal in many Quantity")
        .testEmpty("manyQuantity contains {}", "Contains empty literal in many Quantity")
        .testTrue("80 '/min' in oneQuantity", "In for existing quantity in one")
        .testTrue("oneQuantity contains 80 '/min'", "Contains for existing quantity in one")
        .testFalse("80 'mm[Hg]' in oneQuantity", "In for non-existing quantity in one")
        .testFalse("oneQuantity contains 81 '/min'", "Contains non-existing quantity in one")
        .testEmpty("{} in oneQuantity", "In for empty literal in one Quantity")
        .testEmpty("oneQuantity contains {}", "Contains empty literal in one Quantity")
        .testEmpty("noQuantity in oneQuantity", "In for empty Quantity in one")
        .testEmpty("oneQuantity contains noQuantity", "Contains empty Quantity in one")
        .testEmpty("noQuantity in manyQuantity", "In for empty Quantity in many")
        .testEmpty("manyQuantity contains noQuantity", "Contains empty Quantity in many")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testCodingMembership() {
    return builder()
        .withSubject(sb -> sb
            .coding("noCoding", null)
            .coding("oneCoding", "http://loinc.org|8867-4||'Heart rate'")
            .codingArray("manyCoding",
                "http://loinc.org|8480-6||'Systolic blood pressure'",
                "http://loinc.org|8867-4||'Heart rate'",
                "http://loinc.org|8462-4||'Diastolic blood pressure'")
        )
        .group("Coding type")
        .testTrue("http://loinc.org|8480-6||'Systolic blood pressure' in manyCoding",
            "In for existing coding in many")
        .testTrue("manyCoding contains http://loinc.org|8867-4||'Heart rate'",
            "Contains for existing coding in many")
        .testFalse("http://loinc.org|9999-9||'Non-existent code' in manyCoding",
            "In for non-existing coding in many")
        .testFalse("manyCoding contains http://loinc.org|9999-9||'Non-existent code'",
            "Contains non-existing coding in many")
        .testEmpty("{} in manyCoding", "In for empty literal in many Coding")
        .testEmpty("manyCoding contains {}", "Contains empty literal in many Coding")
        .testTrue("http://loinc.org|8867-4||'Heart rate' in oneCoding",
            "In for existing coding in one")
        .testTrue("oneCoding contains http://loinc.org|8867-4||'Heart rate'",
            "Contains for existing coding in one")
        .testFalse("http://loinc.org|9999-9||'Non-existent code' in oneCoding",
            "In for non-existing coding in one")
        .testFalse("oneCoding contains http://loinc.org|9999-9||'Non-existent code'",
            "Contains non-existing coding in one")
        .testEmpty("{} in oneCoding", "In for empty literal in one Coding")
        .testEmpty("oneCoding contains {}", "Contains empty literal in one Coding")
        .testEmpty("noCoding in oneCoding", "In for empty Coding in one")
        .testEmpty("oneCoding contains noCoding", "Contains empty Coding in one")
        .testEmpty("noCoding in manyCoding", "In for empty Coding in many")
        .testEmpty("manyCoding contains noCoding", "Contains empty Coding in many")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testIntegerMembership() {
    return builder()
        .withSubject(sb -> sb
            .integer("oneInt", 1)
            .integerArray("manyInt", 1, 2, 3)
        )
        .group("Integer type")
        .testTrue("2 in manyInt", "In for existing Integer in many")
        .testTrue("manyInt contains 2", "Contains for existing Integer in many")
        .testFalse("5 in manyInt", "In for non-existing Integer in many")
        .testFalse("manyInt contains 5", "Contains non-existing Integer in many")
        .testEmpty("{} in manyInt", "In for empty Integer in many")
        .testEmpty("manyInt contains {}", "Contains empty Integer in many")
        .testTrue("1 in oneInt", "In for existing Integer in one")
        .testTrue("oneInt contains 1", "Contains for existing Integer in one")
        .testFalse("2 in oneInt", "In for non-existing Integer in one")
        .testFalse("oneInt contains 2", "Contains non-existing Integer in one")
        .testEmpty("{} in oneInt", "In for empty Integer in one")
        .testEmpty("oneInt contains {}", "Contains empty Integer in one")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testStringMembership() {
    return builder()
        .withSubject(sb -> sb
            .string("oneString", "test")
            .stringArray("manyString", "test1", "test2", "test3")
        )
        .group("String type")
        .testTrue("'test2' in manyString", "In for existing String in many")
        .testTrue("manyString contains 'test2'", "Contains for existing String in many")
        .testFalse("'test4' in manyString", "In for non-existing String in many")
        .testFalse("manyString contains 'test4'", "Contains non-existing String in many")
        .testEmpty("{} in manyString", "In for empty String in many")
        .testEmpty("manyString contains {}", "Contains empty String in many")
        .testTrue("'test' in oneString", "In for existing String in one")
        .testTrue("oneString contains 'test'", "Contains for existing String in one")
        .testFalse("'test2' in oneString", "In for non-existing String in one")
        .testFalse("oneString contains 'test2'", "Contains non-existing String in one")
        .testEmpty("{} in oneString", "In for empty String in one")
        .testEmpty("oneString contains {}", "Contains empty String in one")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testBooleanMembership() {
    return builder()
        .withSubject(sb -> sb
            .bool("oneBoolean", true)
            .boolArray("manyBoolean", true, false, true)
        )
        .group("Boolean type")
        .testTrue("true in manyBoolean", "In for existing Boolean in many")
        .testTrue("manyBoolean contains false", "Contains for existing Boolean in many")
        .testEmpty("{} in manyBoolean", "In for non-existing Boolean in many")
        .testEmpty("manyBoolean contains {}", "Contains non-existing Boolean in many")
        .testTrue("true in oneBoolean", "In for existing Boolean in one")
        .testTrue("oneBoolean contains true", "Contains for existing Boolean in one")
        .testFalse("false in oneBoolean", "In for non-existing Boolean in one")
        .testFalse("oneBoolean contains false", "Contains non-existing Boolean in one")
        .testEmpty("{} in oneBoolean", "In for empty Boolean in one")
        .testEmpty("oneBoolean contains {}", "Contains empty Boolean in one")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testDecimalMembership() {
    return builder()
        .withSubject(sb -> sb
            .decimal("oneDecimal", 1.5)
            .decimalArray("manyDecimal", 1.5, 2.5, 3.5)
        )
        .group("Decimal type")
        .testTrue("2.5 in manyDecimal", "In for existing Decimal in many")
        .testTrue("manyDecimal contains 2.5", "Contains for existing Decimal in many")
        .testFalse("4.5 in manyDecimal", "In for non-existing Decimal in many")
        .testFalse("manyDecimal contains 4.5", "Contains non-existing Decimal in many")
        .testEmpty("{} in manyDecimal", "In for empty Decimal in many")
        .testEmpty("manyDecimal contains {}", "Contains empty Decimal in many")
        .testTrue("1.5 in oneDecimal", "In for existing Decimal in one")
        .testTrue("oneDecimal contains 1.5", "Contains for existing Decimal in one")
        .testFalse("2.5 in oneDecimal", "In for non-existing Decimal in one")
        .testFalse("oneDecimal contains 2.5", "Contains non-existing Decimal in one")
        .testEmpty("{} in oneDecimal", "In for empty Decimal in one")
        .testEmpty("oneDecimal contains {}", "Contains empty Decimal in one")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testCrossTypeMembership() {
    return builder()
        .withSubject(sb -> sb
            .string("oneString", "test")
            .boolArray("manyBoolean", true, false, true)
            .codingArray("manyCoding",
                "http://loinc.org|8480-6||'Systolic blood pressure'",
                "http://loinc.org|8867-4||'Heart rate'",
                "http://loinc.org|8462-4||'Diastolic blood pressure'")
        )
        .group("Cross type membership")
        .testError("10 in oneString", "Integer in String one")
        .testError("'true' in manyBoolean", "String in boolean one")
        .testError("'http://loinc.org|8480-6||\\'Systolic blood pressure\\'' in manyCoding",
            "String in Coding many")
        .build();
  }
}
