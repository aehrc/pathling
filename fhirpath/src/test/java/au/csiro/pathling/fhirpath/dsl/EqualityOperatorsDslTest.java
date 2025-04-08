package au.csiro.pathling.fhirpath.dsl;

import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;

@Tag("UnitTest")
public class EqualityOperatorsDslTest extends FhirPathDslTestBase {

  @FhirPathTest
  public Stream<DynamicTest> testStringEquality() {
    return builder()
        .withSubject(sb -> sb
            .string("emptyString", "")
            .string("string1", "test")
            .string("string2", "TEST")
            .string("stringWithSpaces", "  test  ")
            .stringArray("stringArray", "a", "b", "c")
        )
        .group("String equality")
        .testTrue("'' = ''", "Empty string equals empty string")
        .testTrue("'test' = 'test'", "String literal equals itself")
        .testFalse("'test' = 'TEST'", "String equality is case-sensitive")
        .testFalse("'test' = '  test  '", "String equality considers whitespace")
        .testTrue("string1 = 'test'", "String value equals matching literal")
        .testFalse("string1 = string2", "Different string values are not equal")
        .testFalse("string1 = 'other'", "String value not equal to different literal")
        .testEmpty("{} = string1", "Empty collection equality with string returns empty")
        .testEmpty("string1 = {}", "String equality with empty collection returns empty")
        .testError("stringArray = 'a'", "Array equality with single value errors")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testBooleanEquality() {
    return builder()
        .withSubject(sb -> sb
            .bool("trueValue", true)
            .bool("falseValue", false)
            .boolArray("boolArray", true, false)
        )
        .group("Boolean equality")
        .testTrue("true = true", "True equals true")
        .testTrue("false = false", "False equals false")
        .testFalse("true = false", "True not equal to false")
        .testFalse("false = true", "False not equal to true")
        .testTrue("trueValue = true", "Boolean value equals matching literal")
        .testTrue("falseValue = false", "Boolean value equals matching literal")
        .testFalse("trueValue = false", "Boolean value not equal to different literal")
        .testFalse("falseValue = true", "Boolean value not equal to different literal")
        .testEmpty("{} = trueValue", "Empty collection equality with boolean returns empty")
        .testEmpty("falseValue = {}", "Boolean equality with empty collection returns empty")
        .testError("boolArray = true", "Array equality with single value errors")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testIntegerEquality() {
    return builder()
        .withSubject(sb -> sb
            .integer("int0", 0)
            .integer("int1", 1)
            .integer("intNeg", -5)
            .integerArray("intArray", 1, 2, 3)
        )
        .group("Integer equality")
        .testTrue("0 = 0", "Zero equals zero")
        .testTrue("1 = 1", "Integer equals itself")
        .testTrue("-5 = -5", "Negative integer equals itself")
        .testFalse("1 = 2", "Different integers are not equal")
        .testFalse("1 = -1", "Positive not equal to negative")
        .testTrue("int0 = 0", "Integer value equals matching literal")
        .testTrue("int1 = 1", "Integer value equals matching literal")
        .testTrue("intNeg = -5", "Negative integer value equals matching literal")
        .testFalse("int1 = 2", "Integer value not equal to different literal")
        .testEmpty("{} = int1", "Empty collection equality with integer returns empty")
        .testEmpty("int1 = {}", "Integer equality with empty collection returns empty")
        .testError("intArray = 1", "Array equality with single value errors")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testDecimalEquality() {
    return builder()
        .withSubject(sb -> sb
            .decimal("dec0", 0.0)
            .decimal("dec1", 1.0)
            .decimal("decPi", 3.14159)
            .decimal("decNeg", -2.5)
            .decimal("decTrailing", 1.500)
            .decimalArray("decArray", 1.1, 2.2, 3.3)
        )
        .group("Decimal equality")
        .testTrue("0.0 = 0.0", "Zero decimal equals itself")
        .testTrue("1.0 = 1.0", "Decimal equals itself")
        .testTrue("3.14159 = 3.14159", "Decimal with many digits equals itself")
        .testTrue("-2.5 = -2.5", "Negative decimal equals itself")
        .testTrue("1.5 = 1.50", "Decimals with different trailing zeros are equal")
        .testFalse("1.0 = 1.1", "Different decimals are not equal")
        .testFalse("1.0 = -1.0", "Positive not equal to negative")
        .testTrue("dec0 = 0.0", "Decimal value equals matching literal")
        .testTrue("dec1 = 1.0", "Decimal value equals matching literal")
        .testTrue("decPi = 3.14159", "Decimal value with many digits equals matching literal")
        .testTrue("decTrailing = 1.5", "Decimal value equals literal without trailing zeros")
        .testFalse("dec1 = 2.0", "Decimal value not equal to different literal")
        .testEmpty("{} = dec1", "Empty collection equality with decimal returns empty")
        .testEmpty("dec1 = {}", "Decimal equality with empty collection returns empty")
        .testError("decArray = 1.1", "Array equality with single value errors")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testQuantityEquality() {
    return builder()
        .withSubject(sb -> sb
            .quantity("qtyLength", "100 'cm'")
            .quantity("qtyLengthM", "1 'm'")
            .quantity("qtyWeight", "70 'kg'")
            .quantity("qtyTemp", "37 'Cel'")
            .quantity("qtyYear", "1 year")
            .quantity("qtyMonth", "12 months")
            .quantityArray("qtyArray", "120 'mm[Hg]'", "80 'mm[Hg]'")
        )
        .group("Quantity equality")
        .testTrue("100 'cm' = 100 'cm'", "Same quantity equals itself")
        .testTrue("1 'm' = 100 'cm'", "Equivalent quantities in different units are equal")
        .testFalse("70 'kg' = 70 'g'", "Non-equivalent quantities are not equal")
        .testFalse("37 'Cel' = 98.6 '[degF]'", "Different temperature units not automatically converted")
        .testFalse("1 year = 12 months", "Calendar durations not automatically converted")
        .testTrue("qtyLength = 100 'cm'", "Quantity value equals matching literal")
        .testTrue("qtyLength = 1 'm'", "Quantity value equals equivalent literal in different unit")
        .testFalse("qtyLength = qtyWeight", "Different quantity types are not equal")
        .testFalse("qtyYear = qtyMonth", "Different time quantities are not equal")
        .testEmpty("{} = qtyLength", "Empty collection equality with quantity returns empty")
        .testEmpty("qtyLength = {}", "Quantity equality with empty collection returns empty")
        .testError("qtyArray = 120 'mm[Hg]'", "Array equality with single value errors")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testDateTimeEquality() {
    return builder()
        .withSubject(sb -> sb
            .dateTime("dt1", "2023-01-15T14:30:00Z")
            .dateTime("dt2", "2023-01-15T14:30:00+00:00")
            .dateTime("dt3", "2023-01-15T15:30:00+01:00")
            .dateTime("dtPartial1", "2023-01-15")
            .dateTime("dtPartial2", "2023-01")
            .dateTimeArray("dtArray", "2023-01-15T14:30:00Z", "2023-01-16T14:30:00Z")
        )
        .group("DateTime equality")
        .testTrue("@2023-01-15T14:30:00Z = @2023-01-15T14:30:00Z", "Same datetime equals itself")
        .testTrue("@2023-01-15T14:30:00Z = @2023-01-15T14:30:00+00:00", "Equivalent timezone offsets are equal")
        .testTrue("@2023-01-15T14:30:00Z = @2023-01-15T15:30:00+01:00", "Different timezone offsets with same time are equal")
        .testFalse("@2023-01-15T14:30:00Z = @2023-01-15T14:31:00Z", "Different times are not equal")
        .testFalse("@2023-01-15T14:30:00Z = @2023-01-16T14:30:00Z", "Different dates are not equal")
        .testEmpty("@2023-01-15T14:30:00Z = @2023-01-15", "Different precision returns empty")
        .testEmpty("@2023-01-15 = @2023-01", "Different partial precision returns empty")
        .testTrue("dt1 = @2023-01-15T14:30:00Z", "DateTime value equals matching literal")
        .testTrue("dt1 = dt2", "DateTime values with equivalent timezone offsets are equal")
        .testTrue("dt1 = dt3", "DateTime values with different timezone offsets but same time are equal")
        .testEmpty("dt1 = dtPartial1", "DateTime and partial datetime comparison returns empty")
        .testEmpty("{} = dt1", "Empty collection equality with datetime returns empty")
        .testEmpty("dt1 = {}", "DateTime equality with empty collection returns empty")
        .testError("dtArray = @2023-01-15T14:30:00Z", "Array equality with single value errors")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testDateEquality() {
    return builder()
        .withSubject(sb -> sb
            .date("date1", "2023-01-15")
            .date("date2", "2023-01-15")
            .date("date3", "2023-01-16")
            .date("datePartial", "2023-01")
            .dateArray("dateArray", "2023-01-15", "2023-01-16")
        )
        .group("Date equality")
        .testTrue("@2023-01-15 = @2023-01-15", "Same date equals itself")
        .testFalse("@2023-01-15 = @2023-01-16", "Different dates are not equal")
        .testEmpty("@2023-01-15 = @2023-01", "Different precision returns empty")
        .testTrue("date1 = @2023-01-15", "Date value equals matching literal")
        .testTrue("date1 = date2", "Same date values are equal")
        .testFalse("date1 = date3", "Different date values are not equal")
        .testEmpty("date1 = datePartial", "Date and partial date comparison returns empty")
        .testEmpty("{} = date1", "Empty collection equality with date returns empty")
        .testEmpty("date1 = {}", "Date equality with empty collection returns empty")
        .testError("dateArray = @2023-01-15", "Array equality with single value errors")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testTimeEquality() {
    return builder()
        .withSubject(sb -> sb
            .time("time1", "14:30:00")
            .time("time2", "14:30:00")
            .time("time3", "15:45:00")
            .time("timePartial", "14:30")
            .timeArray("timeArray", "14:30:00", "15:45:00")
        )
        .group("Time equality")
        .testTrue("@T14:30:00 = @T14:30:00", "Same time equals itself")
        .testFalse("@T14:30:00 = @T15:45:00", "Different times are not equal")
        .testEmpty("@T14:30:00 = @T14:30", "Different precision returns empty")
        .testTrue("time1 = @T14:30:00", "Time value equals matching literal")
        .testTrue("time1 = time2", "Same time values are equal")
        .testFalse("time1 = time3", "Different time values are not equal")
        .testEmpty("time1 = timePartial", "Time and partial time comparison returns empty")
        .testEmpty("{} = time1", "Empty collection equality with time returns empty")
        .testEmpty("time1 = {}", "Time equality with empty collection returns empty")
        .testError("timeArray = @T14:30:00", "Array equality with single value errors")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testCodingEquality() {
    return builder()
        .withSubject(sb -> sb
            .coding("coding1", "http://loinc.org|8480-6||'Systolic blood pressure'")
            .coding("coding2", "http://loinc.org|8480-6||'Systolic blood pressure'")
            .coding("coding3", "http://loinc.org|8462-4||'Diastolic blood pressure'")
            .coding("codingNoDisplay", "http://loinc.org|8480-6")
            .coding("codingWithVersion", "http://loinc.org|8480-6|2.68|'Systolic blood pressure'")
            .codingArray("codingArray", 
                "http://loinc.org|8480-6||'Systolic blood pressure'", 
                "http://loinc.org|8462-4||'Diastolic blood pressure'")
        )
        .group("Coding equality")
        .testTrue("http://loinc.org|8480-6||'Systolic blood pressure' = http://loinc.org|8480-6||'Systolic blood pressure'", 
            "Same coding equals itself")
        .testFalse("http://loinc.org|8480-6||'Systolic blood pressure' = http://loinc.org|8462-4||'Diastolic blood pressure'", 
            "Different codings are not equal")
        .testFalse("http://loinc.org|8480-6||'Systolic blood pressure' = http://loinc.org|8480-6", 
            "Coding with display not equal to coding without display")
        .testFalse("http://loinc.org|8480-6||'Systolic blood pressure' = http://loinc.org|8480-6|2.68|'Systolic blood pressure'", 
            "Coding without version not equal to coding with version")
        .testTrue("coding1 = http://loinc.org|8480-6||'Systolic blood pressure'", 
            "Coding value equals matching literal")
        .testTrue("coding1 = coding2", "Same coding values are equal")
        .testFalse("coding1 = coding3", "Different coding values are not equal")
        .testFalse("coding1 = codingNoDisplay", "Coding with display not equal to coding without display")
        .testFalse("coding1 = codingWithVersion", "Coding without version not equal to coding with version")
        .testEmpty("{} = coding1", "Empty collection equality with coding returns empty")
        .testEmpty("coding1 = {}", "Coding equality with empty collection returns empty")
        .testError("codingArray = http://loinc.org|8480-6||'Systolic blood pressure'", 
            "Array equality with single value errors")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testCrossTypeEquality() {
    return builder()
        .withSubject(sb -> sb
            .string("stringValue", "123")
            .integer("intValue", 123)
            .decimal("decValue", 123.0)
            .bool("boolValue", true)
            .dateTime("dtValue", "2023-01-15T14:30:00Z")
            .date("dateValue", "2023-01-15")
            .time("timeValue", "14:30:00")
            .quantity("qtyValue", "100 'cm'")
            .coding("codingValue", "http://loinc.org|8480-6||'Systolic blood pressure'")
        )
        .group("Cross-type equality")
        .testFalse("stringValue = intValue", "String and integer with same value are not equal")
        .testFalse("intValue = decValue", "Integer and decimal with same value are not equal")
        .testFalse("'true' = boolValue", "String 'true' and boolean true are not equal")
        .testFalse("dtValue = dateValue", "DateTime and Date are not equal even with same date part")
        .testFalse("'100 cm' = qtyValue", "String representation and quantity are not equal")
        .testFalse("'http://loinc.org|8480-6||''Systolic blood pressure''' = codingValue", 
            "String representation and coding are not equal")
        .build();
  }
}
