package au.csiro.pathling.fhirpath.dsl;

import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;

@Tag("UnitTest")
public class FilteringAndProjectionFunctionsDslTest extends FhirPathDslTestBase {

  @FhirPathTest
  public Stream<DynamicTest> testWhereFunction() {
    return builder()
        .withSubject(sb -> sb
            .integer("n1", 1)
            .integerArray("numbers", 1, 2, 3, 4, 5)
            .string("s1", "test")
            .stringArray("strings", "apple", "banana", "cherry")
            .complex("patient", p -> p
                .string("id", "patient-1")
                .string("active", "true")
                .complexArray("telecom",
                    t -> t.string("system", "phone").string("value", "555-1234").string("use", "home"),
                    t -> t.string("system", "email").string("value", "test@example.com").string("use", "work"),
                    t -> t.string("system", "phone").string("value", "555-5678").string("use", "mobile")
                )
                .complexArray("name",
                    n -> n.string("use", "official").stringArray("given", "John").string("family", "Smith"),
                    n -> n.string("use", "nickname").stringArray("given", "Johnny")
                )
                .complexArray("address",
                    a -> a.string("use", "home").string("city", "Sydney").string("state", "NSW"),
                    a -> a.string("use", "work").string("city", "Melbourne").string("state", "VIC")
                )
            )
            .complex("observation", o -> o
                .string("id", "obs-1")
                .string("status", "final")
                .complexArray("component",
                    c -> c.string("code", "8480-6").decimal("value", 120.0),
                    c -> c.string("code", "8462-4").decimal("value", 80.0)
                )
            )
        )
        .group("where() with primitive types")
        .testEquals(2, "numbers.where($this > 3).count()", "Filter numbers greater than 3")
        .testEquals(3, "numbers.where($this < 4).count()", "Filter numbers less than 4")
        .testEquals(1, "numbers.where($this = 3).count()", "Filter numbers equal to 3")
        .testEquals(4, "numbers.where($this != 3).count()", "Filter numbers not equal to 3")
        .testEquals(0, "numbers.where($this > 10).count()", "Filter with no matches returns empty collection")
        .testEquals(2, "strings.where($this.startsWith('a') or $this.startsWith('c')).count()", 
            "Filter strings with complex condition")
        
        .group("where() with complex types")
        .testEquals(2, "patient.telecom.where(system = 'phone').count()", 
            "Filter telecom by system")
        .testEquals(1, "patient.telecom.where(system = 'phone' and use = 'mobile').count()", 
            "Filter telecom by multiple criteria")
        .testEquals(1, "patient.name.where(use = 'official').count()", 
            "Filter name by use")
        .testEquals(1, "patient.address.where(city = 'Sydney').count()", 
            "Filter address by city")
        .testEquals(1, "observation.component.where(code = '8480-6').count()", 
            "Filter component by code")
        .testEquals(1, "observation.component.where(value > 100).count()", 
            "Filter component by value")
        
        .group("where() with empty collections")
        .testEquals(0, "{}.where($this > 3).count()", "where() on empty collection returns empty")
        .testEquals(0, "patient.identifier.where(use = 'official').count()", 
            "where() on non-existent path returns empty")
        
        .group("where() with nested paths")
        .testEquals(1, "patient.name.where(given = 'John').count()", 
            "Filter by nested element")
        .testEquals(1, "patient.name.where(family.exists()).count()", 
            "Filter by existence of nested element")
        
        .group("where() with boolean logic")
        .testEquals(2, "numbers.where($this > 2 and $this < 5).count()", 
            "Filter with AND condition")
        .testEquals(3, "numbers.where($this < 2 or $this > 4).count()", 
            "Filter with OR condition")
        .testEquals(4, "numbers.where(not($this = 3)).count()", 
            "Filter with NOT condition")
        .testEquals(3, "numbers.where($this < 2 or ($this > 3 and $this <= 5)).count()", 
            "Filter with complex boolean logic")
        
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testSelectFunction() {
    return builder()
        .withSubject(sb -> sb
            .integer("n1", 1)
            .integerArray("numbers", 1, 2, 3, 4, 5)
            .string("s1", "test")
            .stringArray("strings", "apple", "banana", "cherry")
            .complex("patient", p -> p
                .string("id", "patient-1")
                .string("active", "true")
                .complexArray("telecom",
                    t -> t.string("system", "phone").string("value", "555-1234").string("use", "home"),
                    t -> t.string("system", "email").string("value", "test@example.com").string("use", "work"),
                    t -> t.string("system", "phone").string("value", "555-5678").string("use", "mobile")
                )
                .complexArray("name",
                    n -> n.string("use", "official").stringArray("given", "John").string("family", "Smith"),
                    n -> n.string("use", "nickname").stringArray("given", "Johnny")
                )
                .complexArray("address",
                    a -> a.string("use", "home").string("city", "Sydney").string("state", "NSW"),
                    a -> a.string("use", "work").string("city", "Melbourne").string("state", "VIC")
                )
            )
            .complex("observation", o -> o
                .string("id", "obs-1")
                .string("status", "final")
                .complexArray("component",
                    c -> c.string("code", "8480-6").decimal("value", 120.0),
                    c -> c.string("code", "8462-4").decimal("value", 80.0)
                )
            )
        )
        .group("select() with primitive types")
        .testEquals(5, "numbers.select($this * 2).count()", 
            "Select with transformation preserves count")
        .testTrue("numbers.select($this * 2).contains(10)", 
            "Select with numeric transformation")
        .testTrue("strings.select($this.upper()).contains('APPLE')", 
            "Select with string transformation")
        
        .group("select() with complex types")
        .testEquals(3, "patient.telecom.select(system).count()", 
            "Select single property from complex type")
        .testTrue("patient.telecom.select(system).contains('phone')", 
            "Select extracts correct values")
        .testEquals(3, "patient.telecom.select(system + ': ' + value).count()", 
            "Select with string concatenation")
        .testTrue("patient.telecom.select(system + ': ' + value).contains('phone: 555-1234')", 
            "Select with concatenation produces correct values")
        
        .group("select() with empty collections")
        .testEquals(0, "{}.select($this * 2).count()", 
            "select() on empty collection returns empty")
        .testEquals(0, "patient.identifier.select(value).count()", 
            "select() on non-existent path returns empty")
        
        .group("select() with nested paths")
        .testEquals(2, "patient.name.select(given).count()", 
            "Select flattens collections")
        .testTrue("patient.name.select(given).contains('John')", 
            "Select extracts nested values correctly")
        .testTrue("patient.name.select(given).contains('Johnny')", 
            "Select extracts all nested values")
        
        .group("select() with conditional logic")
        .testEquals(5, "numbers.select(iif($this > 3, $this * 2, $this)).count()", 
            "Select with conditional transformation preserves count")
        .testTrue("numbers.select(iif($this > 3, $this * 2, $this)).contains(8)", 
            "Select with conditional transformation for values > 3")
        .testTrue("numbers.select(iif($this > 3, $this * 2, $this)).contains(2)", 
            "Select with conditional transformation for values <= 3")
        
        .group("select() with complex expressions")
        .testEquals(2, "observation.component.select(code + ': ' + value.toString()).count()", 
            "Select with complex expression")
        .testTrue("observation.component.select(code + ': ' + value.toString()).contains('8480-6: 120.0')", 
            "Select with complex expression produces correct values")
        
        .group("Chaining select() with other functions")
        .testEquals(2, "numbers.where($this > 3).select($this * 2).count()", 
            "Chain where() and select()")
        .testTrue("numbers.where($this > 3).select($this * 2).contains(8)", 
            "Chain where() and select() produces correct values")
        .testEquals("phone: 555-1234,email: test@example.com,phone: 555-5678", 
            "patient.telecom.select(system + ': ' + value).join(',')", 
            "Chain select() and join()")
        
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testOfTypeFunction() {
    return builder()
        .withSubject(sb -> sb
            .complex("mixedCollection", m -> m
                .string("stringValue", "test")
                .integer("intValue", 123)
                .decimal("decimalValue", 45.67)
                .bool("boolValue", true)
                .dateTime("dateTimeValue", "2023-01-15T14:30:00Z")
                .date("dateValue", "2023-01-15")
                .time("timeValue", "14:30:00")
                .quantity("quantityValue", "120 'mm[Hg]'")
                .coding("codingValue", "http://loinc.org|8480-6||'Systolic blood pressure'")
            )
            .complex("patient", p -> p
                .string("id", "patient-1")
                .complexArray("name",
                    n -> n.string("use", "official").stringArray("given", "John").string("family", "Smith")
                )
                .complexArray("telecom",
                    t -> t.string("system", "phone").string("value", "555-1234")
                )
            )
        )
        .group("Basic ofType() functionality")
        .testTrue("mixedCollection.stringValue.ofType(String).exists()", 
            "ofType(String) matches string values")
        .testTrue("mixedCollection.intValue.ofType(Integer).exists()", 
            "ofType(Integer) matches integer values")
        .testTrue("mixedCollection.decimalValue.ofType(Decimal).exists()", 
            "ofType(Decimal) matches decimal values")
        .testTrue("mixedCollection.boolValue.ofType(Boolean).exists()", 
            "ofType(Boolean) matches boolean values")
        .testTrue("mixedCollection.dateTimeValue.ofType(DateTime).exists()", 
            "ofType(DateTime) matches datetime values")
        .testTrue("mixedCollection.dateValue.ofType(Date).exists()", 
            "ofType(Date) matches date values")
        .testTrue("mixedCollection.timeValue.ofType(Time).exists()", 
            "ofType(Time) matches time values")
        .testTrue("mixedCollection.quantityValue.ofType(Quantity).exists()", 
            "ofType(Quantity) matches quantity values")
        .testTrue("mixedCollection.codingValue.ofType(Coding).exists()", 
            "ofType(Coding) matches coding values")
        
        .group("ofType() with non-matching types")
        .testFalse("mixedCollection.stringValue.ofType(Integer).exists()", 
            "ofType(Integer) doesn't match string values")
        .testFalse("mixedCollection.intValue.ofType(String).exists()", 
            "ofType(String) doesn't match integer values")
        .testFalse("mixedCollection.dateValue.ofType(DateTime).exists()", 
            "ofType(DateTime) doesn't match date values")
        
        .group("ofType() with empty collections")
        .testFalse("{}.ofType(String).exists()", 
            "ofType() on empty collection returns empty")
        .testFalse("patient.identifier.ofType(String).exists()", 
            "ofType() on non-existent path returns empty")
        
        .group("ofType() with complex types")
        .testTrue("patient.name.ofType(HumanName).exists()", 
            "ofType() with FHIR resource types")
        .testTrue("patient.telecom.ofType(ContactPoint).exists()", 
            "ofType() with FHIR resource types")
        
        .group("Chaining ofType() with other functions")
        .testEquals(1, "mixedCollection.intValue.ofType(Integer).count()", 
            "Chain ofType() and count()")
        .testEquals(2, "mixedCollection.intValue.ofType(Integer).select($this * 2).count()", 
            "Chain ofType(), select() and count()")
        
        .build();
  }
}
