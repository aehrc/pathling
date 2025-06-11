package au.csiro.pathling.fhirpath.dsl;

import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import au.csiro.pathling.test.yaml.FhirTypedLiteral;
import java.util.List;
import java.util.stream.Stream;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;

/**
 * Tests for FHIRPath filtering and projection functions required by SQL on FHIR sharable view
 * profile: - where() function - ofType() function (for non-resource types)
 */
@Tag("UnitTest")
public class FilteringAndProjectionFunctionsDslTest extends FhirPathDslTestBase {

  @FhirPathTest
  public Stream<DynamicTest> testWhere() {
    return builder()
        .withSubject(sb -> sb
            // Arrays
            .stringArray("stringArray", "one", "two", "three")
            .integerArray("integerArray", 1, 2, 3, 4, 5)
            // Complex types
            .elementArray("people",
                person1 -> person1
                    .string("name", "Alice")
                    .integer("age", 25)
                    .bool("active", true),
                person2 -> person2
                    .string("name", "Bob")
                    .integer("age", 40)
                    .bool("active", false),
                person3 -> person3
                    .string("name", "Charlie")
                    .integer("age", 35)
                    .bool("active", true))
        )
        .group("where() function")
        // String where() tests
        .testEquals("two", "stringArray.where($this = 'two')",
            "where() filters string array by equality")
        .testEmpty("stringArray.where($this = 'four')",
            "where() returns empty when no strings match")

        // Integer where() tests
        .testEquals(3, "integerArray.where($this = 3)",
            "where() filters integer array by equality")
        .testEquals(4, "integerArray.where($this > 3).first()",
            "where() filters integer array by comparison and chains with first()")
        .testEquals(List.of(1, 2), "integerArray.where($this < 3)",
            "where() filters integer array by comparison")

        // Complex type where() tests
        .testTrue("people.where(name = 'Alice').exists()",
            "where() filters complex type array by property equality")
        .testEquals(List.of("Alice", "Charlie"), "people.where(active = true).name",
            "where() filters complex type array by boolean property")
        .testEquals(List.of("Bob", "Charlie"), "people.where(age > 30).name",
            "where() filters complex type array by numeric comparison")
        .testTrue("people.where(age > 30 and active = true).first().name = 'Charlie'",
            "where() filters complex type array by multiple conditions")
        .testEquals(List.of("Alice", "Bob"), "people.where(age <= 25 or age >= 40).name",
            "where() filters complex type array by OR condition")
        .testEmpty("people.where(name = 'David')",
            "where() returns empty when no complex types match")

        // Chained where() tests
        .testEquals("Charlie", "people.where(active = true).where(age > 30).name",
            "where() can be chained with another where()")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testOfTypeNonResource() {
    return builder()
        .withSubject(sb -> sb
            // Primitive types for ofType testing
            .string("stringValue", "test")
            .integer("integerValue", 42)
            .decimal("decimalValue", 3.14)
            .bool("booleanValue", true)
            .stringEmpty("emptyString")
            .coding("codingValue", "http://example.org/codesystem|code2|display1")
            .element("quantityValue",
                qt -> qt.fhirType(FHIRDefinedType.QUANTITY)
                    .decimal("value", 11.5)
                    .string("unit", "mg"))
            // Array of mixed primitive types
            .elementArray("heteroattr",
                // only the first element needs to be a full choice type
                val1 -> val1.choice("value")
                    .string("valueString", "string")
                    .integerEmpty("valueInteger")
                    .boolEmpty("valueBoolean"),
                val2 -> val2.integer("valueInteger", 1),
                val3 -> val3.integer("valueInteger", 2)
            )
            .element("heteroComplex",
                val1 -> val1.choice("test")
                    .coding("testCoding",
                        "http://example.org/codesystem|code1|display1")
                    .stringEmpty("testString")
            )
            .element("heteroQuantity",
                val1 -> val1.choice("value")
                    .element("valueQuantity",
                        qt -> qt.fhirType(FHIRDefinedType.QUANTITY)
                            .decimal("value", 10.5)
                            .string("unit", "mg")
                    )
                    .stringEmpty("valueString")
            )
        )
        .group("ofType() function with non-resource types")
        // ofType() tests with primitive types
        .testEquals("test", "stringValue.ofType(System.String)",
            "ofType() returns string value when filtering for System.String type")
        .testEquals(42, "integerValue.ofType(Integer)",
            "ofType() returns integer value when filtering for (Systrem).Integer type")
        .testEquals(3.14, "decimalValue.ofType(decimal)",
            "ofType() returns decimal value when filtering for (FHIR).decimal type")
        .testEquals(true, "booleanValue.ofType(FHIR.boolean)",
            "ofType() returns boolean value when filtering for FHIR.boolean type")
        .testEquals("mg", "quantityValue.ofType(Quantity).unit",
            "ofType() returns quantity value when filtering for (FHIR).Quantity type")
        .testTrue("codingValue.ofType(FHIR.Coding).exists()",
            "ofType() returns coding value when for FHIR.Coding type")
        .testTrue("codingValue.ofType(System.Coding).exists()",
            "ofType() returns coding value when for System.Coding type")

        // ofType() with empty collections
        .testEmpty("emptyString.ofType(string)",
            "ofType() returns empty when applied to empty string")
        .testEmpty("{}.ofType(string)",
            "ofType() returns empty when applied to empty collection")

        // ofType() with heterogeneous collection
        .testEquals("string", "heteroattr.value.ofType(string)",
            "ofType() filters heterogeneous collection for String type")
        .testEquals(List.of(1, 2), "heteroattr.value.ofType(integer)",
            "ofType() filters heterogeneous collection for Integer type")
        .testEmpty("heteroattr.value.ofType(boolean)",
            "ofType() filters heterogeneous collection for Boolean type")
        .testEmpty("heteroattr.value.ofType(decimal)",
            "ofType() filters heterogeneous collection for undefined choice type (Decimal)")
        // ofType() with complex types
        .testEquals(FhirTypedLiteral.toCoding("http://example.org/codesystem|code1|display1"),
            "heteroComplex.test.ofType(Coding)",
            "ofType() returns Coding value when filtering for Coding type")
        .testEmpty("heteroComplex.test.ofType(string)",
            "ofType() returns empty when filtering for Quantity type in complex type")
        // traversing into complex types
        .testEquals("mg", "heteroQuantity.value.ofType(Quantity).unit",
            "can traverse into fields of complex type from ofType(Quantity)")
        // DISABLED: ofType() with Coding type BUG
        // .testEquals("code1",
        //     "heteroComplex.test.ofType(Coding).code",
        //     "can traverse into fields of complex literal type from ofType(Coding)")
        // ofType() with non-matching type
        .testEmpty("stringValue.ofType(Integer)",
            "ofType() returns empty when type doesn't match")
        .testEmpty("integerValue.ofType(Boolean)",
            "ofType() returns empty when type doesn't match")
        .testEmpty("codingValue.ofType(Quantity)",
            "ofType() returns empty when type doesn't match")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testOfTypeWithResources() {
    return builder()
        .withSubject(sb -> sb
            // Individual resources
            .string("resourceType", "Patient")
            .string("id", "patient-1")
            .string("name", "John Doe")
        )
        .group("ofType() function with resource types")
        // Basic ofType() tests with resources
        .testTrue("ofType(Patient).exists()",
            "ofType() filters resources by type Patient")
        // Single resource ofType() tests
        .testEquals("John Doe", "ofType(FHIR.Patient).name",
            "ofType() returns the resource when type matches")
        .testEmpty("ofType(FHIR.Observation)",
            "ofType() returns empty when resource type doesn't match")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testOfTypeWithFhirResources() {
    return builder()
        .withResource(new Condition().setId("Condition/1")
        )
        .group("ofType() function with resource types")
        // Basic ofType() tests with resources
        .testTrue("ofType(Condition).exists()",
            "ofType() filters resources by type Patient")
        // Single resource ofType() tests
        .testEquals("1", "ofType(FHIR.Condition).id",
            "ofType() returns the resource when type matches")
        .testEmpty("ofType(Patient)",
            "ofType() returns empty when resource type doesn't match")
        .build();
  }

}
