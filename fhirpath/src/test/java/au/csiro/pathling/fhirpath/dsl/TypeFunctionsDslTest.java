/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.fhirpath.dsl;

import static au.csiro.pathling.test.yaml.FhirTypedLiteral.toQuantity;

import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import java.math.BigDecimal;
import java.util.stream.Stream;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Observation.ObservationComponentComponent;
import org.hl7.fhir.r4.model.Quantity;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.DynamicTest;

/**
 * Tests for FHIRPath type functions.
 */
public class TypeFunctionsDslTest extends FhirPathDslTestBase {

  @FhirPathTest
  public Stream<DynamicTest> testIsFunction() {
    return builder()
        .withSubject(sb -> sb
            // Primitive types for is() testing
            .string("stringValue", "test")
            .integer("integerValue", 42)
            .decimal("decimalValue", 3.14)
            .bool("booleanValue", true)
            .stringEmpty("emptyString")
            .stringArray("stringArray", "one", "two", "three")
            .coding("codingValue", "http://example.org/codesystem|code2|display1")
            .quantity("quantityValue", "11.5 'mg'")
            // Heterogeneous collection
            .elementArray("heteroattr",
                val1 -> val1.choice("value")
                    .string("valueString", "string")
                    .integerEmpty("valueInteger")
                    .boolEmpty("valueBoolean"),
                val2 -> val2.integer("valueInteger", 1),
                val3 -> val3.integer("valueInteger", 2)
            )
        )
        .group("is() function - primitive type matching")
        // Positive type matches
        .testTrue("stringValue.is(System.String)",
            "is() returns true when value matches System.String type")
        .testTrue("integerValue.is(Integer)",
            "is() returns true when value matches Integer type")
        .testTrue("decimalValue.is(decimal)",
            "is() returns true when value matches decimal type")
        .testTrue("booleanValue.is(FHIR.boolean)",
            "is() returns true when value matches FHIR.boolean type")

        .group("is() function - type mismatches")
        // Negative type matches
        .testFalse("stringValue.is(Integer)",
            "is() returns false when type doesn't match")
        .testFalse("integerValue.is(Boolean)",
            "is() returns false when value is different type")
        .testFalse("codingValue.is(Quantity)",
            "is() returns false when complex type doesn't match")

        .group("is() function - complex types")
        // Complex type matching
        .testTrue("quantityValue.is(Quantity)",
            "is() returns true for Quantity complex type")
        .testTrue("quantityValue.is(FHIR.Quantity)",
            "is() returns true with explicit FHIR namespace")
        .testTrue("codingValue.is(FHIR.Coding)",
            "is() returns true for Coding with FHIR namespace")
        .testTrue("codingValue.is(System.Coding)",
            "is() returns true for Coding with System namespace")
        .testTrue("codingValue.is(Coding)",
            "is() returns true for Coding with unqualified name")

        .group("is() function - edge cases")
        // Empty collections
        .testEmpty("emptyString.is(String)",
            "is() returns empty when applied to empty value")
        .testEmpty("{}.is(String)",
            "is() returns empty when applied to empty collection")

        // Multi-item collections should error
        .testError("stringArray.is(String)",
            "is() throws error on multi-item collection")

        .group("is() function - integration with other functions")
        // Integration with boolean functions
        .testFalse("stringValue.is(String).not()",
            "is() result can be negated with not()")
        .testTrue("stringValue.is(Integer).not()",
            "is() false result can be negated with not()")

        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testAsFunction() {
    return builder()
        .withSubject(sb -> sb
            // Primitive types for as() testing
            .string("stringValue", "test")
            .integer("integerValue", 42)
            .decimal("decimalValue", 3.14)
            .bool("booleanValue", true)
            .stringEmpty("emptyString")
            .stringArray("stringArray", "one", "two", "three")
            .coding("codingValue", "http://example.org/codesystem|code2|display1")
            .quantity("quantityValue", "11.5 'mg'")
            // Heterogeneous collection
            .elementArray("heteroattr",
                val1 -> val1.choice("value")
                    .string("valueString", "string")
                    .integerEmpty("valueInteger")
                    .boolEmpty("valueBoolean"),
                val2 -> val2.integer("valueInteger", 1),
                val3 -> val3.integer("valueInteger", 2)
            )
        )
        .group("as() function - primitive type matching (positive cases)")
        // Positive type matches - should return the actual value
        .testEquals("test", "stringValue.as(System.String)",
            "as() returns value when it matches System.String type")
        .testEquals(42, "integerValue.as(Integer)",
            "as() returns value when it matches Integer type")
        .testEquals(3.14, "decimalValue.as(decimal)",
            "as() returns value when it matches decimal type")
        .testTrue("booleanValue.as(FHIR.boolean)",
            "as() returns value when it matches FHIR.boolean type")

        .group("as() function - type mismatches (negative cases)")
        // Negative type matches - should return empty collection
        .testEmpty("stringValue.as(Integer)",
            "as() returns empty when type doesn't match")
        .testEmpty("integerValue.as(Boolean)",
            "as() returns empty when value is different type")
        .testEmpty("codingValue.as(Quantity)",
            "as() returns empty when complex type doesn't match")

        .group("as() function - complex types")
        // Complex type matching
        .testEquals(toQuantity("11.5 'mg'"), "quantityValue.as(Quantity)",
            "as() returns Quantity value")
        .testEquals("mg", "quantityValue.as(FHIR.Quantity).unit",
            "as() allows traversal after conversion to Quantity")
        .testEquals(11.5, "quantityValue.as(Quantity).value",
            "as() returns Quantity value and allows traversal")
        .testEquals("mg", "quantityValue.as(FHIR.Quantity).unit",
            "as() works with FHIR namespace for Quantity")
        .testEquals("mg", "quantityValue.as(System.Quantity).unit",
            "as() works with System namespace for Quantity")
        .testEquals("code2", "codingValue.as(FHIR.Coding).code",
            "as() returns Coding value and allows traversal")
        .testEquals("code2", "codingValue.as(System.Coding).code",
            "as() works with System namespace for Coding")

        .group("as() function - namespace variations")
        // Test namespace handling
        .testEquals(toQuantity("11 'mg'"), "(11 'mg').as(Quantity)",
            "as() works with unqualified type name")
        .testEquals(toQuantity("12 'cm'"), "(12 'cm').as(System.Quantity)",
            "as() works with System namespace for Quantity")
        // THIS IS A SPECIAL CASE: FHIR.Quantity is the same as System.Quantity in our model
        .testEquals(toQuantity("13 'mg'"), "(13 'mg').as(FHIR.Quantity)",
            "as() returns works for System.Quantity with FHIR namespace")
        .group("as() function - edge cases")
        // Empty collections
        .testEmpty("emptyString.as(String)",
            "as() returns empty when applied to empty value")
        .testEmpty("{}.as(String)",
            "as() returns empty when applied to empty collection")

        // Multi-item collections should error
        .testError("stringArray.as(String)",
            "as() throws error on multi-item collection")

        .group("as() function - with choice elements")
        .testEquals("string", "heteroattr.first().value.as(String)",
            "as() extracts String from choice element")
        .testEmpty("heteroattr.first().value.as(Integer)",
            "as() returns empty when type doesn't match in choice element")
        .testEquals(1, "heteroattr[1].value.as(Integer)",
            "as() extracts Integer from choice element")
        .group("as() function - comparison with is()")
        // Show relationship between is() and as()
        .testTrue("stringValue.is(String) and stringValue.as(String).exists()",
            "is() returns true corresponds to as() returning a value")
        .testTrue("stringValue.is(Integer).not() and stringValue.as(Integer).empty()",
            "is() returns false corresponds to as() returning empty")

        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testTypeFunctionsOnFhirChoiceCollection() {
    // Create an Observation with multiple components using different value types
    final Observation observation = new Observation();
    observation.setStatus(Observation.ObservationStatus.FINAL);
    observation.setId("example-multi-component");

    // Add CodeableConcept for observation code
    observation.setCode(new CodeableConcept()
        .addCoding(new Coding()
            .setSystem("http://loinc.org")
            .setCode("85354-9")
            .setDisplay("Blood pressure panel")));

    // Component 1: valueQuantity - Systolic blood pressure
    final ObservationComponentComponent systolic = new ObservationComponentComponent();
    systolic.setCode(new CodeableConcept()
        .addCoding(new Coding()
            .setSystem("http://loinc.org")
            .setCode("8480-6")
            .setDisplay("Systolic blood pressure")));
    systolic.setValue(new Quantity()
        .setValue(new BigDecimal("120"))
        .setUnit("mm[Hg]")
        .setSystem("http://unitsofmeasure.org")
        .setCode("mm[Hg]"));
    observation.addComponent(systolic);

    // Component 2: valueQuantity - Diastolic blood pressure
    final ObservationComponentComponent diastolic = new ObservationComponentComponent();
    diastolic.setCode(new CodeableConcept()
        .addCoding(new Coding()
            .setSystem("http://loinc.org")
            .setCode("8462-4")
            .setDisplay("Diastolic blood pressure")));
    diastolic.setValue(new Quantity()
        .setValue(new BigDecimal("80"))
        .setUnit("mm[Hg]")
        .setSystem("http://unitsofmeasure.org")
        .setCode("mm[Hg]"));
    observation.addComponent(diastolic);

    // Component 3: valueString - Clinical Assessment
    final ObservationComponentComponent assessment = new ObservationComponentComponent();
    assessment.setCode(new CodeableConcept()
        .addCoding(new Coding()
            .setSystem("http://example.org")
            .setCode("assessment")
            .setDisplay("Clinical Assessment")));
    assessment.setValue(new StringType("Normal blood pressure reading"));
    observation.addComponent(assessment);

    // Component 4: valueCodeableConcept - Body Position
    final ObservationComponentComponent position = new ObservationComponentComponent();
    position.setCode(new CodeableConcept()
        .addCoding(new Coding()
            .setSystem("http://example.org")
            .setCode("position")
            .setDisplay("Body Position")));
    position.setValue(new CodeableConcept()
        .addCoding(new Coding()
            .setSystem("http://snomed.info/sct")
            .setCode("33586001")
            .setDisplay("Sitting position")));
    observation.addComponent(position);

    // Component 5: valueBoolean - Measurement Verified
    final ObservationComponentComponent verified = new ObservationComponentComponent();
    verified.setCode(new CodeableConcept()
        .addCoding(new Coding()
            .setSystem("http://example.org")
            .setCode("verified")
            .setDisplay("Measurement Verified")));
    verified.setValue(new BooleanType(true));
    observation.addComponent(verified);

    return builder()
        .withResource(observation)
        .group("ofType() function on component.value - filtering by type")
        .testEquals("mm[Hg]", "component.value.ofType(Quantity).first().unit",
            "ofType(Quantity) returns first Quantity value")
        .testEquals(120, "component.value.ofType(Quantity).first().value",
            "ofType(Quantity) allows access to first Quantity value")
        .testEquals("mm[Hg]", "component.value.ofType(Quantity)[1].unit",
            "ofType(Quantity) returns second Quantity value")
        .testEquals(80, "component.value.ofType(Quantity)[1].value",
            "ofType(Quantity) allows access to second Quantity value")
        .testEquals("Normal blood pressure reading", "component.value.ofType(String)",
            "ofType(String) returns the String value")
        .testEquals("33586001", "component.value.ofType(CodeableConcept).coding.code",
            "ofType(CodeableConcept) returns CodeableConcept and allows traversal")
        .testTrue("component.value.ofType(Boolean)",
            "ofType(Boolean) returns the Boolean value")
        .testEmpty("component.value.ofType(Integer)",
            "ofType(Integer) returns empty when no Integer values present")

        .group("ofType() function - namespace variations")
        .testEquals(120, "component.value.ofType(FHIR.Quantity).first().value",
            "ofType(FHIR.Quantity) works with FHIR namespace")
        .testEquals(80, "component.value.ofType(FHIR.Quantity)[1].value",
            "ofType(FHIR.Quantity) returns both Quantity values")
        .testEquals(120, "component.value.ofType(System.Quantity).first().value",
            "ofType(System.Quantity) works with System namespace")
        .testEquals(80, "component.value.ofType(System.Quantity)[1].value",
            "ofType(System.Quantity) returns both Quantity values")
        .testEquals("Normal blood pressure reading", "component.value.ofType(FHIR.string)",
            "ofType(FHIR.string) works with FHIR namespace")
        .testEquals("Normal blood pressure reading", "component.value.ofType(System.String)",
            "ofType(System.String) works with System namespace")

        .group("as() function on component.value - filtering individual elements")
        .testEquals(120, "component[0].value.as(Quantity).value",
            "as(Quantity) returns Quantity value when type matches")
        .testEmpty("component[0].value.as(String)",
            "as(String) returns empty when value is Quantity")
        .testEquals("Normal blood pressure reading", "component[2].value.as(String)",
            "as(String) returns String when value matches")
        .testEmpty("component[2].value.as(Quantity)",
            "as(Quantity) returns empty when value is String")
        .testTrue("component[4].value.as(Boolean)",
            "as(Boolean) returns Boolean when value matches")
        .testEmpty("component[4].value.as(String)",
            "as(String) returns empty when value is Boolean")

        .group("as() function - type conversion and traversal")
        .testEquals("mm[Hg]", "component[0].value.as(Quantity).unit",
            "as(Quantity) allows traversal after conversion")
        .testEquals(80, "component[1].value.as(Quantity).value",
            "as(Quantity) allows access to value property")
        .testEquals("33586001", "component[3].value.as(CodeableConcept).coding.code",
            "as(CodeableConcept) allows traversal to nested properties")

        .group("is() function on component.value - type checking")
        .testTrue("component[0].value.is(Quantity)",
            "is(Quantity) returns true for Quantity value")
        .testFalse("component[0].value.is(String)",
            "is(String) returns false for Quantity value")
        .testTrue("component[2].value.is(String)",
            "is(String) returns true for String value")
        .testFalse("component[2].value.is(Quantity)",
            "is(Quantity) returns false for String value")
        .testTrue("component[3].value.is(CodeableConcept)",
            "is(CodeableConcept) returns true for CodeableConcept value")
        .testTrue("component[4].value.is(Boolean)",
            "is(Boolean) returns true for Boolean value")
        .testFalse("component[4].value.is(Integer)",
            "is(Integer) returns false for Boolean value")

        .group("is() function - namespace variations")
        .testTrue("component[0].value.is(FHIR.Quantity)",
            "is(FHIR.Quantity) works with FHIR namespace")
        .testTrue("component[0].value.is(System.Quantity)",
            "is(System.Quantity) works with System namespace")
        .testTrue("component[2].value.is(FHIR.string)",
            "is(FHIR.string) works with FHIR namespace")
        .testTrue("component[2].value.is(System.String)",
            "is(System.String) works with System namespace")

        .group("Integration - combining type functions")
        .testEquals(120, "component.where(value.is(Quantity)).first().value.as(Quantity).value",
            "where() with is() filters first component by value type")
        .testEquals(80, "component.where(value.is(Quantity))[1].value.as(Quantity).value",
            "where() with is() filters second component by value type")
        .testEquals("Normal blood pressure reading",
            "component.where(value.is(String)).value.as(String)",
            "where() with is() and as() extracts String values")
        .testTrue("component.where(value.is(Boolean)).value.as(Boolean)",
            "where() with is() and as() extracts Boolean values")

        .group("Collection operations - ofType() works on collections")
        .testEquals(120, "component.value.ofType(Quantity).first().value",
            "ofType() works on entire component.value collection")
        .testEquals(80, "component.value.ofType(Quantity)[1].value",
            "ofType() filters entire collection and returns multiple values")
        .testEquals("Normal blood pressure reading", "component.value.ofType(String)",
            "ofType() extracts String from heterogeneous collection")
        .testEquals("33586001", "component.value.ofType(CodeableConcept).coding.first().code",
            "ofType() extracts CodeableConcept from heterogeneous collection")

        .group("Collection operations - is() and as() require singletons")
        .testError("component.value.is(Quantity)",
            "is() throws error on multi-item collection")
        .testError("component.value.as(Quantity)",
            "as() throws error on multi-item collection")
        .testError("component.value.is(String)",
            "is() fails on heterogeneous collection regardless of target type")
        .testError("component.value.as(Boolean)",
            "as() fails on heterogeneous collection regardless of target type")

        .group("ofType() is the recommended way to filter heterogeneous collections")
        .testEquals(120, "component.value.ofType(Quantity).first().value",
            "ofType() is preferred over where($this.is()) for heterogeneous collections")
        .testEquals(80, "component.value.ofType(Quantity)[1].value",
            "ofType() returns all matching values from heterogeneous collection")
        .testEquals("Normal blood pressure reading", "component.value.ofType(String)",
            "ofType() extracts typed values from heterogeneous collection")
        .testTrue("component.value.ofType(Boolean)",
            "ofType() works for all types in heterogeneous collection")
        .testEmpty("component.value.ofType(Integer)",
            "ofType() returns empty when type not present in collection")

        .group("Using is() in where() on homogeneous collections (component level)")
        .testEquals(120, "component.where(value.is(Quantity)).first().value.as(Quantity).value",
            "where() with is() works at component level where each value is singular")
        .testEquals(80, "component.where(value.is(Quantity))[1].value.as(Quantity).value",
            "is() in where() filters components based on their singular value type")
        .testEquals("Normal blood pressure reading",
            "component.where(value.is(String)).value.as(String)",
            "where() with is() extracts components with String values")
        .build();
  }

}
