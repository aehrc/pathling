/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

import static au.csiro.pathling.test.dsl.TypeInfoExpectation.toTypeInfo;
import static au.csiro.pathling.test.yaml.FhirTypedLiteral.toQuantity;

import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import java.math.BigDecimal;
import java.util.stream.Stream;
import org.hl7.fhir.r4.model.Address;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.HumanName;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Observation.ObservationComponentComponent;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Quantity;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.DynamicTest;

/** Tests for FHIRPath type functions. */
public class TypeFunctionsDslTest extends FhirPathDslTestBase {

  @FhirPathTest
  public Stream<DynamicTest> testIsFunction() {
    return builder()
        .withSubject(
            sb ->
                sb
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
                    .elementArray(
                        "heteroattr",
                        val1 ->
                            val1.choice("value")
                                .string("valueString", "string")
                                .integerEmpty("valueInteger")
                                .boolEmpty("valueBoolean"),
                        val2 -> val2.integer("valueInteger", 1),
                        val3 -> val3.integer("valueInteger", 2)))
        .group("is() function - primitive type matching")
        // Positive type matches
        .testTrue(
            "stringValue.is(System.String)",
            "is() returns true when value matches System.String type")
        .testTrue("integerValue.is(Integer)", "is() returns true when value matches Integer type")
        .testTrue("decimalValue.is(decimal)", "is() returns true when value matches decimal type")
        .testTrue(
            "booleanValue.is(FHIR.boolean)",
            "is() returns true when value matches FHIR.boolean type")
        .group("is() function - type mismatches")
        // Negative type matches
        .testFalse("stringValue.is(Integer)", "is() returns false when type doesn't match")
        .testFalse("integerValue.is(Boolean)", "is() returns false when value is different type")
        .testFalse("codingValue.is(Quantity)", "is() returns false when complex type doesn't match")
        .group("is() function - complex types")
        // Complex type matching
        .testTrue("quantityValue.is(Quantity)", "is() returns true for Quantity complex type")
        .testTrue(
            "quantityValue.is(FHIR.Quantity)", "is() returns true with explicit FHIR namespace")
        .testTrue("codingValue.is(FHIR.Coding)", "is() returns true for Coding with FHIR namespace")
        .testTrue(
            "codingValue.is(System.Coding)", "is() returns true for Coding with System namespace")
        .testTrue("codingValue.is(Coding)", "is() returns true for Coding with unqualified name")
        .group("is() function - edge cases")
        // Empty collections
        .testEmpty("emptyString.is(String)", "is() returns empty when applied to empty value")
        .testEmpty("{}.is(String)", "is() returns empty when applied to empty collection")

        // Multi-item collections should error
        .testError("stringArray.is(String)", "is() throws error on multi-item collection")
        .group("is() function - integration with other functions")
        // Integration with boolean functions
        .testFalse("stringValue.is(String).not()", "is() result can be negated with not()")
        .testTrue("stringValue.is(Integer).not()", "is() false result can be negated with not()")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testAsFunction() {
    return builder()
        .withSubject(
            sb ->
                sb
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
                    .elementArray(
                        "heteroattr",
                        val1 ->
                            val1.choice("value")
                                .string("valueString", "string")
                                .integerEmpty("valueInteger")
                                .boolEmpty("valueBoolean"),
                        val2 -> val2.integer("valueInteger", 1),
                        val3 -> val3.integer("valueInteger", 2)))
        .group("as() function - primitive type matching (positive cases)")
        // Positive type matches - should return the actual value
        .testEquals(
            "test",
            "stringValue.as(System.String)",
            "as() returns value when it matches System.String type")
        .testEquals(
            42, "integerValue.as(Integer)", "as() returns value when it matches Integer type")
        .testEquals(
            3.14, "decimalValue.as(decimal)", "as() returns value when it matches decimal type")
        .testTrue(
            "booleanValue.as(FHIR.boolean)", "as() returns value when it matches FHIR.boolean type")
        .group("as() function - type mismatches (negative cases)")
        // Negative type matches - should return empty collection
        .testEmpty("stringValue.as(Integer)", "as() returns empty when type doesn't match")
        .testEmpty("integerValue.as(Boolean)", "as() returns empty when value is different type")
        .testEmpty("codingValue.as(Quantity)", "as() returns empty when complex type doesn't match")
        .group("as() function - complex types")
        // Complex type matching
        .testEquals(
            toQuantity("11.5 'mg'"), "quantityValue.as(Quantity)", "as() returns Quantity value")
        .testEquals(
            "mg",
            "quantityValue.as(FHIR.Quantity).unit",
            "as() allows traversal after conversion to Quantity")
        .testEquals(
            11.5,
            "quantityValue.as(Quantity).value",
            "as() returns Quantity value and allows traversal")
        .testEquals(
            "mg",
            "quantityValue.as(FHIR.Quantity).unit",
            "as() works with FHIR namespace for Quantity")
        .testEquals(
            "mg",
            "quantityValue.as(System.Quantity).unit",
            "as() works with System namespace for Quantity")
        .testEquals(
            "code2",
            "codingValue.as(FHIR.Coding).code",
            "as() returns Coding value and allows traversal")
        .testEquals(
            "code2",
            "codingValue.as(System.Coding).code",
            "as() works with System namespace for Coding")
        .group("as() function - namespace variations")
        // Test namespace handling
        .testEquals(
            toQuantity("11 'mg'"),
            "(11 'mg').as(Quantity)",
            "as() works with unqualified type name")
        .testEquals(
            toQuantity("12 'cm'"),
            "(12 'cm').as(System.Quantity)",
            "as() works with System namespace for Quantity")
        // THIS IS A SPECIAL CASE: FHIR.Quantity is the same as System.Quantity in our model
        .testEquals(
            toQuantity("13 'mg'"),
            "(13 'mg').as(FHIR.Quantity)",
            "as() returns works for System.Quantity with FHIR namespace")
        .group("as() function - edge cases")
        // Empty collections
        .testEmpty("emptyString.as(String)", "as() returns empty when applied to empty value")
        .testEmpty("{}.as(String)", "as() returns empty when applied to empty collection")

        // Multi-item collections should error
        .testError("stringArray.as(String)", "as() throws error on multi-item collection")
        .group("as() function - with choice elements")
        .testEquals(
            "string",
            "heteroattr.first().value.as(String)",
            "as() extracts String from choice element")
        .testEmpty(
            "heteroattr.first().value.as(Integer)",
            "as() returns empty when type doesn't match in choice element")
        .testEquals(
            1, "heteroattr[1].value.as(Integer)", "as() extracts Integer from choice element")
        .group("as() function - comparison with is()")
        // Show relationship between is() and as()
        .testTrue(
            "stringValue.is(String) and stringValue.as(String).exists()",
            "is() returns true corresponds to as() returning a value")
        .testTrue(
            "stringValue.is(Integer).not() and stringValue.as(Integer).empty()",
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
    observation.setCode(
        new CodeableConcept()
            .addCoding(
                new Coding()
                    .setSystem("http://loinc.org")
                    .setCode("85354-9")
                    .setDisplay("Blood pressure panel")));

    // Component 1: valueQuantity - Systolic blood pressure
    final ObservationComponentComponent systolic = new ObservationComponentComponent();
    systolic.setCode(
        new CodeableConcept()
            .addCoding(
                new Coding()
                    .setSystem("http://loinc.org")
                    .setCode("8480-6")
                    .setDisplay("Systolic blood pressure")));
    systolic.setValue(
        new Quantity()
            .setValue(new BigDecimal("120"))
            .setUnit("mm[Hg]")
            .setSystem("http://unitsofmeasure.org")
            .setCode("mm[Hg]"));
    observation.addComponent(systolic);

    // Component 2: valueQuantity - Diastolic blood pressure
    final ObservationComponentComponent diastolic = new ObservationComponentComponent();
    diastolic.setCode(
        new CodeableConcept()
            .addCoding(
                new Coding()
                    .setSystem("http://loinc.org")
                    .setCode("8462-4")
                    .setDisplay("Diastolic blood pressure")));
    diastolic.setValue(
        new Quantity()
            .setValue(new BigDecimal("80"))
            .setUnit("mm[Hg]")
            .setSystem("http://unitsofmeasure.org")
            .setCode("mm[Hg]"));
    observation.addComponent(diastolic);

    // Component 3: valueString - Clinical Assessment
    final ObservationComponentComponent assessment = new ObservationComponentComponent();
    assessment.setCode(
        new CodeableConcept()
            .addCoding(
                new Coding()
                    .setSystem("http://example.org")
                    .setCode("assessment")
                    .setDisplay("Clinical Assessment")));
    assessment.setValue(new StringType("Normal blood pressure reading"));
    observation.addComponent(assessment);

    // Component 4: valueCodeableConcept - Body Position
    final ObservationComponentComponent position = new ObservationComponentComponent();
    position.setCode(
        new CodeableConcept()
            .addCoding(
                new Coding()
                    .setSystem("http://example.org")
                    .setCode("position")
                    .setDisplay("Body Position")));
    position.setValue(
        new CodeableConcept()
            .addCoding(
                new Coding()
                    .setSystem("http://snomed.info/sct")
                    .setCode("33586001")
                    .setDisplay("Sitting position")));
    observation.addComponent(position);

    // Component 5: valueBoolean - Measurement Verified
    final ObservationComponentComponent verified = new ObservationComponentComponent();
    verified.setCode(
        new CodeableConcept()
            .addCoding(
                new Coding()
                    .setSystem("http://example.org")
                    .setCode("verified")
                    .setDisplay("Measurement Verified")));
    verified.setValue(new BooleanType(true));
    observation.addComponent(verified);

    return builder()
        .withResource(observation)
        .group("ofType() function on component.value - filtering by type")
        .testEquals(
            "mm[Hg]",
            "component.value.ofType(Quantity).first().unit",
            "ofType(Quantity) returns first Quantity value")
        .testEquals(
            120,
            "component.value.ofType(Quantity).first().value",
            "ofType(Quantity) allows access to first Quantity value")
        .testEquals(
            "mm[Hg]",
            "component.value.ofType(Quantity)[1].unit",
            "ofType(Quantity) returns second Quantity value")
        .testEquals(
            80,
            "component.value.ofType(Quantity)[1].value",
            "ofType(Quantity) allows access to second Quantity value")
        .testEquals(
            "Normal blood pressure reading",
            "component.value.ofType(String)",
            "ofType(String) returns the String value")
        .testEquals(
            "33586001",
            "component.value.ofType(CodeableConcept).coding.code",
            "ofType(CodeableConcept) returns CodeableConcept and allows traversal")
        .testTrue("component.value.ofType(Boolean)", "ofType(Boolean) returns the Boolean value")
        .testEmpty(
            "component.value.ofType(Integer)",
            "ofType(Integer) returns empty when no Integer values present")
        .group("ofType() function - namespace variations")
        .testEquals(
            120,
            "component.value.ofType(FHIR.Quantity).first().value",
            "ofType(FHIR.Quantity) works with FHIR namespace")
        .testEquals(
            80,
            "component.value.ofType(FHIR.Quantity)[1].value",
            "ofType(FHIR.Quantity) returns both Quantity values")
        .testEquals(
            120,
            "component.value.ofType(System.Quantity).first().value",
            "ofType(System.Quantity) works with System namespace")
        .testEquals(
            80,
            "component.value.ofType(System.Quantity)[1].value",
            "ofType(System.Quantity) returns both Quantity values")
        .testEquals(
            "Normal blood pressure reading",
            "component.value.ofType(FHIR.string)",
            "ofType(FHIR.string) works with FHIR namespace")
        .testEquals(
            "Normal blood pressure reading",
            "component.value.ofType(System.String)",
            "ofType(System.String) works with System namespace")
        .group("as() function on component.value - filtering individual elements")
        .testEquals(
            120,
            "component[0].value.as(Quantity).value",
            "as(Quantity) returns Quantity value when type matches")
        .testEmpty(
            "component[0].value.as(String)", "as(String) returns empty when value is Quantity")
        .testEquals(
            "Normal blood pressure reading",
            "component[2].value.as(String)",
            "as(String) returns String when value matches")
        .testEmpty(
            "component[2].value.as(Quantity)", "as(Quantity) returns empty when value is String")
        .testTrue(
            "component[4].value.as(Boolean)", "as(Boolean) returns Boolean when value matches")
        .testEmpty(
            "component[4].value.as(String)", "as(String) returns empty when value is Boolean")
        .group("as() function - type conversion and traversal")
        .testEquals(
            "mm[Hg]",
            "component[0].value.as(Quantity).unit",
            "as(Quantity) allows traversal after conversion")
        .testEquals(
            80,
            "component[1].value.as(Quantity).value",
            "as(Quantity) allows access to value property")
        .testEquals(
            "33586001",
            "component[3].value.as(CodeableConcept).coding.code",
            "as(CodeableConcept) allows traversal to nested properties")
        .group("is() function on component.value - type checking")
        .testTrue("component[0].value.is(Quantity)", "is(Quantity) returns true for Quantity value")
        .testFalse("component[0].value.is(String)", "is(String) returns false for Quantity value")
        .testTrue("component[2].value.is(String)", "is(String) returns true for String value")
        .testFalse("component[2].value.is(Quantity)", "is(Quantity) returns false for String value")
        .testTrue(
            "component[3].value.is(CodeableConcept)",
            "is(CodeableConcept) returns true for CodeableConcept value")
        .testTrue("component[4].value.is(Boolean)", "is(Boolean) returns true for Boolean value")
        .testFalse("component[4].value.is(Integer)", "is(Integer) returns false for Boolean value")
        .group("is() function - namespace variations")
        .testTrue(
            "component[0].value.is(FHIR.Quantity)", "is(FHIR.Quantity) works with FHIR namespace")
        .testTrue(
            "component[0].value.is(System.Quantity)",
            "is(System.Quantity) works with System namespace")
        .testTrue("component[2].value.is(FHIR.string)", "is(FHIR.string) works with FHIR namespace")
        .testTrue(
            "component[2].value.is(System.String)", "is(System.String) works with System namespace")
        .group("Integration - combining type functions")
        .testEquals(
            120,
            "component.where(value.is(Quantity)).first().value.as(Quantity).value",
            "where() with is() filters first component by value type")
        .testEquals(
            80,
            "component.where(value.is(Quantity))[1].value.as(Quantity).value",
            "where() with is() filters second component by value type")
        .testEquals(
            "Normal blood pressure reading",
            "component.where(value.is(String)).value.as(String)",
            "where() with is() and as() extracts String values")
        .testTrue(
            "component.where(value.is(Boolean)).value.as(Boolean)",
            "where() with is() and as() extracts Boolean values")
        .group("Collection operations - ofType() works on collections")
        .testEquals(
            120,
            "component.value.ofType(Quantity).first().value",
            "ofType() works on entire component.value collection")
        .testEquals(
            80,
            "component.value.ofType(Quantity)[1].value",
            "ofType() filters entire collection and returns multiple values")
        .testEquals(
            "Normal blood pressure reading",
            "component.value.ofType(String)",
            "ofType() extracts String from heterogeneous collection")
        .testEquals(
            "33586001",
            "component.value.ofType(CodeableConcept).coding.first().code",
            "ofType() extracts CodeableConcept from heterogeneous collection")
        .group("Collection operations - is() and as() require singletons")
        .testError("component.value.is(Quantity)", "is() throws error on multi-item collection")
        .testError("component.value.as(Quantity)", "as() throws error on multi-item collection")
        .testError(
            "component.value.is(String)",
            "is() fails on heterogeneous collection regardless of target type")
        .testError(
            "component.value.as(Boolean)",
            "as() fails on heterogeneous collection regardless of target type")
        .group("ofType() is the recommended way to filter heterogeneous collections")
        .testEquals(
            120,
            "component.value.ofType(Quantity).first().value",
            "ofType() is preferred over where($this.is()) for heterogeneous collections")
        .testEquals(
            80,
            "component.value.ofType(Quantity)[1].value",
            "ofType() returns all matching values from heterogeneous collection")
        .testEquals(
            "Normal blood pressure reading",
            "component.value.ofType(String)",
            "ofType() extracts typed values from heterogeneous collection")
        .testTrue(
            "component.value.ofType(Boolean)",
            "ofType() works for all types in heterogeneous collection")
        .testEmpty(
            "component.value.ofType(Integer)",
            "ofType() returns empty when type not present in collection")
        .group("Using is() in where() on homogeneous collections (component level)")
        .testEquals(
            120,
            "component.where(value.is(Quantity)).first().value.as(Quantity).value",
            "where() with is() works at component level where each value is singular")
        .testEquals(
            80,
            "component.where(value.is(Quantity))[1].value.as(Quantity).value",
            "is() in where() filters components based on their singular value type")
        .testEquals(
            "Normal blood pressure reading",
            "component.where(value.is(String)).value.as(String)",
            "where() with is() extracts components with String values")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testTypeFunctionsOnFhirComplexTypes() {
    // Create a Patient with multiple complex type elements
    final Patient patient = new Patient();
    patient.setId("example-complex-types");

    // Add multiple HumanName items (for collection testing)
    patient.addName(new HumanName().setFamily("Smith").addGiven("John").addGiven("David"));

    patient.addName(
        new HumanName().setFamily("Jones").addGiven("Mary").setUse(HumanName.NameUse.MAIDEN));

    patient.addName(
        new HumanName().setFamily("Smith").addGiven("Jane").setUse(HumanName.NameUse.NICKNAME));

    // Add multiple Address items (for collection testing)
    patient.addAddress(
        new Address()
            .setCity("Sydney")
            .setCountry("Australia")
            .setState("NSW")
            .setPostalCode("2000")
            .setUse(Address.AddressUse.HOME));

    patient.addAddress(
        new Address()
            .setCity("Melbourne")
            .setCountry("Australia")
            .setState("VIC")
            .setPostalCode("3000")
            .setUse(Address.AddressUse.WORK));

    return builder()
        .withResource(patient)
        .group("ofType() function - filtering HumanName collections")
        .testEquals(
            "Smith",
            "name.ofType(FHIR.HumanName).first().family",
            "ofType(HumanName) returns first HumanName and allows property access")
        .testEquals(
            "Jones",
            "name.ofType(FHIR.HumanName)[1].family",
            "ofType(HumanName) returns second HumanName from collection")
        .testEquals(
            "Smith",
            "name.ofType(FHIR.HumanName)[2].family",
            "ofType(HumanName) returns third HumanName from collection")
        .testEquals(
            "John",
            "name.ofType(FHIR.HumanName).first().given.first()",
            "ofType(HumanName) allows traversal to repeated given names")
        .testTrue(
            "name.ofType(FHIR.HumanName) = name",
            "ofType(HumanName) returns all HumanName items from collection")
        .group("ofType() function - filtering Address collections")
        .testEquals(
            "Sydney",
            "address.ofType(FHIR.Address).first().city",
            "ofType(Address) returns first Address and allows property access")
        .testEquals(
            "Melbourne",
            "address.ofType(FHIR.Address)[1].city",
            "ofType(Address) returns second Address from collection")
        .testEquals(
            "Australia",
            "address.ofType(FHIR.Address).first().country",
            "ofType(Address) allows traversal to country property")
        .testTrue(
            "address.ofType(FHIR.Address) = address",
            "ofType(Address) returns all Address items from collection")
        .group("ofType() function - namespace variations for complex types")
        .testEquals(
            "Smith",
            "name.ofType(FHIR.HumanName).first().family",
            "ofType(FHIR.HumanName) works with FHIR namespace")
        .testEquals(
            "Sydney",
            "address.ofType(FHIR.Address).first().city",
            "ofType(FHIR.Address) works with FHIR namespace")
        .testError(
            "name.ofType(System.HumanName)",
            "ofType(System.HumanName) throws error - complex types only exist in FHIR namespace")
        .testError(
            "address.ofType(System.Address)",
            "ofType(System.Address) throws error - complex types only exist in FHIR namespace")
        .group("ofType() function - empty results")
        .testEmpty(
            "name.ofType(ContactPoint)",
            "ofType(ContactPoint) returns empty when type not present in name collection")
        .testEmpty(
            "address.ofType(FHIR.HumanName)",
            "ofType(HumanName) returns empty when applied to address collection")
        .testEmpty(
            "name.ofType(Quantity)",
            "ofType(Quantity) returns empty when primitive type requested on complex type"
                + " collection")
        .group("is() function - HumanName type checking on singletons")
        .testTrue(
            "name.first().is(FHIR.HumanName)",
            "is(HumanName) returns true when first name is HumanName")
        .testTrue(
            "name[1].is(FHIR.HumanName)",
            "is(HumanName) returns true when second name is HumanName")
        .testFalse(
            "name.first().is(FHIR.Address)", "is(Address) returns false when name is HumanName")
        .testFalse(
            "name.first().is(ContactPoint)",
            "is(ContactPoint) returns false when name is HumanName")
        .testFalse(
            "name.first().is(String)",
            "is(String) returns false when HumanName is not a primitive type")
        .group("is() function - Address type checking on singletons")
        .testTrue(
            "address.first().is(FHIR.Address)",
            "is(Address) returns true when first address is Address")
        .testTrue(
            "address[1].is(FHIR.Address)",
            "is(Address) returns true when second address is Address")
        .testFalse(
            "address.first().is(FHIR.HumanName)",
            "is(HumanName) returns false when address is Address")
        .testFalse(
            "address.first().is(ContactPoint)",
            "is(ContactPoint) returns false when address is Address")
        .group("is() function - namespace variations for complex types")
        .testTrue("name.first().is(FHIR.HumanName)", "is(FHIR.HumanName) works with FHIR namespace")
        .testTrue("address.first().is(FHIR.Address)", "is(FHIR.Address) works with FHIR namespace")
        .testError(
            "name.first().is(System.HumanName)",
            "is(System.HumanName) throws error - complex types only exist in FHIR namespace")
        .testError(
            "address.first().is(System.Address)",
            "is(System.Address) throws error - complex types only exist in FHIR namespace")
        .group("is() function - edge cases on complex types")
        .testError("name.is(FHIR.HumanName)", "is(HumanName) throws error on multi-item collection")
        .testError("address.is(FHIR.Address)", "is(Address) throws error on multi-item collection")
        .testEmpty(
            "name.where(family = 'NonExistent').first().is(FHIR.HumanName)",
            "is(HumanName) returns empty when applied to empty singleton")
        .group("as() function - HumanName type casting with property traversal")
        .testEquals(
            "Smith",
            "name.first().as(FHIR.HumanName).family",
            "as(HumanName) returns HumanName and allows family property access")
        .testEquals(
            "Jones",
            "name[1].as(FHIR.HumanName).family",
            "as(HumanName) works on second name with property traversal")
        .testEquals(
            "John",
            "name.first().as(FHIR.HumanName).given.first()",
            "as(HumanName) allows traversal to repeated given names")
        .testEquals(
            "Mary",
            "name[1].as(FHIR.HumanName).given.first()",
            "as(HumanName) allows property traversal on indexed element")
        .group("as() function - Address type casting with property traversal")
        .testEquals(
            "Sydney",
            "address.first().as(FHIR.Address).city",
            "as(Address) returns Address and allows city property access")
        .testEquals(
            "Melbourne",
            "address[1].as(FHIR.Address).city",
            "as(Address) works on second address with property traversal")
        .testEquals(
            "Australia",
            "address.first().as(FHIR.Address).country",
            "as(Address) allows country property access")
        .testEquals(
            "NSW",
            "address.first().as(FHIR.Address).state",
            "as(Address) allows state property access")
        .group("as() function - failed casts return empty")
        .testEmpty(
            "name.first().as(FHIR.Address)",
            "as(Address) returns empty when name is not Address type")
        .testEmpty(
            "name.first().as(ContactPoint)",
            "as(ContactPoint) returns empty when name is not ContactPoint type")
        .testEmpty(
            "address.first().as(FHIR.HumanName)",
            "as(HumanName) returns empty when address is not HumanName type")
        .testEmpty(
            "address.first().as(String)",
            "as(String) returns empty when Address is not a primitive type")
        .group("as() function - namespace variations for complex types")
        .testEquals(
            "Smith",
            "name.first().as(FHIR.HumanName).family",
            "as(FHIR.HumanName) works with FHIR namespace")
        .testEquals(
            "Sydney",
            "address.first().as(FHIR.Address).city",
            "as(FHIR.Address) works with FHIR namespace")
        .testError(
            "name.first().as(System.HumanName)",
            "as(System.HumanName) throws error - complex types only exist in FHIR namespace")
        .testError(
            "address.first().as(System.Address)",
            "as(System.Address) throws error - complex types only exist in FHIR namespace")
        .group("as() function - edge cases on complex types")
        .testError("name.as(FHIR.HumanName)", "as(HumanName) throws error on multi-item collection")
        .testError("address.as(FHIR.Address)", "as(Address) throws error on multi-item collection")
        .testEmpty(
            "name.where(family = 'NonExistent').first().as(FHIR.HumanName).family",
            "as(HumanName) on empty singleton returns empty")
        .group("Integration - combining type functions with where()")
        .testEquals(
            "Smith",
            "name.where($this.is(FHIR.HumanName)).first().family",
            "where() with is() filters names by type")
        .testEquals(
            "Jones",
            "name.where($this.is(FHIR.HumanName))[1].family",
            "where() with is() preserves collection order")
        .testEquals(
            "Smith",
            "name.where($this.is(FHIR.HumanName) and family = 'Smith').first().family",
            "where() combines is() with property conditions")
        .testEquals(
            "Sydney",
            "address.where($this.is(FHIR.Address)).first().city",
            "where() with is() filters addresses by type")
        .testEquals(
            "Sydney",
            "address.where($this.is(FHIR.Address) and use = 'home').city",
            "where() combines is() with property conditions on Address")
        .group("Integration - chaining type functions")
        .testEquals(
            "Smith",
            "name.ofType(FHIR.HumanName).first().as(FHIR.HumanName).family",
            "ofType() can be followed by as() for type casting")
        .testTrue(
            "name.first().is(FHIR.HumanName) and name.first().as(FHIR.HumanName).exists()",
            "is() returns true corresponds to as() returning value")
        .testEquals(
            "Sydney",
            "address.ofType(FHIR.Address).where(city = 'Sydney').first().as(FHIR.Address).city",
            "ofType(), where(), and as() can be chained together")
        .group("Integration - real-world patterns")
        .testEquals(
            "Smith",
            "name.ofType(FHIR.HumanName).where(use = 'nickname').family",
            "ofType() filters by type then where() filters by property")
        .testEquals(
            "Melbourne",
            "address.ofType(FHIR.Address).where(use = 'work').city",
            "ofType() and where() combination for business logic")
        .testEquals(
            "Smith",
            "name.where($this.is(FHIR.HumanName) and family = 'Smith')[1].family",
            "Filter names by type and property returns multiple matching items")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testTypeFunction() {
    final Patient patient = new Patient();
    patient.setId("type-test");
    patient.setActive(true);
    patient.setBirthDateElement(new org.hl7.fhir.r4.model.DateType("1990-01-01"));
    patient.setMaritalStatus(
        new CodeableConcept()
            .addCoding(
                new Coding(
                    "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus", "M", "Married")));
    patient.addName(new HumanName().setFamily("Smith").addGiven("John").addGiven("David"));
    patient.addContact(
        new Patient.ContactComponent()
            .setName(new HumanName().setFamily("Jones"))
            .addRelationship(
                new CodeableConcept()
                    .addCoding(
                        new Coding(
                            "http://terminology.hl7.org/CodeSystem/v2-0131", "N", "Next-of-Kin"))));

    return builder()
        .withResource(patient)
        // System primitive literals
        .group("type() - System primitive literals")
        .testEquals(
            toTypeInfo("System.Integer(System.Any)"),
            "1.type()",
            "Integer literal type is System.Integer")
        .testEquals(
            toTypeInfo("System.Boolean(System.Any)"),
            "true.type()",
            "Boolean literal type is System.Boolean")
        .testEquals(
            toTypeInfo("System.String(System.Any)"),
            "'hello'.type()",
            "String literal type is System.String")
        .testEquals(
            toTypeInfo("System.Decimal(System.Any)"),
            "3.14.type()",
            "Decimal literal type is System.Decimal")
        .testEquals(
            toTypeInfo("System.Date(System.Any)"),
            "@2024-01-01.type()",
            "Date literal type is System.Date")
        .testEquals(
            toTypeInfo("System.DateTime(System.Any)"),
            "@2024-01-01T10:00:00.type()",
            "DateTime literal type is System.DateTime")
        .testEquals(
            toTypeInfo("System.Time(System.Any)"),
            "@T10:00:00.type()",
            "Time literal type is System.Time")
        // System Quantity and Coding literals
        .group("type() - System Quantity and Coding literals")
        .testEquals(
            toTypeInfo("System.Quantity(System.Any)"),
            "(10 'mg').type()",
            "Quantity literal type is System.Quantity")
        .testEquals(
            toTypeInfo("System.Coding(System.Any)"),
            "(http://example.com|code).type()",
            "Coding literal type is System.Coding")
        // FHIR primitive elements
        .group("type() - FHIR primitive elements")
        .testEquals(
            toTypeInfo("FHIR.boolean(FHIR.Element)"),
            "Patient.active.type()",
            "FHIR boolean element type is FHIR.boolean")
        .testEquals(
            toTypeInfo("FHIR.date(FHIR.Element)"),
            "Patient.birthDate.type()",
            "FHIR date element type is FHIR.date")
        // FHIR complex type elements
        .group("type() - FHIR complex type elements")
        .testEquals(
            toTypeInfo("FHIR.CodeableConcept(FHIR.Element)"),
            "Patient.maritalStatus.type()",
            "CodeableConcept element type is FHIR.CodeableConcept")
        // FHIR backbone elements return generic BackboneElement type.
        .group("type() - FHIR backbone elements")
        .testEquals(
            toTypeInfo("FHIR.BackboneElement(FHIR.Element)"),
            "Patient.contact.first().type()",
            "Backbone element type is FHIR.BackboneElement")
        // FHIR resource types
        .group("type() - FHIR resource types")
        .testEquals(
            toTypeInfo("FHIR.Patient(FHIR.Resource)"),
            "Patient.type()",
            "Patient resource type is FHIR.Patient")
        // Empty collection
        .group("type() - empty collection")
        .testEmpty("{}.type()", "type() returns empty for empty collection")
        // Nested type().type() returns System.Object
        .group("type() - nested type() call")
        .testEquals(
            toTypeInfo("System.Object(System.Any)"),
            "1.type().type()",
            "Nested type() returns System.Object")
        // ofType() followed by type()
        .group("type() - ofType() followed by type()")
        .testEquals(
            "Patient",
            "Patient.ofType(Patient).type().name",
            "ofType() followed by type() returns correct name")
        // FHIRPath operations produce System types
        .group("type() - operations produce System types")
        .testEquals(
            toTypeInfo("System.Boolean(System.Any)"),
            "Patient.active.not().type()",
            "not() produces System.Boolean")
        .testEquals(
            toTypeInfo("System.String(System.Any)"),
            "(Patient.name.first().given.first() + Patient.name.first().family).type()",
            "String concatenation produces System.String")
        // Multiple elements produce multiple TypeInfo structs
        .group("type() - multiple elements")
        .testEquals(
            2, "('John' | 'Mary').type().count()", "type() returns one TypeInfo per element")
        // FHIR plural collection produces one TypeInfo per element
        .group("type() - FHIR plural collection")
        .testEquals(
            toTypeInfo("FHIR.string(FHIR.Element)"),
            "Patient.name.given.type().first()",
            "FHIR plural collection type() returns FHIR.string")
        .testEquals(
            2,
            "Patient.name.given.type().count()",
            "FHIR plural collection type() returns one TypeInfo per non-null element")
        // where() preserves FHIR context
        .group("type() - where() preserves FHIR context")
        .testEquals(
            toTypeInfo("FHIR.boolean(FHIR.Element)"),
            "Patient.active.where($this).type()",
            "where() preserves FHIR type context")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testTypeFunctionOnChoiceFields() {
    // Create an Observation with multiple components using different value types.
    final Observation observation = new Observation();
    observation.setId("type-choice-test");
    observation.setStatus(Observation.ObservationStatus.FINAL);
    observation.setCode(
        new CodeableConcept()
            .addCoding(new Coding("http://loinc.org", "85354-9", "Blood pressure panel")));

    // Component 0: valueQuantity
    final ObservationComponentComponent comp0 = new ObservationComponentComponent();
    comp0.setCode(
        new CodeableConcept().addCoding(new Coding("http://loinc.org", "8480-6", "Systolic")));
    comp0.setValue(
        new Quantity()
            .setValue(new BigDecimal("120"))
            .setUnit("mm[Hg]")
            .setSystem("http://unitsofmeasure.org")
            .setCode("mm[Hg]"));
    observation.addComponent(comp0);

    // Component 1: valueQuantity
    final ObservationComponentComponent comp1 = new ObservationComponentComponent();
    comp1.setCode(
        new CodeableConcept().addCoding(new Coding("http://loinc.org", "8462-4", "Diastolic")));
    comp1.setValue(
        new Quantity()
            .setValue(new BigDecimal("80"))
            .setUnit("mm[Hg]")
            .setSystem("http://unitsofmeasure.org")
            .setCode("mm[Hg]"));
    observation.addComponent(comp1);

    // Component 2: valueString
    final ObservationComponentComponent comp2 = new ObservationComponentComponent();
    comp2.setCode(
        new CodeableConcept()
            .addCoding(new Coding("http://example.org", "assessment", "Assessment")));
    comp2.setValue(new StringType("Normal reading"));
    observation.addComponent(comp2);

    // Component 3: valueCodeableConcept
    final ObservationComponentComponent comp3 = new ObservationComponentComponent();
    comp3.setCode(
        new CodeableConcept().addCoding(new Coding("http://example.org", "position", "Position")));
    comp3.setValue(
        new CodeableConcept()
            .addCoding(new Coding("http://snomed.info/sct", "33586001", "Sitting")));
    observation.addComponent(comp3);

    // Component 4: valueBoolean
    final ObservationComponentComponent comp4 = new ObservationComponentComponent();
    comp4.setCode(
        new CodeableConcept().addCoding(new Coding("http://example.org", "verified", "Verified")));
    comp4.setValue(new BooleanType(true));
    observation.addComponent(comp4);

    // Component 5: no value set
    final ObservationComponentComponent comp5 = new ObservationComponentComponent();
    comp5.setCode(
        new CodeableConcept().addCoding(new Coding("http://example.org", "empty", "Empty")));
    observation.addComponent(comp5);

    return builder()
        .withResource(observation)
        // Singular choice elements - type() returns correct TypeInfo per type.
        .group("type() - singular choice elements")
        .testEquals(
            toTypeInfo("FHIR.Quantity(FHIR.Element)"),
            "component[0].value.type()",
            "Quantity choice element type is FHIR.Quantity")
        .testEquals(
            toTypeInfo("FHIR.string(FHIR.Element)"),
            "component[2].value.type()",
            "String choice element type is FHIR.string")
        .testEquals(
            toTypeInfo("FHIR.CodeableConcept(FHIR.Element)"),
            "component[3].value.type()",
            "CodeableConcept choice element type is FHIR.CodeableConcept")
        .testEquals(
            toTypeInfo("FHIR.boolean(FHIR.Element)"),
            "component[4].value.type()",
            "Boolean choice element type is FHIR.boolean")
        // Choice element with no value set returns null.
        .group("type() - choice element with no value")
        .testTrue(
            "component[5].value.type().name.empty()",
            "Choice element with no value returns empty type name")
        // Plural choice collection - heterogeneous types.
        .group("type() - plural choice collection")
        .testEquals(
            6,
            "component.value.type().count()",
            "type() returns one TypeInfo per component value including null")
        .testEquals(
            toTypeInfo("FHIR.Quantity(FHIR.Element)"),
            "component.value.type().first()",
            "First component type is FHIR.Quantity")
        .testEquals(
            toTypeInfo("FHIR.Quantity(FHIR.Element)"),
            "component.value.type()[1]",
            "Second component type is FHIR.Quantity")
        .testEquals(
            toTypeInfo("FHIR.string(FHIR.Element)"),
            "component.value.type()[2]",
            "Third component type is FHIR.string")
        .testEquals(
            toTypeInfo("FHIR.CodeableConcept(FHIR.Element)"),
            "component.value.type()[3]",
            "Fourth component type is FHIR.CodeableConcept")
        .testEquals(
            toTypeInfo("FHIR.boolean(FHIR.Element)"),
            "component.value.type()[4]",
            "Fifth component type is FHIR.boolean")
        // Composition with other FHIRPath functions.
        .group("type() - composition with FHIRPath functions")
        .testEquals(
            4,
            "component.value.type().name.distinct().count()",
            "Distinct type names count is 4 (Quantity, string, CodeableConcept, boolean)")
        .testEquals(
            2,
            "component.where(value.type().name = 'Quantity').count()",
            "where() filtering by type name finds Quantity components")
        .testEquals(
            1,
            "component.where(value.type().name = 'string').count()",
            "where() filtering by type name finds string components")
        .build();
  }
}
