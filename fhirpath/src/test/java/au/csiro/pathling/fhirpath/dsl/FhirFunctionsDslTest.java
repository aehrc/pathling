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

import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import java.util.List;
import java.util.stream.Stream;
import org.hl7.fhir.r4.model.Address;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.ContactPoint;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.HumanName;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.DynamicTest;

public class FhirFunctionsDslTest extends FhirPathDslTestBase {

  @FhirPathTest
  public Stream<DynamicTest> testExtensionWithDifferentTypes() {
    final Patient patient = new Patient();

    // Add extensions with different value types
    patient.addExtension(
        new Extension("http://example.org/stringExt", new StringType("string_value")));
    patient.addExtension(new Extension("http://example.org/booleanExt", new BooleanType(true)));
    patient.addExtension(new Extension("http://example.org/integerExt", new IntegerType(42)));

    // Add a complex extension with nested extensions
    final Extension complexExt = new Extension("http://example.org/complexExt");
    complexExt.addExtension(new Extension("nestedString", new StringType("nested_value")));
    complexExt.addExtension(new Extension("nestedBoolean", new BooleanType(false)));
    patient.addExtension(complexExt);

    return builder()
        .withResource(patient)
        .group("Extensions with different value types")
        .testEquals(
            "string_value",
            "extension('http://example.org/stringExt').value.ofType(string)",
            "Extension with string value returns correct string value")
        .testTrue(
            "extension('http://example.org/booleanExt').value.ofType(boolean)",
            "Extension with boolean value returns correct boolean value")
        .testEquals(
            42,
            "extension('http://example.org/integerExt').value.ofType(integer)",
            "Extension with integer value returns correct integer value")
        .group("Complex extensions with nested extensions")
        .testEquals(
            "nested_value",
            "extension('http://example.org/complexExt').extension('nestedString').value.ofType(string)",
            "Nested extension within complex extension returns correct string value")
        .testFalse(
            "extension('http://example.org/complexExt').extension('nestedBoolean').value.ofType(boolean)",
            "Nested extension within complex extension returns correct boolean value")
        .testEmpty(
            "extension('http://example.org/complexExt').extension('nonExistentNested')",
            "Non-existent nested extension returns empty collection")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testExtensionOnComplexElements() {
    final Patient patient = new Patient();

    // Add extensions to different complex elements
    final HumanName name = new HumanName().setFamily("Smith").addGiven("John");
    name.addExtension(
        new Extension("http://example.org/nameExt", new StringType("name_ext_value")));
    patient.addName(name);

    final Address address = new Address().setCity("Sydney").setCountry("Australia");
    address.addExtension(
        new Extension("http://example.org/addressExt", new StringType("address_ext_value")));
    patient.addAddress(address);

    final ContactPoint telecom = new ContactPoint().setValue("+61123456789");
    telecom.addExtension(
        new Extension("http://example.org/telecomExt", new StringType("telecom_ext_value")));
    patient.addTelecom(telecom);

    // Add extension to a CodeableConcept
    final CodeableConcept maritalStatus = new CodeableConcept().setText("Married");
    maritalStatus.addExtension(
        new Extension("http://example.org/maritalExt", new StringType("marital_ext_value")));
    patient.setMaritalStatus(maritalStatus);

    // Add extension to a Reference
    final Reference managingOrg = new Reference("Organization/123");
    managingOrg.addExtension(
        new Extension("http://example.org/refExt", new StringType("ref_ext_value")));
    patient.setManagingOrganization(managingOrg);

    return builder()
        .withResource(patient)
        .group("Extensions on complex elements")
        .testEquals(
            "name_ext_value",
            "name.extension('http://example.org/nameExt').value.ofType(string)",
            "Extension on HumanName returns correct value")
        .testEquals(
            "address_ext_value",
            "address.extension('http://example.org/addressExt').value.ofType(string)",
            "Extension on Address returns correct value")
        .testEquals(
            "telecom_ext_value",
            "telecom.extension('http://example.org/telecomExt').value.ofType(string)",
            "Extension on ContactPoint returns correct value")
        .testEquals(
            "marital_ext_value",
            "maritalStatus.extension('http://example.org/maritalExt').value.ofType(string)",
            "Extension on CodeableConcept returns correct value")
        .testEquals(
            "ref_ext_value",
            "managingOrganization.extension('http://example.org/refExt').value.ofType(string)",
            "Extension on Reference returns correct value")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testExtensionWithMultipleOccurrences() {
    final Patient patient = new Patient();

    // Add multiple extensions with the same URL
    patient.addExtension(new Extension("http://example.org/multiExt", new StringType("value1")));
    patient.addExtension(new Extension("http://example.org/multiExt", new StringType("value2")));
    patient.addExtension(new Extension("http://example.org/multiExt", new StringType("value3")));

    // Add extensions with different URLs
    patient.addExtension(new Extension("http://example.org/ext1", new StringType("ext1_value")));
    patient.addExtension(new Extension("http://example.org/ext2", new StringType("ext2_value")));

    // Add extension with a Coding value
    final Coding coding =
        new Coding()
            .setSystem("http://example.org/system")
            .setCode("code1")
            .setDisplay("Display Text");
    patient.addExtension(new Extension("http://example.org/codingExt", coding));

    return builder()
        .withResource(patient)
        .group("Multiple extensions with same URL")
        .testEquals(
            List.of("value1", "value2", "value3"),
            "extension('http://example.org/multiExt').value.ofType(string)",
            "First value of multiple extensions with same URL is one of the expected values")
        .testTrue(
            "extension('http://example.org/multiExt').exists(value.ofType(string) = 'value1')",
            "Multiple extensions with same URL contains expected value1")
        .testTrue(
            "extension('http://example.org/multiExt').exists(value.ofType(string) = 'value2')",
            "Multiple extensions with same URL contains expected value2")
        .testTrue(
            "extension('http://example.org/multiExt').exists(value.ofType(string) = 'value3')",
            "Multiple extensions with same URL contains expected value3")
        .group("Extensions with different URLs")
        .testEquals(
            "ext1_value",
            "extension('http://example.org/ext1').value.ofType(string)",
            "Extension with specific URL returns correct value")
        .testEquals(
            "ext2_value",
            "extension('http://example.org/ext2').value.ofType(string)",
            "Extension with different URL returns correct value")
        .group("Extension with Coding value")
        .testEquals(
            "code1",
            "extension('http://example.org/codingExt').value.ofType(Coding).code",
            "Extension with Coding value returns correct code")
        .build();
  }
}
