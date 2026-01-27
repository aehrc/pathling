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

import static org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType.REFERENCE;

import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;

/**
 * Tests for SQL on FHIR Join Key functions: - getResourceKey() - getReferenceKey()
 *
 * <p>These functions are required by the SQL on FHIR shareable view profile.
 */
public class JoinKeyFunctionsDslTest extends FhirPathDslTestBase {

  @FhirPathTest
  public Stream<DynamicTest> testGetResourceKey() {
    return builder()
        .withSubject(sb -> sb.string("resourceType", "Patient").string("id", "1"))
        .group("getResourceKey() function")
        .testEquals(
            "Patient/1",
            "getResourceKey()",
            "getResourceKey() returns a non-empty value for a Patient resource")
        .testError(
            "nonResource.getResourceKey()",
            "getResourceKey() throws an error when called on a non-resource element")
        .testError(
            "'string'.getResourceKey()",
            "getResourceKey() throws an error when called on a primitive type")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testGetReferenceKey() {
    return builder()
        .withSubject(
            sb ->
                sb
                    // Define references with proper FHIR Reference type
                    .element(
                        "patientReference",
                        ref -> ref.fhirType(REFERENCE).string("reference", "Patient/patient-123"))
                    .element(
                        "observationReference",
                        ref -> ref.fhirType(REFERENCE).string("reference", "Observation/obs-456"))
                    .element(
                        "emptyReference", ref -> ref.fhirType(REFERENCE).stringEmpty("reference"))
                    // Define a collection of references
                    .elementArray(
                        "multipleReferences",
                        ref1 -> ref1.fhirType(REFERENCE).string("reference", "Patient/patient-123"),
                        ref2 ->
                            ref2.fhirType(REFERENCE).string("reference", "Practitioner/pract-456"))
                    // Define a non-reference element
                    .element("nonReference", elem -> elem.string("value", "Test")))
        .group("getReferenceKey() function with no type parameter")
        .testEquals(
            "Patient/patient-123",
            "patientReference.getReferenceKey()",
            "getReferenceKey() returns the relative reference string for a Patient reference")
        .testEquals(
            "Observation/obs-456",
            "observationReference.getReferenceKey()",
            "getReferenceKey() returns the relative reference string for an Observation reference")
        .testEmpty(
            "emptyReference.getReferenceKey()",
            "getReferenceKey() returns empty for an empty reference")
        .testEquals(
            List.of("Patient/patient-123", "Practitioner/pract-456"),
            "multipleReferences.getReferenceKey()",
            "getReferenceKey() returns the relative reference string for references in a"
                + " collection")
        .group("getReferenceKey() function with type parameter on collection")
        .testEquals(
            List.of("Patient/patient-123"),
            "multipleReferences.getReferenceKey(Patient)",
            "getReferenceKey() with type parameter returns only matching references from a"
                + " collection")
        .testEquals(
            List.of("Practitioner/pract-456"),
            "multipleReferences.getReferenceKey(Practitioner)",
            "getReferenceKey() with type parameter returns only matching references from a"
                + " collection")
        .testEmpty(
            "multipleReferences.getReferenceKey(Observation)",
            "getReferenceKey() with non-matching type returns empty for a collection of references")
        .group("getReferenceKey() function with type parameter on single reference")
        .testEquals(
            "Patient/patient-123",
            "patientReference.getReferenceKey(Patient)",
            "getReferenceKey() with matching type returns the relative reference string")
        .testEmpty(
            "patientReference.getReferenceKey(Observation)",
            "getReferenceKey() with non-matching type returns empty")
        .testEquals(
            "Observation/obs-456",
            "observationReference.getReferenceKey(Observation)",
            "getReferenceKey() with matching type returns the relative reference string for"
                + " Observation")
        .testEmpty(
            "observationReference.getReferenceKey(Patient)",
            "getReferenceKey() with non-matching type returns empty for Observation")
        .group("getReferenceKey() function error cases")
        .testError(
            "nonReference.getReferenceKey()",
            "getReferenceKey() throws an error when called on a non-reference element")
        .testError(
            "'string'.getReferenceKey()",
            "getReferenceKey() throws an error when called on a primitive type")
        .testError(
            "patientReference.getReferenceKey(Patient, 'extra')",
            "getReferenceKey() throws an error when called with more than one parameter")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testResourceKeyMatchesReferenceKeyWithVersionedId() {
    // This test demonstrates issue #2519: when a resource has a versioned ID in id_versioned,
    // getResourceKey() should return an unversioned key that matches getReferenceKey().
    // References typically don't include version info, so the keys must match for joining.
    return builder()
        .withSubject(
            sb ->
                sb.string("resourceType", "Patient")
                    .string("id", "patient-123")
                    // Simulate a resource that was encoded with versioned IdType - this populates
                    // id_versioned with the full versioned reference format.
                    .string("id_versioned", "Patient/patient-123/_history/1")
                    .element(
                        "selfReference",
                        ref -> ref.fhirType(REFERENCE).string("reference", "Patient/patient-123")))
        .group("getResourceKey() and getReferenceKey() matching with versioned IDs")
        .testEquals(
            "Patient/patient-123",
            "getResourceKey()",
            "getResourceKey() should return unversioned key (not id_versioned) to match reference"
                + " format")
        .testEquals(
            "Patient/patient-123",
            "selfReference.getReferenceKey()",
            "getReferenceKey() returns unversioned reference")
        .build();
  }
}
