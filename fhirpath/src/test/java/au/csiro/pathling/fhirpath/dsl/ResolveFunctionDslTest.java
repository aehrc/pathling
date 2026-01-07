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

import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathModelBuilder;
import au.csiro.pathling.test.dsl.FhirPathTest;
import java.util.List;
import java.util.stream.Stream;
import org.hl7.fhir.r4.model.Appointment;
import org.hl7.fhir.r4.model.Appointment.AppointmentParticipantComponent;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Reference;
import org.junit.jupiter.api.DynamicTest;

/**
 * Tests for the limited {@code resolve()} function implementation.
 *
 * <p>The {@code resolve()} function returns type information for references, supporting type
 * checking with the {@code is} operator. It does not support field traversal.
 *
 * <p>Type extraction behaviour:
 *
 * <ul>
 *   <li>If {@code Reference.type} field is present, it is used regardless of reference format
 *       (including contained and logical references)
 *   <li>If {@code Reference.type} is absent, type is parsed from {@code Reference.reference} for
 *       relative, absolute, and canonical reference formats
 *   <li>The {@code type} field always takes precedence over the parsed type
 * </ul>
 *
 * @author Piotr Szul
 * @see <a href="https://github.com/aehrc/pathling/issues/2522">Issue #2522</a>
 */
public class ResolveFunctionDslTest extends FhirPathDslTestBase {

  @FhirPathTest
  public Stream<DynamicTest> testBasicTypeExtraction() {
    return builder()
        .withSubject(
            sb ->
                sb
                    // Reference with explicit type field
                    .element(
                        "patientRef",
                        ref ->
                            ref.fhirReference()
                                .string("reference", "Patient/123")
                                .string("type", "Patient"))

                    // Reference with only reference string (relative)
                    .element(
                        "locationRef",
                        ref -> ref.fhirReference().string("reference", "Location/room-1"))

                    // Reference with absolute URL
                    .element(
                        "orgRef",
                        ref ->
                            ref.fhirReference()
                                .string("reference", "http://example.org/fhir/Organization/org-1"))

                    // Reference with canonical URL
                    .element(
                        "vsRef",
                        ref ->
                            ref.fhirReference()
                                .string("reference", "http://hl7.org/fhir/ValueSet/my-valueset"))

                    // Reference with canonical URL with version
                    .element(
                        "csRef",
                        ref ->
                            ref.fhirReference()
                                .string("reference", "http://hl7.org/fhir/CodeSystem/example|1.0")))
        .group("Basic type extraction - explicit type field")
        .testTrue(
            "patientRef.resolve() is Patient",
            "Reference with explicit type field resolves to Patient")
        .testFalse(
            "patientRef.resolve() is Organization",
            "Reference with Patient type does not resolve to Organization")
        .group("Basic type extraction - relative reference")
        .testTrue(
            "locationRef.resolve() is Location",
            "Reference with relative reference resolves to Location")
        .testFalse(
            "locationRef.resolve() is Patient",
            "Reference with Location type does not resolve to Patient")
        .group("Basic type extraction - absolute URL")
        .testTrue(
            "orgRef.resolve() is Organization",
            "Reference with absolute URL resolves to Organization")
        .testFalse(
            "orgRef.resolve() is Patient",
            "Reference with Organization type does not resolve to Patient")
        .group("Basic type extraction - canonical URL")
        .testTrue(
            "vsRef.resolve() is ValueSet", "Reference with canonical URL resolves to ValueSet")
        .testTrue(
            "csRef.resolve() is CodeSystem",
            "Reference with canonical URL and version resolves to CodeSystem")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testTypePriority() {
    return builder()
        .withSubject(
            sb ->
                sb
                    // Type field takes precedence over reference string
                    .element(
                        "conflictRef",
                        ref ->
                            ref.fhirReference()
                                .string("reference", "Patient/123")
                                .string("type", "Practitioner"))

                    // Type field present, no reference string
                    .element(
                        "typeOnlyRef", ref -> ref.fhirReference().string("type", "Organization"))

                    // Reference string only, no type field
                    .element(
                        "refOnlyRef",
                        ref -> ref.fhirReference().string("reference", "Encounter/456"))
                    .element(
                        "containedWithType",
                        ref ->
                            ref.fhirReference()
                                .string("type", "Observation")
                                .string("reference", "#Test")))
        .group("Type priority - type field takes precedence")
        .testTrue(
            "conflictRef.resolve() is Practitioner",
            "Type field takes precedence over reference string")
        .testFalse(
            "conflictRef.resolve() is Patient",
            "Reference string type is ignored when type field present")
        .testTrue(
            "typeOnlyRef.resolve() is Organization",
            "Reference with only type field resolves correctly")
        .testTrue(
            "refOnlyRef.resolve() is Encounter",
            "Reference with only reference string resolves correctly")
        .testTrue(
            "containedWithType.resolve() is Observation",
            "Reference with contained format and type field resolves correctly")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testEdgeCases() {
    return builder()
        .withSubject(
            sb ->
                sb
                    // Contained reference (starts with #) WITHOUT type field
                    .element(
                        "containedRef",
                        ref -> ref.fhirReference().string("reference", "#patient-1"))

                    // Logical reference (identifier-only) WITH type field
                    .element(
                        "logicalWithType", ref -> ref.fhirReference().string("type", "Patient"))

                    // Logical reference (identifier-only) WITHOUT type field and no reference
                    .element("logicalNoType", FhirPathModelBuilder::fhirReference)

                    // Empty reference string
                    .element("emptyRef", FhirPathModelBuilder::fhirReference)

                    // Malformed reference
                    .element(
                        "badRef",
                        ref -> ref.fhirReference().string("reference", "not-a-valid-reference"))

                    // Reference with just an ID (no resource type)
                    .element("idOnlyRef", ref -> ref.fhirReference().string("reference", "12345"))

                    // Reference that's just a #
                    .element("hashOnlyRef", ref -> ref.fhirReference().string("reference", "#"))

                    // Reference with lowercase type (invalid)
                    .element(
                        "lowercaseRef",
                        ref -> ref.fhirReference().string("reference", "patient/123")))
        .group("Edge cases - contained reference without type")
        .testEmpty(
            "containedRef.resolve()",
            "Contained reference without type field returns empty collection")
        .group("Edge cases - logical references")
        .testTrue(
            "logicalWithType.resolve() is Patient",
            "Logical reference with type field resolves correctly")
        .testEmpty(
            "logicalNoType.resolve()",
            "Logical reference without type field returns empty collection")
        .group("Edge cases - empty and malformed")
        .testEmpty("emptyRef.resolve()", "Empty reference returns empty collection")
        .testEmpty("badRef.resolve()", "Malformed reference returns empty collection")
        .testEmpty("idOnlyRef.resolve()", "Reference with just ID returns empty collection")
        .testEmpty("hashOnlyRef.resolve()", "Reference with just # returns empty collection")
        .testEmpty(
            "lowercaseRef.resolve()", "Reference with lowercase type returns empty collection")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testTypeCheckingIntegration() {
    return builder()
        .withSubject(
            sb ->
                sb
                    // Multiple references with different types
                    .elementArray(
                        "actors",
                        ref1 ->
                            ref1.fhirReference()
                                .string("display", "ref-patient-1")
                                .string("reference", "Patient/p1")
                                .string("type", "Patient"),
                        ref2 ->
                            ref2.fhirReference()
                                .string("display", "ref-practitioner-1")
                                .string("reference", "Practitioner/pr1")
                                .string("type", "Practitioner"),
                        ref3 ->
                            ref3.fhirReference()
                                .string("display", "ref-location-1")
                                .string("reference", "Location/loc1")
                                .string("type", "Location"))

                    // Single patient reference for simple tests
                    .element(
                        "singlePatient",
                        ref ->
                            ref.fhirReference()
                                .string("reference", "Patient/p1")
                                .string("type", "Patient")))
        .group("Type checking integration - with where()")
        .testEquals(
            List.of("ref-patient-1"),
            "actors.where(resolve() is Patient).display",
            "Can filter references by type using where() and resolve()")
        .testEquals(
            List.of("ref-practitioner-1"),
            "actors.where(resolve() is Practitioner).display",
            "Can filter for Practitioner references")
        .testEquals(
            List.of("ref-location-1"),
            "actors.where(resolve() is Location).display",
            "Can filter for Location references")
        .testEmpty(
            "actors.where(resolve() is Organization).display",
            "Filtering for non-existent type returns no results")
        .group("Type checking integration - with exists()")
        .testTrue(
            "singlePatient.resolve() is Patient.exists()",
            "Can use exists() with resolve() and is operator")
        .group("Type checking integration - negative type check")
        .testFalse("singlePatient.resolve() is Organization", "Negative type check returns false")
        .testFalse("singlePatient.resolve() is Practitioner", "Different type check returns false")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testTraversalPrevention() {
    return builder()
        .withSubject(
            sb ->
                sb.element(
                    "patientRef",
                    ref ->
                        ref.fhirReference()
                            .string("reference", "Patient/123")
                            .string("type", "Patient")))
        .group("Traversal prevention - field access not allowed")
        .testError("patientRef.resolve().name", "Traversal after resolve() is not supported")
        .testError("patientRef.resolve().id", "Cannot access id field after resolve()")
        .testError(
            "patientRef.resolve().managingOrganization",
            "Cannot access reference field after resolve()")
        .testError("patientRef.resolve().active", "Cannot access boolean field after resolve()")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testInputValidation() {
    return builder()
        .withSubject(
            sb ->
                sb.string("stringValue", "test")
                    .integer("integerValue", 42)
                    .element(
                        "patientRef",
                        ref -> ref.fhirReference().string("reference", "Patient/123")))
        .group("Input validation - non-reference types")
        .testError("stringValue.resolve()", "resolve() on string throws InvalidUserInputError")
        .testError("integerValue.resolve()", "resolve() on integer throws InvalidUserInputError")
        .group("Input validation - valid reference input")
        .testTrue(
            "patientRef.resolve() is Patient", "resolve() works correctly on valid Reference type")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testQualifiedTypeNames() {
    return builder()
        .withSubject(
            sb ->
                sb.element(
                    "patientRef",
                    ref ->
                        ref.fhirReference()
                            .string("reference", "Patient/123")
                            .string("type", "Patient")))
        .group("Qualified type names - FHIR namespace")
        .testTrue(
            "patientRef.resolve() is FHIR.Patient",
            "Type checking works with FHIR namespace qualifier")
        .group("Qualified type names - unqualified")
        .testTrue(
            "patientRef.resolve() is Patient", "Type checking works with unqualified type name")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testMultipleReferences() {
    return builder()
        .withSubject(
            sb ->
                sb.elementArray(
                    "multipleRefs",
                    ref1 ->
                        ref1.fhirReference()
                            .string("display", "ref-p1")
                            .string("reference", "Patient/p1"),
                    ref2 ->
                        ref2.fhirReference()
                            .string("display", "ref-p2")
                            .string("reference", "Patient/p2"),
                    ref3 ->
                        ref3.fhirReference()
                            .string("display", "ref-pr1")
                            .string("reference", "Practitioner/pr1"),
                    ref ->
                        ref.fhirReference()
                            .string("display", "ref-contained-pr")
                            .string("type", "Practitioner")
                            .string("reference", "#containedPractitioner"),
                    ref4 ->
                        ref4.fhirReference()
                            .string("display", "ref-contained-no-type")
                            .string("reference", "#contained"), // Should be excluded
                    ref5 -> ref5.fhirReference().string("display", "ref-empty")))
        .group("Multiple references - filtering by type")
        .testEquals(
            List.of("ref-p1", "ref-p2"),
            "multipleRefs.where(resolve() is Patient).display",
            "Can find Patient references in collection")
        .testEquals(
            List.of("ref-pr1", "ref-contained-pr"),
            "multipleRefs.where(resolve() is Practitioner).display",
            "Can find Practitioner references in collection")
        .testEquals(
            List.of("ref-p1", "ref-p2", "ref-pr1", "ref-contained-pr"),
            "multipleRefs.where(resolve().exists()).display",
            "Can find all valid references in collection")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testVariousReferenceFormats() {
    return builder()
        .withSubject(
            sb ->
                sb
                    // Different URL formats
                    .element(
                        "relativeRef",
                        ref -> ref.fhirReference().string("reference", "Patient/123"))
                    .element(
                        "absoluteRef",
                        ref ->
                            ref.fhirReference()
                                .string("reference", "http://example.org/fhir/Patient/456"))
                    .element(
                        "absoluteRefHttps",
                        ref ->
                            ref.fhirReference()
                                .string("reference", "https://example.org/fhir/Practitioner/789"))
                    .element(
                        "canonicalSimple",
                        ref ->
                            ref.fhirReference()
                                .string("reference", "http://hl7.org/fhir/ValueSet/example"))
                    .element(
                        "canonicalWithVersion",
                        ref ->
                            ref.fhirReference()
                                .string(
                                    "reference", "http://hl7.org/fhir/CodeSystem/example|2.0.0"))

                    // URN format (should return empty)
                    .element(
                        "urnRef",
                        ref ->
                            ref.fhirReference()
                                .string(
                                    "reference", "urn:uuid:53fefa32-fcbb-4ff8-8a92-55ee120877b7")))
        .group("Various reference formats - relative")
        .testTrue(
            "relativeRef.resolve() is Patient", "Relative reference format resolves correctly")
        .group("Various reference formats - absolute HTTP")
        .testTrue("absoluteRef.resolve() is Patient", "Absolute HTTP reference resolves correctly")
        .testTrue(
            "absoluteRefHttps.resolve() is Practitioner",
            "Absolute HTTPS reference resolves correctly")
        .group("Various reference formats - canonical")
        .testTrue("canonicalSimple.resolve() is ValueSet", "Canonical URL resolves correctly")
        .testTrue(
            "canonicalWithVersion.resolve() is CodeSystem",
            "Canonical URL with version resolves correctly")
        .group("Various reference formats - URN (unsupported)")
        .testEmpty("urnRef.resolve()", "URN reference returns empty collection")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testResolveCollections() {
    return builder()
        .withSubject(
            sb ->
                sb
                    // Collection with multiple references of same type
                    .elementArray(
                        "patientRefs",
                        ref1 ->
                            ref1.fhirReference()
                                .string("reference", "Patient/p1")
                                .string("type", "Patient"),
                        ref2 ->
                            ref2.fhirReference()
                                .string("reference", "Patient/p2")
                                .string("type", "Patient"),
                        ref3 ->
                            ref3.fhirReference()
                                .string("reference", "Patient/p3")
                                .string("type", "Patient"))

                    // Collection with mixed types
                    .elementArray(
                        "mixedRefs",
                        ref1 ->
                            ref1.fhirReference()
                                .string("display", "mixed-p1")
                                .string("reference", "Patient/p1")
                                .string("type", "Patient"),
                        ref2 ->
                            ref2.fhirReference()
                                .string("display", "mixed-pr1")
                                .string("reference", "Practitioner/pr1")
                                .string("type", "Practitioner"),
                        ref3 ->
                            ref3.fhirReference()
                                .string("display", "mixed-loc1")
                                .string("reference", "Location/loc1")
                                .string("type", "Location"),
                        ref4 ->
                            ref4.fhirReference()
                                .string("display", "mixed-p2")
                                .string("reference", "Patient/p2")
                                .string("type", "Patient"))

                    // Collection with some invalid references
                    .elementArray(
                        "mixedValidInvalid",
                        ref1 ->
                            ref1.fhirReference()
                                .string("display", "valid-p1")
                                .string("reference", "Patient/p1")
                                .string("type", "Patient"),
                        ref2 ->
                            ref2.fhirReference()
                                .string("display", "invalid-contained")
                                .string("reference", "#contained"), // No type - should not resolve
                        ref3 ->
                            ref3.fhirReference()
                                .string("display", "valid-pr1")
                                .string("reference", "Practitioner/pr1")
                                .string("type", "Practitioner"),
                        ref4 ->
                            ref4.fhirReference()
                                .string(
                                    "display",
                                    "invalid-empty") // Empty reference and type - should not
                                                     // resolve
                        ))
        .group("Resolve collections - all same type")
        .testTrue(
            "patientRefs.resolve().first() is Patient", "First resolved reference is Patient type")
        .testTrue(
            "patientRefs.resolve()[0] is Patient",
            "First resolved reference (by index) is Patient type")
        .testTrue(
            "patientRefs.resolve()[1] is Patient",
            "Second resolved reference (by index) is Patient type")
        .testTrue(
            "patientRefs.resolve()[2] is Patient",
            "Third resolved reference (by index) is Patient type")
        .group("Resolve collections - mixed types with ofType()")
        .testTrue(
            "mixedRefs.resolve().ofType(Patient).exists()",
            "Can filter resolved references to Patient type")
        .testTrue(
            "mixedRefs.resolve().ofType(Practitioner).exists()",
            "Can filter resolved references to Practitioner type")
        .testTrue(
            "mixedRefs.resolve().ofType(Location).exists()",
            "Can filter resolved references to Location type")
        .testFalse(
            "mixedRefs.resolve().ofType(Organization).exists()",
            "Filtering to non-existent type returns no results")
        .group("Resolve collections - first() and is/as operators")
        .testTrue("mixedRefs.resolve().first() is Patient", "First resolved reference is Patient")
        .testTrue(
            "mixedRefs.resolve()[1] is Practitioner", "Second resolved reference is Practitioner")
        .testTrue("mixedRefs.resolve()[2] is Location", "Third resolved reference is Location")
        .group("Resolve collections - counting and filtering")
        .testEquals(
            3, "patientRefs.resolve().count()", "Resolving 3 Patient references returns 3 items")
        .testEquals(
            4, "mixedRefs.resolve().count()", "Resolving 4 mixed references returns 4 items")
        .testEquals(
            List.of("mixed-p1", "mixed-p2"),
            "mixedRefs.where(resolve() is Patient).display",
            "Filtering mixed refs by Patient type returns correct displays")
        .testEquals(
            List.of("mixed-pr1"),
            "mixedRefs.where(resolve() is Practitioner).display",
            "Filtering mixed refs by Practitioner type returns correct display")
        .group("Resolve collections - with invalid references")
        .testEquals(
            2, "mixedValidInvalid.resolve().count()", "Invalid references are excluded from count")
        .testEquals(
            1,
            "mixedValidInvalid.resolve().ofType(Patient).count()",
            "One Patient reference after filtering out invalid refs")
        .testEquals(
            1,
            "mixedValidInvalid.resolve().ofType(Practitioner).count()",
            "One Practitioner reference after filtering out invalid refs")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testResolveWithHapiResources() {
    // Create a Patient with a managingOrganization reference
    final Patient patient = new Patient();
    patient.setId("patient-1");
    patient.setActive(true);

    // Set managingOrganization reference
    final Reference orgRef = new Reference();
    orgRef.setReference("Organization/org-123");
    orgRef.setType("Organization");
    patient.setManagingOrganization(orgRef);

    // Set generalPractitioner references (collection)
    final Reference gp1 = patient.addGeneralPractitioner();
    gp1.setDisplay("gp1-ref");
    gp1.setReference("Practitioner/gp1");
    gp1.setType("Practitioner");

    final Reference gp2 = patient.addGeneralPractitioner();
    gp2.setDisplay("gp2-ref");
    gp2.setReference("Practitioner/gp2");
    gp2.setType("Practitioner");

    final Reference clinic = patient.addGeneralPractitioner();
    clinic.setDisplay("clinic-ref");
    clinic.setReference("Organization/clinic1");
    clinic.setType("Organization");

    return builder()
        .withResource(patient)
        .group("HAPI resource - singular reference")
        .testTrue(
            "Patient.managingOrganization.resolve() is Organization",
            "Singular reference resolves to Organization")
        .testFalse(
            "Patient.managingOrganization.resolve() is Practitioner",
            "Singular reference is not Practitioner")
        .group("HAPI resource - collection of references")
        .testEquals(
            3,
            "Patient.generalPractitioner.resolve().count()",
            "Resolving collection of 3 references returns 3 items")
        .testEquals(
            2,
            "Patient.generalPractitioner.resolve().ofType(Practitioner).count()",
            "Two Practitioner references in collection")
        .testEquals(
            1,
            "Patient.generalPractitioner.resolve().ofType(Organization).count()",
            "One Organization reference in collection")
        .group("HAPI resource - filtering with where() and resolve()")
        .testEquals(
            List.of("gp1-ref", "gp2-ref"),
            "Patient.generalPractitioner.where(resolve() is Practitioner).display",
            "Can filter references by resolved type")
        .testEquals(
            List.of("clinic-ref"),
            "Patient.generalPractitioner.where(resolve() is Organization).display",
            "Can find Organization reference in collection")
        .group("HAPI resource - first() with resolve()")
        .testTrue(
            "Patient.generalPractitioner.resolve().first() is Practitioner",
            "First resolved reference is Practitioner")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testResolveWithComplexHapiResources() {
    // Create an Appointment with multiple participants
    final Appointment appointment = new Appointment();
    appointment.setId("appt-1");

    // Add participants with different actor types
    final AppointmentParticipantComponent participant1 = appointment.addParticipant();
    final Reference actor1 = participant1.getActor();
    actor1.setDisplay("actor-p1");
    actor1.setReference("Patient/patient-1");
    actor1.setType("Patient");

    final AppointmentParticipantComponent participant2 = appointment.addParticipant();
    final Reference actor2 = participant2.getActor();
    actor2.setDisplay("actor-pr1");
    actor2.setReference("Practitioner/doctor-1");
    actor2.setType("Practitioner");

    final AppointmentParticipantComponent participant3 = appointment.addParticipant();
    final Reference actor3 = participant3.getActor();
    actor3.setDisplay("actor-loc1");
    actor3.setReference("Location/room-5");
    actor3.setType("Location");

    final AppointmentParticipantComponent participant4 = appointment.addParticipant();
    final Reference actor4 = participant4.getActor();
    actor4.setDisplay("actor-p2");
    actor4.setReference("Patient/patient-2");
    actor4.setType("Patient");

    // Add a participant with a contained reference (should be excluded)
    final AppointmentParticipantComponent participant5 = appointment.addParticipant();
    final Reference actor5 = participant5.getActor();
    actor5.setDisplay("actor-contained");
    actor5.setReference("#contained-practitioner");

    return builder()
        .withResource(appointment)
        .group("Complex HAPI resource - nested references")
        .testEquals(
            4,
            "Appointment.participant.actor.resolve().count()",
            "Resolving nested references excludes contained reference")
        .testEquals(
            2,
            "Appointment.participant.actor.resolve().ofType(Patient).count()",
            "Two Patient actors in appointment")
        .testEquals(
            1,
            "Appointment.participant.actor.resolve().ofType(Practitioner).count()",
            "One Practitioner actor in appointment")
        .testEquals(
            1,
            "Appointment.participant.actor.resolve().ofType(Location).count()",
            "One Location actor in appointment")
        .group("Complex HAPI resource - where() with resolve() is Type")
        .testEquals(
            List.of("actor-p1", "actor-p2"),
            "Appointment.participant.actor.where(resolve() is Patient).display",
            "Filter to Patient actors using resolve() is")
        .testEquals(
            List.of("actor-loc1"),
            "Appointment.participant.actor.where(resolve() is Location).display",
            "Can find Location actor")
        .testEmpty(
            "Appointment.participant.actor.where(resolve() is Organization).display",
            "No Organization actors present")
        .group("Complex HAPI resource - combining resolve() with other operators")
        .testEquals(
            List.of("actor-p1", "actor-pr1", "actor-p2"),
            "Appointment.participant.actor.where(resolve() is Patient or resolve() is Practitioner).display",
            "Can combine type checks with or")
        .testEquals(
            List.of("actor-loc1"),
            "Appointment.participant.where(actor.resolve() is Location).actor.display",
            "Can use resolve() in nested where()")
        .build();
  }

  @FhirPathTest
  public Stream<DynamicTest> testResolveWithAsOperator() {
    return builder()
        .withSubject(
            sb ->
                sb.element(
                        "patientRef",
                        ref ->
                            ref.fhirReference()
                                .string("reference", "Patient/123")
                                .string("type", "Patient"))
                    .element(
                        "practitionerRef",
                        ref ->
                            ref.fhirReference()
                                .string("reference", "Practitioner/456")
                                .string("type", "Practitioner"))
                    .elementArray(
                        "mixedRefs",
                        ref1 ->
                            ref1.fhirReference()
                                .string("reference", "Patient/p1")
                                .string("type", "Patient"),
                        ref2 ->
                            ref2.fhirReference()
                                .string("reference", "Practitioner/pr1")
                                .string("type", "Practitioner")))
        .group("resolve() with as operator - singular references")
        .testTrue(
            "patientRef.resolve().as(Patient).exists()",
            "Can use as() operator after resolve() for matching type")
        .testEmpty(
            "patientRef.resolve().as(Practitioner)", "as() returns empty for non-matching type")
        .testEmpty(
            "practitionerRef.resolve().as(Patient)", "as() returns empty when type doesn't match")
        .group("resolve() with as operator - collections")
        .testTrue(
            "mixedRefs.resolve().first().as(Patient).exists()",
            "Can use as() on first() of resolved collection")
        .testTrue(
            "mixedRefs.resolve()[1].as(Practitioner).exists()",
            "Can use as() on second item of resolved collection")
        .build();
  }
}
