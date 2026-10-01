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

import static org.assertj.core.api.Assertions.assertThat;
import static org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType.REFERENCE;

import au.csiro.pathling.test.datasource.ObjectDataSource;
import au.csiro.pathling.test.dsl.FhirPathDslTestBase;
import au.csiro.pathling.test.dsl.FhirPathTest;
import au.csiro.pathling.test.layout.TestLayout;
import au.csiro.pathling.utilities.Streams;
import au.csiro.pathling.views.FhirView;
import au.csiro.pathling.views.FhirViewExecutor;
import ca.uhn.fhir.parser.IParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Reference;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests for SQL on FHIR Join Key functions: - getResourceKey() - getReferenceKey()
 *
 * <p>These functions are required by the SQL on FHIR shareable view profile.
 *
 * <p>The keys, and the join they feed, are also tested over the reference fixtures in {@code
 * viewTests/references.json} in both layouts, whichever layout is active (T088). The new layout
 * stores no versioned key, so these show that neither key depends on one (FR-033).
 */
public class JoinKeyFunctionsDslTest extends FhirPathDslTestBase {

  private static final String FIXTURE = "/viewTests/references.json";

  // Observations whose references carry no reference string anywhere, so that the new layout's
  // Reference structure has no reference field.
  private static final List<String> LOGICAL_OBSERVATIONS =
      List.of(
          "{\"resourceType\":\"Observation\",\"id\":\"l1\",\"status\":\"final\","
              + "\"code\":{\"text\":\"logical\"},"
              + "\"subject\":{\"type\":\"Patient\",\"identifier\":{\"value\":\"p1\"}},"
              + "\"performer\":[{\"display\":\"someone\"}]}",
          "{\"resourceType\":\"Observation\",\"id\":\"l2\",\"status\":\"final\","
              + "\"code\":{\"text\":\"no references\"}}");

  @Autowired Gson gson;

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

  @FhirPathTest
  public Stream<DynamicTest> testVersionedKeysOverTheActiveLayout() {
    // A versioned reference keeps its version under the current rule (#2797), and a resource
    // whose id carries a version still has an unversioned key. On the previous layout the version
    // is also stored in id_versioned, which the key must not read.
    final Observation observation = new Observation();
    observation.setId("o4");
    observation.setSubject(new Reference("Patient/p2/_history/3"));
    final Patient patient = new Patient();
    patient.setIdElement(new IdType("Patient", "p2", "3"));
    return Stream.concat(
        builder()
            .withResource(observation)
            .group("getReferenceKey() of a versioned reference")
            .testEquals(
                "Patient/p2/_history/3",
                "subject.getReferenceKey()",
                "getReferenceKey() keeps the version of a versioned reference")
            .testEquals(
                "Patient/p2/_history/3",
                "subject.getReferenceKey(Patient)",
                "getReferenceKey() with a matching type keeps the version")
            .testEmpty(
                "subject.getReferenceKey(Practitioner)",
                "getReferenceKey() with a non-matching type is empty for a versioned reference")
            .build(),
        builder()
            .withResource(patient)
            .group("getResourceKey() of a resource with a versioned id")
            .testEquals(
                "Patient/p2",
                "getResourceKey()",
                "getResourceKey() is built from the plain id, not a stored versioned key")
            .build());
  }

  @Test
  void newLayoutStoresNoVersionedKey() throws IOException {
    final List<String> newLayout = versionedFields(dataSource(TestLayout.POF));
    final List<String> previousLayout = versionedFields(dataSource(TestLayout.PREVIOUS));

    // The previous layout stores a versioned key beside each id, which shows that the search
    // finds one where it exists. The new layout stores none, at any depth.
    assertThat(previousLayout).contains("Patient.id_versioned", "Observation.id_versioned");
    assertThat(newLayout).isEmpty();
  }

  @Nonnull
  static Stream<TestLayout> layouts() {
    return Stream.of(TestLayout.PREVIOUS, TestLayout.POF);
  }

  @ParameterizedTest(name = "over the {0} layout")
  @MethodSource("layouts")
  void keysOverEachLayout(@Nonnull final TestLayout layout) throws IOException {
    final FhirViewExecutor executor = executor(dataSource(layout));

    assertThat(rows(executor, resourceKeyView("Patient")))
        .containsExactlyInAnyOrder("p1|Patient/p1", "p2|Patient/p2");
    assertThat(
            rows(
                executor,
                """
                {
                  "resource": "Observation",
                  "select": [
                    {
                      "column": [
                        { "name": "obs_id", "path": "id" },
                        { "name": "key", "path": "subject.getReferenceKey()" },
                        { "name": "patient_key", "path": "subject.getReferenceKey(Patient)" }
                      ]
                    }
                  ]
                }
                """))
        .containsExactlyInAnyOrder(
            "o1|Patient/p1|Patient/p1",
            "o2|Patient/p-missing|Patient/p-missing",
            "o3|Device/d1|null",
            "o4|Patient/p2/_history/3|Patient/p2/_history/3",
            "o5|null|null");
  }

  @ParameterizedTest(name = "over the {0} layout")
  @MethodSource("layouts")
  void joinOverEachLayout(@Nonnull final TestLayout layout) throws IOException {
    final FhirViewExecutor executor = executor(dataSource(layout));
    final Dataset<Row> targets =
        run(executor, resourceKeyView("Patient"))
            .unionByName(run(executor, resourceKeyView("Practitioner")))
            .alias("t");
    final Dataset<Row> references =
        run(
                executor,
                """
                {
                  "resource": "Observation",
                  "select": [
                    { "column": [ { "name": "obs_id", "path": "id" } ] },
                    {
                      "unionAll": [
                        { "column": [ { "name": "ref_key", "path": "subject.getReferenceKey()" } ] },
                        {
                          "forEach": "performer",
                          "column": [ { "name": "ref_key", "path": "getReferenceKey()" } ]
                        }
                      ]
                    }
                  ]
                }
                """)
            .alias("r");

    // The join is over the columns the views output, as a caller would write it. Only the
    // resolvable references find a target: the versioned one keeps its version and joins nothing.
    assertThat(
            references
                .join(targets, references.col("ref_key").equalTo(targets.col("key")))
                .select("r.obs_id", "t.target_id")
                .collectAsList()
                .stream()
                .map(JoinKeyFunctionsDslTest::render))
        .containsExactlyInAnyOrder("o1|p1", "o1|pr1", "o1|p2");
  }

  @ParameterizedTest(name = "over the {0} layout")
  @MethodSource("layouts")
  void referencesWithoutReferenceStringsJoinNothing(@Nonnull final TestLayout layout)
      throws IOException {
    final List<IBaseResource> resources = new ArrayList<>(fixtureResources());
    resources.removeIf(resource -> resource.fhirType().equals("Observation"));
    final IParser parser = fhirEncoders.getContext().newJsonParser();
    LOGICAL_OBSERVATIONS.forEach(json -> resources.add(parser.parseResource(json)));
    final ObjectDataSource dataSource =
        new ObjectDataSource(spark, fhirEncoders, resources, layout);
    final FhirViewExecutor executor = executor(dataSource);

    // On the new layout, no stored Reference carries a reference field.
    if (layout.isPof()) {
      assertThat(fieldPaths(dataSource.read("Observation").schema(), "Observation"))
          .contains("Observation.subject.type", "Observation.performer.display")
          .doesNotContain("Observation.subject.reference", "Observation.performer.reference");
    }
    final Dataset<Row> keys =
        run(
                executor,
                """
                {
                  "resource": "Observation",
                  "select": [
                    {
                      "column": [
                        { "name": "obs_id", "path": "id" },
                        { "name": "ref_key", "path": "subject.getReferenceKey()" },
                        {
                          "name": "performer_keys",
                          "path": "performer.getReferenceKey().count()"
                        }
                      ]
                    }
                  ]
                }
                """)
            .alias("r");
    assertThat(keys.collectAsList().stream().map(JoinKeyFunctionsDslTest::render))
        .containsExactlyInAnyOrder("l1|null|0", "l2|null|0");

    final Dataset<Row> patients = run(executor, resourceKeyView("Patient")).alias("p");
    assertThat(keys.join(patients, keys.col("ref_key").equalTo(patients.col("key"))).count())
        .isZero();
  }

  @Nonnull
  private static String resourceKeyView(@Nonnull final String resourceType) {
    return """
    {
      "resource": "%s",
      "select": [
        {
          "column": [
            { "name": "target_id", "path": "id" },
            { "name": "key", "path": "getResourceKey()" }
          ]
        }
      ]
    }
    """
        .formatted(resourceType);
  }

  @Nonnull
  private ObjectDataSource dataSource(@Nonnull final TestLayout layout) throws IOException {
    return new ObjectDataSource(spark, fhirEncoders, fixtureResources(), layout);
  }

  @Nonnull
  private FhirViewExecutor executor(@Nonnull final ObjectDataSource dataSource) {
    return new FhirViewExecutor(fhirEncoders.getContext(), dataSource);
  }

  @Nonnull
  private Dataset<Row> run(@Nonnull final FhirViewExecutor executor, @Nonnull final String view) {
    return executor.buildQuery(gson.fromJson(view, FhirView.class));
  }

  @Nonnull
  private List<String> rows(@Nonnull final FhirViewExecutor executor, @Nonnull final String view) {
    return run(executor, view).collectAsList().stream()
        .map(JoinKeyFunctionsDslTest::render)
        .toList();
  }

  @Nonnull
  private static String render(@Nonnull final Row row) {
    final List<String> values = new ArrayList<>();
    for (int i = 0; i < row.length(); i++) {
      values.add(String.valueOf(row.get(i)));
    }
    return String.join("|", values);
  }

  @Nonnull
  private static List<String> versionedFields(@Nonnull final ObjectDataSource dataSource) {
    return dataSource.getResourceTypes().stream()
        .flatMap(type -> fieldPaths(dataSource.read(type).schema(), type).stream())
        .filter(path -> path.endsWith("_versioned"))
        .toList();
  }

  @Nonnull
  private static List<String> fieldPaths(@Nonnull final DataType type, @Nonnull final String path) {
    final List<String> paths = new ArrayList<>();
    if (type instanceof final ArrayType array) {
      paths.addAll(fieldPaths(array.elementType(), path));
    } else if (type instanceof final StructType struct) {
      for (final StructField field : struct.fields()) {
        final String fieldPath = path + "." + field.name();
        paths.add(fieldPath);
        paths.addAll(fieldPaths(field.dataType(), fieldPath));
      }
    }
    return paths;
  }

  @Nonnull
  private List<IBaseResource> fixtureResources() throws IOException {
    final IParser parser = fhirEncoders.getContext().newJsonParser();
    try (final InputStream input = getClass().getResourceAsStream(FIXTURE)) {
      final JsonNode fixture = new ObjectMapper().readTree(input);
      return Streams.streamOf(fixture.get("resources").elements())
          .map(resource -> (IBaseResource) parser.parseResource(resource.toString()))
          .toList();
    }
  }
}
