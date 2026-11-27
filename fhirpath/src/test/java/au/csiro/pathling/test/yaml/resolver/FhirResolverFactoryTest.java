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
package au.csiro.pathling.test.yaml.resolver;

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.schema.DefinitionCanonicalStructure;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.layout.PofDatasets;
import au.csiro.pathling.utilities.StructureMerge;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests the load of a YAML case whose subject is a FHIR resource (path 1 of R11, FR-021b): the
 * case's resource is read with a defined schema, which is the schema inferred from all of the
 * suite's input resources of its type, merged by name with the case's own inferred schema.
 *
 * <p>Schemas are compared by their catalog strings, which leave out nullability, because the
 * nullability of a defined schema is not kept by a defined read (R15 to R20).
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
class FhirResolverFactoryTest {

  /** A patient with a name, as an input file of the suite. */
  private static final String PATIENT_WITH_NAME =
      """
      {"resourceType":"Patient","id":"p1","gender":"female",
       "name":[{"family":"Smith","given":["Ann"]}]}\
      """;

  /** A patient with contact details, as another input file of the suite. */
  private static final String PATIENT_WITH_TELECOM =
      """
      {"resourceType":"Patient","id":"p2","active":true,
       "telecom":[{"system":"phone","value":"123"}]}\
      """;

  /** An observation, so that the suite holds more than one type. */
  private static final String OBSERVATION =
      """
      {"resourceType":"Observation","id":"o1","status":"final",
       "code":{"text":"weight"},"valueQuantity":{"value":5,"unit":"kg"}}\
      """;

  /** A patient given inline by a case, carrying an element no input file of the suite carries. */
  private static final String INLINE_PATIENT =
      """
      {"resourceType":"Patient","id":"p3","birthDate":"1970-01-01",
       "name":[{"family":"Jones","_family":{"id":"f1"}}]}\
      """;

  /** A patient whose deceasedBoolean no other document of the suite or case carries. */
  private static final String DECEASED_PATIENT =
      """
      {"resourceType":"Patient","id":"p4","deceasedBoolean":true}\
      """;

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  @Nonnull
  private SuiteSchemas suite() {
    return SuiteSchemas.of(List.of(PATIENT_WITH_NAME, PATIENT_WITH_TELECOM, OBSERVATION));
  }

  @Test
  void suiteSchemaIsInferredFromAllOfTheSuitesResourcesOfTheType() {
    final StructType expected = inferred("Patient", PATIENT_WITH_NAME, PATIENT_WITH_TELECOM);

    final StructType schema = suite().schemaFor(runtime(), "Patient");

    assertThat(schema.catalogString()).isEqualTo(expected.catalogString());
  }

  @Test
  void suiteSchemaIsDerivedOncePerSuiteAndType() {
    final SuiteSchemas suite = suite();
    final RuntimeContext rt = runtime();

    final StructType patients = suite.schemaFor(rt, "Patient");
    final StructType observations = suite.schemaFor(rt, "Observation");

    assertThat(suite.schemaFor(rt, "Patient")).isSameAs(patients);
    assertThat(suite.schemaFor(rt, "Observation")).isSameAs(observations);
    assertThat(observations).isNotEqualTo(patients);
  }

  @Test
  void inputFileIsLoadedWithTheSuiteSchemaMergedWithItsOwn() {
    final Dataset<Row> resolved = resolve(PATIENT_WITH_NAME);

    assertThat(resolved.schema().catalogString())
        .isEqualTo(definedSchema(PATIENT_WITH_NAME).catalogString());
  }

  @Test
  void inlineSubjectIsLoadedWithTheSuiteSchemaMergedWithItsOwn() {
    final Dataset<Row> resolved = resolve(INLINE_PATIENT);

    assertThat(resolved.schema().catalogString())
        .isEqualTo(definedSchema(INLINE_PATIENT).catalogString());
  }

  @Test
  void definedSchemaIsAtLeastAsWideAsTheCasesInferredSchema() {
    for (final String json : List.of(PATIENT_WITH_NAME, INLINE_PATIENT)) {
      final Set<String> resolvedPaths = paths(resolve(json).schema());
      assertThat(resolvedPaths).as(json).containsAll(paths(inferred("Patient", json)));
    }
    // The suite widens a case beyond what its own resource carries.
    assertThat(paths(resolve(PATIENT_WITH_NAME).schema())).contains("telecom.value", "active");
  }

  @Test
  void resolvedSchemaCarriesAnElementOnlyTheSuppliedSuiteSchemasCarry() {
    // The suite's other patient is the only document with deceasedBoolean, so the factory can
    // carry it only by loading the case with the suite schemas it is given.
    final SuiteSchemas suite = SuiteSchemas.of(List.of(PATIENT_WITH_NAME, DECEASED_PATIENT));

    final Dataset<Row> resolved =
        FhirResolverFactory.of(PATIENT_WITH_NAME, suite).apply(runtime()).getDataset();

    assertThat(resolved.schema().fieldNames()).contains("deceasedBoolean");
    assertThat(resolved.selectExpr("deceasedBoolean").first().isNullAt(0)).isTrue();
  }

  @Test
  void resolvedDatasetKeepsTheCasesValues() {
    final Dataset<Row> resolved = resolve(INLINE_PATIENT);

    assertThat(resolved.count()).isEqualTo(1);
    final Row row =
        resolved
            .selectExpr("id", "birthDate", "name[0].family", "name[0]._family.id", "active")
            .first();
    assertThat(row.getString(0)).isEqualTo("p3");
    assertThat(row.getString(1)).isEqualTo("1970-01-01");
    assertThat(row.getString(2)).isEqualTo("Jones");
    assertThat(row.getString(3)).isEqualTo("f1");
    // An element only the suite carries is present and empty.
    assertThat(row.isNullAt(4)).isTrue();
  }

  @Nonnull
  private RuntimeContext runtime() {
    return RuntimeContext.of(spark, fhirEncoders);
  }

  @Nonnull
  private Dataset<Row> resolve(@Nonnull final String json) {
    return FhirResolverFactory.of(json, suite()).apply(runtime()).getDataset();
  }

  /** The schema a resource is read with: the suite's, merged by name with the resource's own. */
  @Nonnull
  private StructType definedSchema(@Nonnull final String json) {
    return StructureMerge.merge(
        inferred("Patient", PATIENT_WITH_NAME, PATIENT_WITH_TELECOM),
        inferred("Patient", json),
        DefinitionCanonicalStructure.forResource(
            FhirDefinitionContext.of(fhirEncoders.getContext()), "Patient"));
  }

  /** The schema an inferred read gives a set of resources, as the suites read them today. */
  @Nonnull
  private StructType inferred(@Nonnull final String resourceType, @Nonnull final String... json) {
    return PofDatasets.fromJson(spark, fhirEncoders.getContext(), resourceType, List.of(json))
        .schema();
  }

  /** The dotted paths of every field of a schema, at every depth. */
  @Nonnull
  private static Set<String> paths(@Nonnull final StructType schema) {
    return paths("", schema).collect(Collectors.toSet());
  }

  @Nonnull
  private static Stream<String> paths(@Nonnull final String prefix, @Nonnull final DataType type) {
    DataType element = type;
    while (element instanceof final ArrayType array) {
      element = array.elementType();
    }
    if (!(element instanceof final StructType struct)) {
      return Stream.empty();
    }
    return Stream.of(struct.fields())
        .flatMap(
            field -> {
              final String path = prefix + field.name();
              return Stream.concat(Stream.of(path), paths(path + ".", field.dataType()));
            });
  }
}
