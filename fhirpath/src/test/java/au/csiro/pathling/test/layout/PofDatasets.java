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

package au.csiro.pathling.test.layout;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.not;

import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.io.FhirReader;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;

/**
 * Builds the dataset for a set of test fixtures in the Parquet on FHIR layout, by reading them as
 * FHIR JSON through the public read path.
 *
 * <p>A fixture built as a HAPI object is serialised to JSON first, so the fluent builders survive
 * unchanged. The schema the read derives is always the fitted one, and the active {@link
 * TestSchemaMode} is checked so that a request for another mode fails rather than being answered
 * with this one.
 *
 * @author Piotr Szul
 */
public final class PofDatasets {

  private PofDatasets() {}

  /**
   * Returns a dataset of resources built as HAPI objects, in the Parquet on FHIR layout.
   *
   * @param spark the Spark session
   * @param fhirContext the FHIR context the resources belong to
   * @param resourceType the type of every resource
   * @param resources the resources
   * @return the resources, in the Parquet on FHIR layout
   */
  @Nonnull
  public static Dataset<Row> fromResources(
      @Nonnull final SparkSession spark,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final String resourceType,
      @Nonnull final List<? extends IBaseResource> resources) {
    // HAPI strips the version from a versioned reference by default, so the parser is told to keep
    // it, and the fixture is stored as it was built.
    final IParser parser = fhirContext.newJsonParser();
    parser.setStripVersionsFromReferences(false);
    final List<String> json = resources.stream().map(parser::encodeResourceToString).toList();
    return fromJson(spark, fhirContext, resourceType, json);
  }

  /**
   * Returns a dataset of resources built as HAPI objects, in the Parquet on FHIR layout, under the
   * schema fitted to them together with a set of fillers, which are then left out.
   *
   * <p>An element only a filler populates is carried by the schema and is null in every resource
   * returned, as it is in a table where other resources populate it. A test can therefore read an
   * element present but unpopulated, as well as absent.
   *
   * @param spark the Spark session
   * @param fhirContext the FHIR context the resources belong to
   * @param resourceType the type of every resource
   * @param resources the resources to return
   * @param fillers resources that populate elements the schema is to carry, whose ids are distinct
   *     from those of the resources to return
   * @return the resources, in the Parquet on FHIR layout, without the fillers
   * @throws IllegalArgumentException where a filler has no id, or shares its id with a resource
   */
  @Nonnull
  public static Dataset<Row> fromResources(
      @Nonnull final SparkSession spark,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final String resourceType,
      @Nonnull final List<? extends IBaseResource> resources,
      @Nonnull final List<? extends IBaseResource> fillers) {
    final Set<String> fillerIds =
        fillers.stream().map(PofDatasets::fillerId).collect(Collectors.toSet());
    resources.stream()
        .map(resource -> resource.getIdElement().getIdPart())
        .filter(fillerIds::contains)
        .findFirst()
        .ifPresent(
            id -> {
              throw new IllegalArgumentException("A filler shares its id with a resource: " + id);
            });
    // A resource without an id is kept, which a bare negated membership test would drop, because
    // it evaluates to null for a null id.
    return fromResources(
            spark,
            fhirContext,
            resourceType,
            Stream.concat(resources.stream(), fillers.stream()).toList())
        .filter(col("id").isNull().or(not(col("id").isin(fillerIds.toArray()))));
  }

  @Nonnull
  private static String fillerId(@Nonnull final IBaseResource filler) {
    final String id = filler.getIdElement().getIdPart();
    if (id == null) {
      throw new IllegalArgumentException("A filler has no id, so it cannot be left out");
    }
    return id;
  }

  /**
   * Returns a dataset of resources given as FHIR JSON documents, in the Parquet on FHIR layout.
   *
   * @param spark the Spark session
   * @param fhirContext the FHIR context the resources belong to
   * @param resourceType the type of every resource
   * @param json the resources, one FHIR JSON document each
   * @return the resources, in the Parquet on FHIR layout
   */
  @Nonnull
  public static Dataset<Row> fromJson(
      @Nonnull final SparkSession spark,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final String resourceType,
      @Nonnull final List<String> json) {
    TestSchemaMode.active();
    return FhirReader.of(spark, FhirDefinitionContext.of(fhirContext))
        .json()
        .read(resourceType, spark.createDataset(json, Encoders.STRING()));
  }
}
