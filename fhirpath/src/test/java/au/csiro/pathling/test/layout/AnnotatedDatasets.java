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

import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluatorBuilder;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.io.Annotator;
import au.csiro.pathling.schema.SchemaConfiguration;
import au.csiro.pathling.test.datasource.PrunedSchemaReader;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.execution.FileSourceScanExec;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import scala.jdk.javaapi.CollectionConverters;

/**
 * Helpers shared by the tests of how the engine reads stored, annotated data: storing fixtures as
 * encoding stores them, evaluating an expression over them, and seeing what the read of a query
 * prunes to.
 */
public final class AnnotatedDatasets {

  private static final Parser PARSER = new Parser();

  private AnnotatedDatasets() {}

  /**
   * Annotates resources as encoding stores them under a configuration, where one is given, and
   * writes them to Parquet and reads them back, so that the engine reads stored files.
   *
   * @param fhirContext the FHIR context the resources belong to
   * @param resourceType the type of the resources
   * @param resources the resources, in the Parquet on FHIR layout
   * @param annotation the configuration to annotate them under, or empty to store them as they are
   * @param path where to write them
   * @return the stored resources, read back
   */
  @Nonnull
  public static Dataset<Row> stored(
      @Nonnull final FhirContext fhirContext,
      @Nonnull final String resourceType,
      @Nonnull final Dataset<Row> resources,
      @Nonnull final Optional<SchemaConfiguration> annotation,
      @Nonnull final String path) {
    final Dataset<Row> dataset =
        annotation
            .map(
                configuration ->
                    Annotator.of(FhirDefinitionContext.of(fhirContext), configuration)
                        .annotate(resourceType, resources))
            .orElse(resources);
    return PrunedSchemaReader.write(dataset, path).read();
  }

  /**
   * Evaluates an expression over a dataset of resources of one type, to the engine's own form of
   * its result, by resource id.
   *
   * @param fhirContext the FHIR context the resources belong to
   * @param resourceType the type of the resources
   * @param data the resources
   * @param expression the expression
   * @return the id and the value of the expression for each resource
   */
  @Nonnull
  public static Dataset<Row> evaluate(
      @Nonnull final FhirContext fhirContext,
      @Nonnull final ResourceType resourceType,
      @Nonnull final Dataset<Row> data,
      @Nonnull final String expression) {
    return DatasetEvaluatorBuilder.create(resourceType, fhirContext)
        .withDataset(data)
        .build()
        .evaluate(PARSER.parse(expression))
        .toIdValueDataset();
  }

  /**
   * Returns the schema the scan of a query reads, which shows what the read prunes to.
   *
   * @param query the query
   * @return the schema of its first file scan
   */
  @Nonnull
  public static String readSchema(@Nonnull final Dataset<Row> query) {
    return CollectionConverters.asJava(query.queryExecution().executedPlan().collectLeaves())
        .stream()
        .filter(FileSourceScanExec.class::isInstance)
        .map(FileSourceScanExec.class::cast)
        .findFirst()
        .orElseThrow()
        .requiredSchema()
        .simpleString();
  }
}
