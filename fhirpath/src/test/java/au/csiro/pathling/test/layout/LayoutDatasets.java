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

import static au.csiro.pathling.UnitTestDependencies.fhirContext;
import static au.csiro.pathling.UnitTestDependencies.jsonParser;

import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.io.FhirReader;
import ca.uhn.fhir.parser.IParser;
import jakarta.annotation.Nonnull;
import java.util.List;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.hl7.fhir.instance.model.api.IBaseResource;

/**
 * Builds the dataset for a set of test fixtures in the layout the {@link TestLayout} dimension
 * selects, so that a fixture is written once and read on either layout.
 *
 * <p>On the previous layout a fixture is encoded with {@link FhirEncoders}, exactly as the test
 * data sources have always done. On the new layout it is read as FHIR JSON through the new-layout
 * transform, which derives the stored schema from the definitions and the data (decision 68). A
 * fixture built as a HAPI object is serialised to JSON first, so the fluent builders survive
 * unchanged, and versioned references keep their version as the previous layout's encoder keeps it.
 * The schema the transform derives is always pruned, which is the only schema mode until M6, and
 * the active {@link TestSchemaMode} is checked so that a request for another mode fails rather than
 * being answered with this one.
 *
 * @author Piotr Szul
 */
public final class LayoutDatasets {

  private LayoutDatasets() {}

  /**
   * Returns a dataset of resources built as HAPI objects, in a layout.
   *
   * @param spark the Spark session
   * @param encoders the encoders for the previous layout, which also carry the FHIR context
   * @param layout the layout to build the dataset in
   * @param resourceType the type of every resource
   * @param resources the resources
   * @return the resources, in the layout
   */
  @Nonnull
  public static Dataset<Row> fromResources(
      @Nonnull final SparkSession spark,
      @Nonnull final FhirEncoders encoders,
      @Nonnull final TestLayout layout,
      @Nonnull final String resourceType,
      @Nonnull final List<IBaseResource> resources) {
    TestSchemaMode.active();
    if (layout.isPof()) {
      // HAPI strips the version from a versioned reference by default, which the previous layout's
      // encoder does not, so the parser is told to keep it and the fixture means the same thing
      // on both layouts.
      final IParser parser = encoders.getContext().newJsonParser();
      parser.setStripVersionsFromReferences(false);
      final List<String> json = resources.stream().map(parser::encodeResourceToString).toList();
      return newLayout(spark, encoders, resourceType, json);
    }
    final ExpressionEncoder<IBaseResource> encoder = encoders.of(resourceType);
    return spark.createDataset(resources, encoder).toDF();
  }

  /**
   * Returns a dataset of resources given as FHIR JSON documents, in a layout.
   *
   * @param spark the Spark session
   * @param encoders the encoders for the previous layout, which also carry the FHIR context
   * @param layout the layout to build the dataset in
   * @param resourceType the type of every resource
   * @param json the resources, one FHIR JSON document each
   * @return the resources, in the layout
   */
  @Nonnull
  public static Dataset<Row> fromJson(
      @Nonnull final SparkSession spark,
      @Nonnull final FhirEncoders encoders,
      @Nonnull final TestLayout layout,
      @Nonnull final String resourceType,
      @Nonnull final List<String> json) {
    TestSchemaMode.active();
    if (layout.isPof()) {
      return newLayout(spark, encoders, resourceType, json);
    }
    final Dataset<String> dataset = spark.createDataset(json, Encoders.STRING());
    final ExpressionEncoder<IBaseResource> encoder = encoders.of(resourceType);
    return dataset
        .map(
            (MapFunction<String, IBaseResource>)
                document -> jsonParser(fhirContext()).parseResource(document),
            encoder)
        .toDF();
  }

  @Nonnull
  private static Dataset<Row> newLayout(
      @Nonnull final SparkSession spark,
      @Nonnull final FhirEncoders encoders,
      @Nonnull final String resourceType,
      @Nonnull final List<String> json) {
    return FhirReader.of(spark, FhirDefinitionContext.of(encoders.getContext()))
        .json()
        .read(resourceType, spark.createDataset(json, Encoders.STRING()));
  }
}
