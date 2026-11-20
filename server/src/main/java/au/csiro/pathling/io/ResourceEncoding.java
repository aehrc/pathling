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

package au.csiro.pathling.io;

import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.library.PathlingContext;
import ca.uhn.fhir.parser.IParser;
import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.instance.model.api.IBaseResource;

/**
 * Moves resources between HAPI objects and datasets in the Parquet on FHIR layout, through FHIR
 * JSON.
 *
 * <p>The server holds resources as HAPI objects where it receives or returns them one at a time,
 * and as datasets in the Parquet on FHIR layout where it stores and queries them. The library
 * converts between that layout and FHIR JSON, so a resource crosses between the two as JSON.
 *
 * <pre>{@code
 * Dataset<Row> rows = ResourceEncoding.encode(context, "Patient", List.of(patient));
 * List<IBaseResource> resources = ResourceEncoding.decode(context, "Patient", rows);
 * }</pre>
 *
 * @author John Grimes
 */
public final class ResourceEncoding {

  private static final String FHIR_JSON = "application/fhir+json";

  private static final String ID = "id";

  private ResourceEncoding() {}

  /**
   * Encodes resources of one type as a dataset in the Parquet on FHIR layout.
   *
   * @param context the Pathling context
   * @param resourceType the type of the resources
   * @param resources the resources, all of the given type
   * @return a dataset with one row per resource
   */
  @Nonnull
  public static Dataset<Row> encode(
      @Nonnull final PathlingContext context,
      @Nonnull final String resourceType,
      @Nonnull final List<? extends IBaseResource> resources) {
    // HAPI parsers are not thread-safe, so each call uses its own.
    final IParser parser = context.getFhirContext().newJsonParser();
    final List<String> documents = resources.stream().map(parser::encodeResourceToString).toList();
    return context.encode(
        context.getSpark().createDataset(documents, Encoders.STRING()).toDF(), resourceType);
  }

  /**
   * Decodes a dataset of resources in the Parquet on FHIR layout into HAPI objects, collecting
   * every row to the driver.
   *
   * @param context the Pathling context
   * @param resourceType the type of the resources
   * @param rows the dataset
   * @return the resources, in the order of the rows
   */
  @Nonnull
  public static List<IBaseResource> decode(
      @Nonnull final PathlingContext context,
      @Nonnull final String resourceType,
      @Nonnull final Dataset<Row> rows) {
    final List<String> documents = context.decode(rows, resourceType, FHIR_JSON).collectAsList();
    final IParser parser = context.getFhirContext().newJsonParser();
    return documents.stream()
        .map(document -> (IBaseResource) parser.parseResource(document))
        .toList();
  }

  /**
   * Returns an empty dataset of a resource type in the Parquet on FHIR layout, for a type that has
   * no stored data.
   *
   * <p>The layout fits its schema to the data, so encoding no resources yields no element columns.
   * The id column is added, because the server selects and filters on it for every type.
   *
   * @param context the Pathling context
   * @param resourceType the resource type
   * @return a dataset with no rows
   */
  @Nonnull
  public static Dataset<Row> empty(
      @Nonnull final PathlingContext context, @Nonnull final String resourceType) {
    final Dataset<Row> encoded = encode(context, resourceType, List.of());
    return Arrays.asList(encoded.columns()).contains(ID)
        ? encoded
        : encoded.withColumn(ID, lit(null).cast(DataTypes.StringType));
  }
}
