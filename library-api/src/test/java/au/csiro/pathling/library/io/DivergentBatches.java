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

package au.csiro.pathling.library.io;

import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.io.FhirReader;
import au.csiro.pathling.library.PathlingContext;
import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.StructType;

/**
 * Two batches of {@code Patient} resources whose fitted schemas diverge in both directions, at the
 * top level and inside a repeating element (US5).
 *
 * <p>The first batch populates {@code gender} and {@code name.family}; the second populates {@code
 * birthDate} and {@code name.given}. Each batch therefore carries columns the other lacks, so a
 * reader that takes its schema from one file loses columns whichever file it happens to list first,
 * rather than only when the listing order is unlucky.
 *
 * <p>The batches are written in the new layout through {@code io}'s own entry point, because the
 * public API does not write that layout until it switches (Phase 12).
 */
public final class DivergentBatches {

  /** The resource type of every resource in both batches. */
  public static final String RESOURCE_TYPE = "Patient";

  private static final List<String> FIRST =
      List.of(
          """
          {"resourceType":"Patient","id":"p1","gender":"male",
           "name":[{"family":"Alpha"},{"family":"Beta"}]}\
          """,
          """
          {"resourceType":"Patient","id":"p2","gender":"female","name":[{"family":"Gamma"}]}\
          """);

  private static final List<String> SECOND =
      List.of(
          """
          {"resourceType":"Patient","id":"p3","birthDate":"1990-01-01",
           "name":[{"given":["Delta"]}]}\
          """,
          """
          {"resourceType":"Patient","id":"p4","birthDate":"1985-06-15",
           "name":[{"given":["Epsilon","Zeta"]},{"given":["Eta"]}]}\
          """);

  /**
   * An update to a resource of the first batch, together with a resource neither batch carries, in
   * the second batch's shape. Merged over the first batch, it replaces {@code p1} with a resource
   * that has no gender and no family.
   */
  private static final List<String> SECOND_UPDATING_FIRST =
      List.of(
          """
          {"resourceType":"Patient","id":"p1","birthDate":"1970-03-03",
           "name":[{"given":["Updated"]}]}\
          """,
          """
          {"resourceType":"Patient","id":"p3","birthDate":"1990-01-01",
           "name":[{"given":["Delta"]}]}\
          """);

  private DivergentBatches() {}

  /**
   * Returns the first batch, in the new layout.
   *
   * @param context the Pathling context
   * @return the first batch
   */
  @Nonnull
  public static Dataset<Row> first(@Nonnull final PathlingContext context) {
    return toLayout(context, FIRST);
  }

  /**
   * Returns the second batch, in the new layout.
   *
   * @param context the Pathling context
   * @return the second batch
   */
  @Nonnull
  public static Dataset<Row> second(@Nonnull final PathlingContext context) {
    return toLayout(context, SECOND);
  }

  /**
   * Returns a batch in the second batch's shape that updates {@code p1} and adds {@code p3}.
   *
   * @param context the Pathling context
   * @return the updating batch
   */
  @Nonnull
  public static Dataset<Row> secondUpdatingFirst(@Nonnull final PathlingContext context) {
    return toLayout(context, SECOND_UPDATING_FIRST);
  }

  /**
   * Writes both batches into one directory of Parquet files, as two appends of one file each, so
   * that the directory holds two files with divergent schemas. The directory is named for the
   * resource type, as the file-based sources expect.
   *
   * @param context the Pathling context
   * @param warehouse the directory to create the resource directory in
   * @return the path of the resource directory
   */
  @Nonnull
  public static String writeParquet(
      @Nonnull final PathlingContext context, @Nonnull final String warehouse) {
    final String table = warehouse + "/" + RESOURCE_TYPE + ".parquet";
    first(context).coalesce(1).write().mode(SaveMode.Overwrite).parquet(table);
    second(context).coalesce(1).write().mode(SaveMode.Append).parquet(table);
    return table;
  }

  /**
   * Returns the field names of the struct inside the {@code name} array of a schema.
   *
   * @param schema a {@code Patient} schema
   * @return the field names of the name element
   */
  @Nonnull
  public static List<String> nameFields(@Nonnull final StructType schema) {
    final StructType name =
        (StructType) ((ArrayType) schema.apply("name").dataType()).elementType();
    return Arrays.asList(name.fieldNames());
  }

  /**
   * Collects one string column of a dataset by resource id, with null where the resource has no
   * value.
   *
   * @param dataset the dataset, which must have an {@code id} column
   * @param column the column to collect
   * @return the values by id
   */
  @Nonnull
  public static Map<String, String> byId(
      @Nonnull final Dataset<Row> dataset, @Nonnull final String column) {
    final Map<String, String> values = new HashMap<>();
    dataset
        .selectExpr("id", "CAST(" + column + " AS STRING)")
        .collectAsList()
        .forEach(row -> values.put(row.getString(0), row.getString(1)));
    return values;
  }

  /**
   * Returns one entry per name of every resource, as the resource id, the family and the given
   * names separated by colons, sorted so that two collections can be compared without regard to row
   * order.
   *
   * @param patients the resources
   * @return the names, sorted
   */
  @Nonnull
  public static List<String> names(@Nonnull final Dataset<Row> patients) {
    return patients
        .selectExpr("id", "explode(name) AS n")
        .selectExpr(
            "concat(id, ':', coalesce(n.family, ''), ':',"
                + " coalesce(array_join(n.given, ' '), ''))")
        .as(Encoders.STRING())
        .collectAsList()
        .stream()
        .sorted()
        .toList();
  }

  @Nonnull
  private static Dataset<Row> toLayout(
      @Nonnull final PathlingContext context, @Nonnull final List<String> json) {
    return FhirReader.of(context.getSpark(), FhirDefinitionContext.of(context.getFhirContext()))
        .json()
        .read(RESOURCE_TYPE, context.getSpark().createDataset(json, Encoders.STRING()));
  }
}
