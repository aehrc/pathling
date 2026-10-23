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

import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * One resource a bundle carries, as a document of its own, beside what each URN it mentions
 * resolves to among the bundle's entries (decision 83, FR-062). It is what the bundle routes of
 * both formats split a bundle into, before the resource is read as any other document of its
 * format.
 *
 * @param document the resource, as a document
 * @param references the relative reference each URN the resource mentions resolves to, where the
 *     bundle has an entry for it
 */
record BundleResource(@Nonnull String document, @Nonnull Map<String, String> references) {

  /** The prefix of a reference that may name another entry of the same bundle. */
  @Nonnull static final String URN = "urn:";

  /** The name of the column holding the document. */
  @Nonnull static final String DOCUMENT_COLUMN = "document";

  /** The name of the column holding the resolutions of the URNs the document mentions. */
  @Nonnull static final String REFERENCES_COLUMN = "references";

  /** The type of a resource as a value of a Spark column. */
  @Nonnull
  static final StructType TYPE =
      new StructType()
          .add(DOCUMENT_COLUMN, DataTypes.StringType, false)
          .add(
              REFERENCES_COLUMN,
              DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, false),
              false);

  /** The name of the column holding a resource as Spark parsed it, before it is shaped. */
  @Nonnull static final String PARSED = "parsed";

  /**
   * Returns a function that splits a bundle into the resources of one type it carries, as an array
   * of {@link #TYPE}, for {@link #resources}.
   *
   * @param split the split of one bundle, given the bundle and the type of the resources wanted
   * @return the function
   */
  @Nonnull
  static UserDefinedFunction splitter(
      @Nonnull final UDF2<String, String, List<BundleResource>> split) {
    return functions.udf(
        (UDF2<String, String, List<Row>>)
            (bundle, resourceType) ->
                split.call(bundle, resourceType).stream().map(BundleResource::toRow).toList(),
        DataTypes.createArrayType(TYPE, false));
  }

  /**
   * Returns the resources of one type that a dataset of bundles carries, one per row, as columns
   * named {@link #DOCUMENT_COLUMN} and {@link #REFERENCES_COLUMN}.
   *
   * @param bundles the bundles, one per row, where a null row carries nothing
   * @param split the function that splits one bundle into an array of {@link #TYPE}
   * @param resourceType the type of the resources wanted
   * @return the resources
   */
  @Nonnull
  static Dataset<Row> resources(
      @Nonnull final Dataset<String> bundles,
      @Nonnull final UserDefinedFunction split,
      @Nonnull final String resourceType) {
    return bundles
        .select(
            functions
                .explode(
                    split.apply(functions.col(bundles.columns()[0]), functions.lit(resourceType)))
                .alias("resource"))
        .select("resource." + DOCUMENT_COLUMN, "resource." + REFERENCES_COLUMN);
  }

  /**
   * Resolves a reference to another entry of the bundle a resource came from, where the resource's
   * resolutions name it, and keeps any other reference as written (decision 83, FR-062).
   *
   * @param reference the stored reference
   * @param references the resolutions of the URNs the resource mentions
   * @return the reference, resolved
   */
  @Nonnull
  static Column resolve(@Nonnull final Column reference, @Nonnull final Column references) {
    return functions
        .when(
            reference.startsWith(URN),
            functions.coalesce(functions.try_element_at(references, reference), reference))
        .otherwise(reference);
  }

  /**
   * Returns the resource as a value of a column of {@link #TYPE}.
   *
   * @return the row
   */
  @Nonnull
  Row toRow() {
    return RowFactory.create(document, references);
  }
}
