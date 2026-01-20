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

package au.csiro.pathling.fhirpath.execution;

import jakarta.annotation.Nonnull;
import java.util.stream.Stream;
import lombok.experimental.UtilityClass;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
 * Utility class for transforming between flat resource schemas and evaluation schemas.
 * <p>
 * This class provides methods to convert between two schema representations:
 * <ul>
 *   <li><b>Flat schema</b>: The standard Pathling-encoded schema where resource fields are
 *       top-level columns (e.g., id, meta, name, gender, birthDate, ...)</li>
 *   <li><b>Evaluation schema</b>: The wrapped schema used during FHIRPath evaluation with
 *       structure: id, key, ResourceType{id, meta, name, ...}</li>
 * </ul>
 * <p>
 * The evaluation schema is used internally by FHIRPath evaluators to provide a consistent
 * structure for resource access during expression evaluation.
 */
@UtilityClass
public class ResourceSchemaTransformation {

  /**
   * The name of the key column in the evaluation schema.
   */
  public static final String KEY_COLUMN = "key";

  /**
   * The name of the versioned ID column in the flat schema that maps to the key column.
   */
  public static final String ID_VERSIONED_COLUMN = "id_versioned";

  /**
   * The name of the ID column.
   */
  public static final String ID_COLUMN = "id";

  /**
   * Wraps a flat resource dataset into the evaluation schema.
   * <p>
   * Transforms from:
   * <pre>
   *   id, id_versioned, meta, name, gender, birthDate, ...
   * </pre>
   * To:
   * <pre>
   *   id, key, ResourceType{id, id_versioned, meta, name, gender, birthDate, ...}
   * </pre>
   * <p>
   * The {@code id_versioned} column becomes the {@code key} column, and all original columns
   * are wrapped into a struct column named after the resource type.
   *
   * @param resourceCode the resource type code (e.g., "Patient", "Observation")
   * @param flatDataset the dataset with flat resource schema
   * @return a dataset with the evaluation schema
   */
  @Nonnull
  public static Dataset<Row> wrapToEvaluationSchema(
      @Nonnull final String resourceCode,
      @Nonnull final Dataset<Row> flatDataset) {
    return flatDataset.select(
        flatDataset.col(ID_COLUMN),
        flatDataset.col(ID_VERSIONED_COLUMN).alias(KEY_COLUMN),
        functions.struct(
            Stream.of(flatDataset.columns())
                .map(flatDataset::col).toArray(Column[]::new)
        ).alias(resourceCode));
  }

  /**
   * Unwraps an evaluation schema dataset back to the flat resource schema.
   * <p>
   * Transforms from:
   * <pre>
   *   id, key, ResourceType{id, id_versioned, meta, name, gender, birthDate, ...}
   * </pre>
   * To:
   * <pre>
   *   id, id_versioned, meta, name, gender, birthDate, ...
   * </pre>
   * <p>
   * This extracts all fields from the nested struct column back to top-level columns.
   *
   * @param resourceCode the resource type code (e.g., "Patient", "Observation")
   * @param wrappedDataset the dataset with evaluation schema
   * @return a dataset with flat resource schema
   */
  @Nonnull
  public static Dataset<Row> unwrapToFlatSchema(
      @Nonnull final String resourceCode,
      @Nonnull final Dataset<Row> wrappedDataset) {
    return wrappedDataset.select(resourceCode + ".*");
  }
}
