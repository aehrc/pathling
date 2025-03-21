/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import jakarta.annotation.Nonnull;
import lombok.Value;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A wrapper class that pairs a Spark Dataset with a FHIRPath Collection.
 * <p>
 * This class is used during FHIRPath evaluation to maintain the association between:
 * <ul>
 *   <li>The Spark Dataset containing the actual data</li>
 *   <li>The FHIRPath Collection that provides the logical representation and operations</li>
 * </ul>
 * <p>
 * CollectionDataset enables operations that require both the dataset structure and the
 * collection's logical representation, such as converting to canonical form or extracting
 * specific columns for output.
 */
@Value(staticConstructor = "of")
public class CollectionDataset {

  /**
   * Column name for the resource identifier.
   */
  public static final String ID_COLUMN = "id";
  
  /**
   * Column name for the expression result value.
   */
  public static final String VALUE_COLUMN = "value";

  /**
   * The Spark Dataset containing the actual data.
   * <p>
   * This dataset typically includes an "id" column for resource identification and
   * additional columns containing the data being processed.
   */
  @Nonnull
  Dataset<Row> dataset;

  /**
   * The FHIRPath Collection providing the logical representation of the data.
   * <p>
   * This collection contains the column representation and type information needed
   * to interpret and manipulate the data in the dataset.
   */
  @Nonnull
  Collection value;

  /**
   * Gets the Spark SQL Column that represents the value in this collection.
   * <p>
   * This method delegates to the underlying Collection to get its column value,
   * which is used for operations on the dataset.
   *
   * @return The Spark SQL Column representing the value
   */
  @Nonnull
  public Column getValueColumn() {
    return value.getColumnValue();
  }

  /**
   * Converts this CollectionDataset to a simplified dataset with just id and value columns.
   * <p>
   * This method is useful for creating a standardized output format with:
   * <ul>
   *   <li>An "id" column identifying the resource</li>
   *   <li>A "value" column containing the result of the FHIRPath expression</li>
   * </ul>
   *
   * @return A new Dataset with id and value columns
   */
  @Nonnull
  public Dataset<Row> toIdValueDataset() {
    return dataset.select(
        dataset.col(ID_COLUMN).alias(ID_COLUMN), 
        getValueColumn().alias(VALUE_COLUMN)
    );
  }

  /**
   * Converts this CollectionDataset to its canonical form.
   * <p>
   * The canonical form standardizes the representation of values, which is useful for
   * operations like comparison and sorting. This method:
   * <ul>
   *   <li>Keeps the same dataset structure</li>
   *   <li>Maps the collection to use canonical column representations</li>
   * </ul>
   *
   * @return A new CollectionDataset with the collection in canonical form
   */
  @Nonnull
  public CollectionDataset toCanonical() {
    return new CollectionDataset(dataset, value.map(ColumnRepresentation::asCanonical));
  }
}
