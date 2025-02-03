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

@Value(staticConstructor = "of")
public class CollectionDataset {

  @Nonnull
  Dataset<Row> dataset;

  @Nonnull
  Collection value;

  @Nonnull
  public Column getValueColumn() {
    return value.getColumnValue();
  }

  @Nonnull
  public Dataset<Row> toIdValueDataset() {
    return dataset.select(dataset.col("id").alias("id"), getValueColumn().alias("value"));
  }

  @Nonnull
  public CollectionDataset toCanonical() {
    return new CollectionDataset(dataset, value.map(ColumnRepresentation::asCanonical));
  }
}
