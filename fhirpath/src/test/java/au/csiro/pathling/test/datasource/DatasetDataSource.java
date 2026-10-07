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

package au.csiro.pathling.test.datasource;

import static java.util.Objects.requireNonNull;

import au.csiro.pathling.io.source.DataSource;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * A data source over datasets that have already been read, keyed by resource type. Useful where a
 * test needs the engine to read data exactly as a particular Spark read produced it, rather than as
 * encoded from objects in memory.
 */
public class DatasetDataSource implements DataSource {

  @Nonnull private final Map<String, Dataset<Row>> data;

  /**
   * Creates a data source over the given datasets.
   *
   * @param data the datasets, keyed by resource type
   */
  public DatasetDataSource(@Nonnull final Map<String, Dataset<Row>> data) {
    this.data = Map.copyOf(data);
  }

  @Nonnull
  @Override
  public Dataset<Row> read(@Nullable final String resourceCode) {
    return requireNonNull(data.get(resourceCode));
  }

  @Nonnull
  @Override
  public Set<String> getResourceTypes() {
    return data.keySet();
  }
}
