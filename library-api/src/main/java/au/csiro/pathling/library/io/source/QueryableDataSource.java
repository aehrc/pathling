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
package au.csiro.pathling.library.io.source;

import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.io.sink.DataSinkBuilder;
import au.csiro.pathling.library.query.FhirViewQuery;
import au.csiro.pathling.views.FhirView;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * A FHIR data source that can be queried, and can also be written out to a data sink.
 *
 * @author Piotr Szul
 * @author John Grimes
 */
public interface QueryableDataSource extends DataSource {

  /**
   * @return a builder capable of writing this data source using various methods
   */
  @Nonnull
  DataSinkBuilder write();

  /**
   * @param subjectResource the subject resource code
   * @return an executable {@link FhirViewQuery}
   */
  @Nonnull
  FhirViewQuery view(@Nullable final String subjectResource);

  /**
   * @param view a {@link FhirView} to be executed
   * @return an executable {@link FhirViewQuery}
   */
  @Nonnull
  FhirViewQuery view(@Nullable FhirView view);

  /**
   * Applies a transformation to each dataset within this data source.
   *
   * @param operator the transformation to apply
   * @return a new DataSource containing the transformed datasets
   */
  @Nonnull
  default QueryableDataSource map(@Nonnull final UnaryOperator<Dataset<Row>> operator) {
    return map((resourceType, rowDataset) -> operator.apply(rowDataset));
  }

  /**
   * Applies a transformation to each dataset within this data source.
   * @param operator the transformation to apply
   * @return a new DataSource containing the transformed datasets
   */
  QueryableDataSource map(@Nonnull final BiFunction<String, Dataset<Row>, Dataset<Row>> operator);

  /**
   * Filter the dataset by resource type to remove entire datasets.
   * This does not filter IN the dataset, but the datasets as a whole. If filtering of specific columns
   * across all datasets is desired, use
   * <pre>{@code
   *   dataSource.map(dataset -> dataset.filter(...));
   * }</pre>
   * @param resourceTypePredicate The predicate to keep datasets
   * @return a new DataSource containing only the datasets where the associated resource type matched the predicate
   */
  @Nonnull
  QueryableDataSource filterByResourceType(@Nonnull final Predicate<String> resourceTypePredicate);
  
  /**
   * Caches the datasets in this data source to improve performance for subsequent queries.
   *
   * @return a new DataSource with cached datasets
   */
  DataSource cache();

}
