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

package au.csiro.pathling.library.query;

import static au.csiro.pathling.utilities.Preconditions.requireNonBlank;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Base class for queries that use filters. Subclasses must implement the {@link #doExecute} method
 * to perform the actual execution of the query.
 *
 * @param <T> actual type of the query.
 */
@SuppressWarnings({"unchecked"})
public abstract class AbstractQueryWithFilters<T extends AbstractQueryWithFilters<?>> {

  @Nonnull
  protected final ResourceType subjectResource;

  @Nullable
  protected PathlingClient pathlingClient = null;

  @Nonnull
  protected final List<String> filters = new ArrayList<>();

  protected AbstractQueryWithFilters(
      @Nonnull final ResourceType subjectResource) {
    this.subjectResource = subjectResource;
  }

  /**
   * Binds the query to a specific client.
   *
   * @param pathlingClient the client to use.
   * @return this query
   */
  @Nonnull
  public T withClient(@Nonnull final PathlingClient pathlingClient) {
    this.pathlingClient = pathlingClient;
    return (T) this;
  }

  /**
   * Adds a fhirpath filter expression to the query. The extract query result only include rows for
   * resources that match ALL the filters.
   *
   * @param filterFhirpath the filter expression to add.
   * @return this query.
   */
  @Nonnull
  public T withFilter(@Nonnull final String filterFhirpath) {
    filters.add(requireNonBlank(filterFhirpath, "Filter expression cannot be blank"));
    return (T) this;
  }

  /**
   * Executes the query on the bound client.
   *
   * @return the dataset with the result of the query.
   */
  @Nonnull
  public Dataset<Row> execute() {
    return doExecute(requireNonNull(this.pathlingClient));
  }

  /**
   * Executes the query on the given client.
   *
   * @param pathlingClient the client to execute the query against.
   * @return the dataset with the result of the query.
   */
  @Nonnull
  public Dataset<Row> execute(@Nonnull final PathlingClient pathlingClient) {
    return doExecute(pathlingClient);
  }

  /**
   * Performs the actual execution of the query.
   *
   * @param pathlingClient the client to execute the query against.
   * @return the dataset with the result of the query.
   */
  @Nonnull
  protected abstract Dataset<Row> doExecute(@Nonnull final PathlingClient pathlingClient);

}
