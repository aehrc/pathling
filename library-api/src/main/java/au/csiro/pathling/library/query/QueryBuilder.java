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

import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Base class for queries that use filters. Subclasses must implement the {@link #execute} method to
 * perform the actual execution of the query.
 */
@SuppressWarnings({"unchecked"})
public abstract class QueryBuilder {

  @Nonnull
  protected final String subjectResource;

  @Nonnull
  protected final QueryDispatcher dispatcher;

  protected QueryBuilder(@Nonnull final QueryDispatcher dispatcher,
      @Nonnull final String subjectResource) {
    this.dispatcher = dispatcher;
    this.subjectResource = subjectResource;
  }

  /**
   * Executes the query.
   *
   * @return the dataset with the result of the query
   */
  @Nonnull
  public abstract Dataset<Row> execute();

}
