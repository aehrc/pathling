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

import au.csiro.pathling.extract.ExtractRequest;
import au.csiro.pathling.query.ExpressionWithLabel;
import au.csiro.pathling.utilities.Lists;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Represents an extract query.
 *
 * @author Piotr Szul
 */
public class ExtractQuery extends QueryBuilder<ExtractQuery> {

  @Nonnull
  private final List<ExpressionWithLabel> columns = new ArrayList<>();

  @Nonnull
  private Optional<Integer> limit = Optional.empty();

  public ExtractQuery(@Nonnull final QueryDispatcher executor,
      @Nonnull final ResourceType subjectResource) {
    super(executor, subjectResource);
  }

  /**
   * Adds an expression that represents a column to be extracted in the result.
   *
   * @param expression the column expression
   * @return this query
   */
  @Nonnull
  public ExtractQuery column(@Nonnull final String expression) {
    columns.add(ExpressionWithLabel.withExpressionAsLabel(expression));
    return this;
  }

  /**
   * Adds an expression that represents a labelled column to be extracted in the result.
   *
   * @param expression the column expressions
   * @param label the label of the column
   * @return this query
   */
  @Nonnull
  public ExtractQuery column(@Nonnull final String expression, @Nonnull final String label) {
    columns.add(ExpressionWithLabel.of(expression, label));
    return this;
  }

  /**
   * Sets a limit on the number of rows returned by this query.
   *
   * @param limit the limit
   * @return this query
   */
  public ExtractQuery limit(final int limit) {
    this.limit = Optional.of(limit);
    return this;
  }

  @Nonnull
  @Override
  public Dataset<Row> execute() {
    final ExtractRequest request = new ExtractRequest(subjectResource,
        Lists.normalizeEmpty(columns),
        Lists.normalizeEmpty(filters),
        limit);
    return dispatcher.dispatch(request);
  }

}
