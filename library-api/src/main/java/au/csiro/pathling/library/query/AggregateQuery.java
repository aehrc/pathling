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

import au.csiro.pathling.aggregate.AggregateRequest;
import au.csiro.pathling.query.ExpressionWithLabel;
import au.csiro.pathling.utilities.Lists;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Represents an aggregate query.
 *
 * @author Piotr Szul
 */
public class AggregateQuery extends QueryBuilder<AggregateQuery> {

  @Nonnull
  private final List<ExpressionWithLabel> groupings = new ArrayList<>();

  @Nonnull
  private final List<ExpressionWithLabel> aggregations = new ArrayList<>();

  public AggregateQuery(@Nonnull final QueryDispatcher dispatcher,
      @Nonnull final ResourceType subjectResource) {
    super(dispatcher, subjectResource);
  }

  /**
   * Adds an expression that represents an aggregation column.
   *
   * @param expression the column expressions
   * @return this query
   */
  @Nonnull
  public AggregateQuery aggregation(@Nonnull final String expression) {
    aggregations.add(ExpressionWithLabel.withExpressionAsLabel(expression));
    return this;
  }

  /**
   * Adds an expression that represents a labelled aggregation column.
   *
   * @param expression the aggregation expression
   * @param label the label for the column
   * @return this query
   */
  @Nonnull
  public AggregateQuery aggregation(@Nonnull final String expression, @Nonnull final String label) {
    aggregations.add(ExpressionWithLabel.of(expression, label));
    return this;
  }

  /**
   * Adds an expression that represents a grouping column.
   *
   * @param expression the column expressions
   * @return this query
   */
  @Nonnull
  public AggregateQuery grouping(@Nonnull final String expression) {
    groupings.add(ExpressionWithLabel.withExpressionAsLabel(expression));
    return this;
  }

  /**
   * Adds an expression that represents a labelled grouping column.
   *
   * @param expression the grouping expression
   * @param label the label for the column
   * @return this query
   */
  @Nonnull
  public AggregateQuery grouping(@Nonnull final String expression, @Nonnull final String label) {
    groupings.add(ExpressionWithLabel.of(expression, label));
    return this;
  }

  @Nonnull
  @Override
  public Dataset<Row> execute() {
    final AggregateRequest request = new AggregateRequest(subjectResource,
        Lists.normalizeEmpty(aggregations),
        Lists.normalizeEmpty(groupings),
        Lists.normalizeEmpty(filters));
    return dispatcher.dispatch(request);
  }

}
