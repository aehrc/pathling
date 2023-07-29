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

import au.csiro.pathling.views.FhirView;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Represents a FHIR view.
 *
 * @author John Grimes
 */
public class FhirViewBuilder extends QueryBuilder<FhirViewBuilder> {

  @Nonnull
  private final List<NamedExpression> columns = new ArrayList<>();

  @Nonnull
  private final List<VariableExpression> variables = new ArrayList<>();

  public FhirViewBuilder(@Nonnull final QueryDispatcher dispatcher,
      @Nonnull final ResourceType subjectResource) {
    super(dispatcher, subjectResource);
  }

  /**
   * Adds an expression that represents a column to be included in the view.
   *
   * @param expression the column expression
   * @param name the name of the column
   * @return this query
   */
  @Nonnull
  public FhirViewBuilder column(@Nullable final String expression, @Nullable final String name) {
    columns.add(
        new NamedExpression(requireNonBlank(expression, "Column expression cannot be blank"),
            requireNonBlank(name, "Column name cannot be blank")));
    return this;
  }

  /**
   * Adds an expression that represents a variable to be defined within the view.
   *
   * @param expression the variable expression
   * @param name the name of the variable
   * @return this query
   */
  @Nonnull
  public FhirViewBuilder variable(@Nullable final String expression, @Nullable final String name,
      @Nullable final String whenMany) {
    variables.add(
        new VariableExpression(requireNonBlank(expression, "Variable expression cannot be blank"),
            requireNonBlank(name, "Variable name cannot be blank"), WhenMany.fromCode(whenMany)));
    return this;
  }

  @Nonnull
  @Override
  public Dataset<Row> execute() {
    final FhirView view = new FhirView(subjectResource, columns, variables, filters);
    return dispatcher.dispatch(view);
  }

}
