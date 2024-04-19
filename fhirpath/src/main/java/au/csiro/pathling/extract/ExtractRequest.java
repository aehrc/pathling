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

package au.csiro.pathling.extract;

import static au.csiro.pathling.utilities.Lists.normalizeEmpty;
import static au.csiro.pathling.utilities.Preconditions.checkPresent;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.query.ExpressionWithLabel;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import lombok.Value;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Represents the information provided as part of an invocation of the "extract" operation.
 *
 * @author John Grimes
 */
@Value
public class ExtractRequest {

  @Nonnull
  ResourceType subjectResource;

  @Nonnull
  List<ExpressionWithLabel> columnsWithLabels;

  @Nonnull
  List<String> filters;

  @Nonnull
  Optional<Integer> limit;

  /**
   * @return The list of column expressions
   */
  @Nonnull
  public List<String> getColumns() {
    return ExpressionWithLabel.expressionsAsList(columnsWithLabels);
  }

  /**
   * Constructs the instance of ExtractRequest from user input, performing necessary validations.
   *
   * @param subjectResource the resource which will serve as the input context for each expression
   * @param columns a set of columns expressions to execute over the data
   * @param filters the criteria by which the data should be filtered
   * @param limit the maximum number of rows to return
   * @return the constructed instance of {@link ExtractRequest}
   */
  public static ExtractRequest fromUserInput(@Nonnull final ResourceType subjectResource,
      @Nonnull final Optional<List<String>> columns, @Nonnull final Optional<List<String>> filters,
      @Nonnull final Optional<Integer> limit) {
    checkUserInput(columns.isPresent() && columns.get().size() > 0,
        "Query must have at least one column expression");
    checkUserInput(columns.get().stream().noneMatch(String::isBlank),
        "Column expression cannot be blank");
    filters.ifPresent(f -> checkUserInput(f.stream().noneMatch(String::isBlank),
        "Filter expression cannot be blank"));
    limit.ifPresent(l -> checkUserInput(l > 0, "Limit must be greater than zero"));
    return new ExtractRequest(subjectResource,
        ExpressionWithLabel.fromUnlabelledExpressions(checkPresent(columns)),
        normalizeEmpty(filters), limit);
  }
}
