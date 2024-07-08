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

package au.csiro.pathling.aggregate;

import static au.csiro.pathling.utilities.Preconditions.checkPresent;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.query.ExpressionWithLabel;
import au.csiro.pathling.utilities.Lists;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import lombok.Value;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * Represents the information provided as part of an invocation of the "aggregate" operation.
 *
 * @author John Grimes
 */
@Value
public class AggregateRequest {

  @Nonnull
  ResourceType subjectResource;

  @Nonnull
  List<ExpressionWithLabel> aggregationsWithLabels;

  @Nonnull
  List<ExpressionWithLabel> groupingsWithLabels;

  @Nonnull
  List<String> filters;

  /**
   * @return The list of aggregation expressions
   */
  @Nonnull
  public List<String> getAggregations() {
    return ExpressionWithLabel.expressionsAsList(aggregationsWithLabels);
  }

  /**
   * @return The list of grouping expressions
   */
  @Nonnull
  public List<String> getGroupings() {
    return ExpressionWithLabel.expressionsAsList(groupingsWithLabels);
  }

  /**
   * Constructs the instance of {@code AggregateRequest} from user input, performing necessary
   * validations.
   *
   * @param subjectResource The resource which will serve as the input context for each expression
   * @param aggregations A set of aggregation expressions to execute over the data
   * @param groupings Instructions on how the data should be grouped when aggregating
   * @param filters The criteria by which the data should be filtered
   * @return A new instance of {@code AggregateRequest}
   */
  public static AggregateRequest fromUserInput(@Nonnull final ResourceType subjectResource,
      @Nonnull final Optional<List<String>> aggregations,
      @Nonnull final Optional<List<String>> groupings,
      @Nonnull final Optional<List<String>> filters) {
    checkUserInput(aggregations.isPresent() && aggregations.get().size() > 0,
        "Query must have at least one aggregation expression");
    checkUserInput(aggregations.get().stream().noneMatch(String::isBlank),
        "Aggregation expression cannot be blank");
    groupings.ifPresent(g -> checkUserInput(g.stream().noneMatch(String::isBlank),
        "Grouping expression cannot be blank"));
    filters.ifPresent(f -> checkUserInput(f.stream().noneMatch(String::isBlank),
        "Filter expression cannot be blank"));
    return new AggregateRequest(subjectResource,
        ExpressionWithLabel.fromUnlabelledExpressions(checkPresent(aggregations)),
        ExpressionWithLabel.fromUnlabelledExpressions(Lists.normalizeEmpty(groupings)),
        Lists.normalizeEmpty(filters));
  }
}
