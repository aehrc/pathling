/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.aggregate;

import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
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
  List<String> aggregations;

  @Nonnull
  List<String> groupings;

  @Nonnull
  List<String> filters;

  /**
   * @param subjectResource The resource which will serve as the input context for each expression
   * @param aggregations A set of aggregation expressions to execute over the data
   * @param groupings Instructions on how the data should be grouped when aggregating
   * @param filters The criteria by which the data should be filtered
   */
  public AggregateRequest(@Nonnull final ResourceType subjectResource,
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
    this.subjectResource = subjectResource;
    this.aggregations = aggregations.get();
    this.groupings = groupings.orElse(Collections.emptyList());
    this.filters = filters.orElse(Collections.emptyList());
  }

}
