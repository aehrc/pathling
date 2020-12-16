/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.aggregate;

import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.errors.InvalidUserInputError;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Value;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.StringType;

/**
 * Represents the information provided as part of an invocation of the "aggregate" operation.
 *
 * @author John Grimes
 */
@Value
public class AggregateRequest {

  private static final String AGGREGATION_PARAMETER = "aggregation";
  private static final String GROUPING_PARAMETER = "grouping";
  private static final String FILTER_PARAMETER = "filter";

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
      @Nonnull final List<String> aggregations, @Nonnull final List<String> groupings,
      @Nonnull final List<String> filters) {
    checkUserInput(aggregations.size() > 0, "Query must have at least one aggregation expression");
    checkUserInput(aggregations.stream().noneMatch(String::isBlank),
        "Aggregation expression cannot be blank");
    checkUserInput(groupings.stream().noneMatch(String::isBlank),
        "Grouping expression cannot be blank");
    checkUserInput(filters.stream().noneMatch(String::isBlank),
        "Filter expression cannot be blank");
    this.subjectResource = subjectResource;
    this.aggregations = aggregations;
    this.groupings = groupings;
    this.filters = filters;
  }

  /**
   * This static build method takes a {@link Parameters} resource (with the parameters defined
   * within the "aggregate" OperationDefinition) and populates the values into a new {@link
   * AggregateRequest} object.
   *
   * @param parameters a {@link Parameters} object
   * @return an AggregateRequest
   */
  @Nonnull
  public static AggregateRequest from(@Nonnull final Parameters parameters) {
    final ResourceType subjectResource = getSubjectResource(parameters);
    final List<String> aggregations = getExpressions(parameters, AGGREGATION_PARAMETER);
    final List<String> groupings = getExpressions(parameters, GROUPING_PARAMETER);
    final List<String> filters = getExpressions(parameters, FILTER_PARAMETER);

    return new AggregateRequest(subjectResource, aggregations, groupings, filters);
  }

  @Nonnull
  private static ResourceType getSubjectResource(@Nonnull final Parameters parameters) {
    final ParametersParameterComponent subjectResourceParam = parameters.getParameter().stream()
        .filter(param -> param.getName().equals("subjectResource"))
        .findFirst()
        .orElseThrow(
            () -> new InvalidUserInputError("There must be one subject resource parameter"));
    checkUserInput(subjectResourceParam.getValue() instanceof CodeType,
        "Subject resource parameter must have code value");
    final CodeType subjectResourceCode = (CodeType) subjectResourceParam.getValue();
    final ResourceType subjectResource;
    try {
      subjectResource = ResourceType.fromCode(subjectResourceCode.asStringValue());
    } catch (final FHIRException e) {
      throw new InvalidUserInputError(
          "Subject resource must be a member of https://hl7.org/fhir/ValueSet/resource-types.");
    }
    return subjectResource;
  }

  @Nonnull
  private static List<String> getExpressions(@Nonnull final Parameters parameters,
      @Nonnull final String parameter) {
    return parameters.getParameter().stream()
        .filter(
            param -> param.getName().equals(parameter) && param.getValue() instanceof StringType)
        .map(aggregation -> aggregation.getValue().toString())
        .collect(Collectors.toList());
  }

}
