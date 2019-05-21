/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query;

import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hl7.fhir.dstu3.model.Parameters;
import org.hl7.fhir.dstu3.model.Parameters.ParametersParameterComponent;

/**
 * Represents the information provided as part of an invocation of the `aggregate-query` operation.
 *
 * @author John Grimes
 */
@SuppressWarnings("WeakerAccess")
public class AggregateQuery {

  @Nonnull
  private final List<Aggregation> aggregations = new ArrayList<>();

  @Nonnull
  private final List<Grouping> groupings = new ArrayList<>();

  @Nonnull
  private final List<String> filters = new ArrayList<>();

  /**
   * This constructor takes a Parameters resource (with the parameters defined within the
   * `aggregate-query` OperationDefinition) and populates the values into a new AggregateQuery
   * object.
   */
  public AggregateQuery(Parameters parameters) {
    parameters.getParameter().stream()
        .filter(param -> param.getName().equals("aggregation"))
        .forEach(aggregationParameter -> {
          Optional<ParametersParameterComponent> label = aggregationParameter.getPart()
              .stream()
              .filter(part -> part.getName().equals("label"))
              .findFirst();
          Optional<ParametersParameterComponent> expression = aggregationParameter.getPart()
              .stream()
              .filter(part -> part.getName().equals("expression"))
              .findFirst();
          Aggregation aggregation = new Aggregation();
          label.ifPresent(parametersParameterComponent -> {
            // Check for missing value.
            if (parametersParameterComponent.getValue() == null) {
              throw new InvalidRequestException("Aggregation label must have value");
            }
            aggregation.setLabel(parametersParameterComponent.getValue().toString());
          });
          expression.ifPresent(parametersParameterComponent -> {
            // Check for missing value.
            if (parametersParameterComponent.getValue() == null) {
              throw new InvalidRequestException("Aggregation expression must have value");
            }
            aggregation.setExpression(parametersParameterComponent.getValue().toString());
          });
          aggregations.add(aggregation);
        });
    parameters.getParameter().stream()
        .filter(param -> param.getName().equals("grouping"))
        .forEach(aggregationParameter -> {
          Optional<ParametersParameterComponent> label = aggregationParameter.getPart()
              .stream()
              .filter(part -> part.getName().equals("label"))
              .findFirst();
          Optional<ParametersParameterComponent> expression = aggregationParameter.getPart()
              .stream()
              .filter(part -> part.getName().equals("expression"))
              .findFirst();
          Grouping grouping = new Grouping();
          label.ifPresent(parametersParameterComponent -> {
            // Check for missing value.
            if (parametersParameterComponent.getValue() == null) {
              throw new InvalidRequestException("Grouping label must have value");
            }
            grouping.setLabel(parametersParameterComponent.getValue().toString());
          });
          expression.ifPresent(parametersParameterComponent -> {
            // Check for missing value.
            if (parametersParameterComponent.getValue() == null) {
              throw new InvalidRequestException("Grouping expression must have value");
            }
            grouping.setExpression(parametersParameterComponent.getValue().toString());
          });
          groupings.add(grouping);
        });
    filters.addAll(parameters.getParameter().stream()
        .filter(param -> param.getName().equals("filter"))
        .map(param -> {
          // Check for missing value.
          if (param.getValue() == null) {
            throw new InvalidRequestException("Filter parameter must have value");
          }
          return param.getValue().toString();
        })
        .collect(Collectors.toList()));
  }

  @Nonnull
  public List<Aggregation> getAggregations() {
    return aggregations;
  }

  @Nonnull
  public List<Grouping> getGroupings() {
    return groupings;
  }

  @Nonnull
  public List<String> getFilters() {
    return filters;
  }

  public static class Aggregation {

    @Nullable
    private String label;

    @Nullable
    private String expression;

    @Nullable
    public String getLabel() {
      return label;
    }

    public void setLabel(@Nullable String label) {
      this.label = label;
    }

    @Nullable
    public String getExpression() {
      return expression;
    }

    public void setExpression(@Nullable String expression) {
      this.expression = expression;
    }

  }

  public static class Grouping {

    @Nullable
    private String label;

    @Nullable
    private String expression;

    @Nullable
    public String getLabel() {
      return label;
    }

    public void setLabel(@Nullable String label) {
      this.label = label;
    }

    @Nullable
    public String getExpression() {
      return expression;
    }

    public void setExpression(@Nullable String expression) {
      this.expression = expression;
    }

  }
}
