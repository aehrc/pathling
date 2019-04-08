/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
          label.ifPresent(parametersParameterComponent -> aggregation
              .setLabel(parametersParameterComponent.getValue().toString()));
          expression.ifPresent(parametersParameterComponent -> aggregation
              .setExpression(parametersParameterComponent.getValue().toString()));
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
          label.ifPresent(parametersParameterComponent -> grouping
              .setLabel(parametersParameterComponent.getValue().toString()));
          expression.ifPresent(parametersParameterComponent -> grouping
              .setExpression(parametersParameterComponent.getValue().toString()));
          groupings.add(grouping);
        });
  }

  @Nonnull
  public List<Aggregation> getAggregations() {
    return aggregations;
  }

  @Nonnull
  public List<Grouping> getGroupings() {
    return groupings;
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
