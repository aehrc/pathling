/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.hl7.fhir.dstu3.model.Parameters;
import org.hl7.fhir.dstu3.model.Parameters.ParametersParameterComponent;

/**
 * @author John Grimes
 */
public class AggregateQuery {

  private final List<Aggregation> aggregations = new ArrayList<>();
  private final List<Grouping> groupings = new ArrayList<>();

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

  public List<Aggregation> getAggregations() {
    return aggregations;
  }

  public List<Grouping> getGroupings() {
    return groupings;
  }

  public static class Aggregation {

    private String label;
    private String expression;

    public String getLabel() {
      return label;
    }

    public void setLabel(String label) {
      this.label = label;
    }

    public String getExpression() {
      return expression;
    }

    public void setExpression(String expression) {
      this.expression = expression;
    }

  }

  public static class Grouping {

    private String label;
    private String expression;

    public String getLabel() {
      return label;
    }

    public void setLabel(String label) {
      this.label = label;
    }

    public String getExpression() {
      return expression;
    }

    public void setExpression(String expression) {
      this.expression = expression;
    }

  }
}
