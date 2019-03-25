/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query;

import java.util.ArrayList;
import java.util.List;
import org.hl7.fhir.dstu3.model.Parameters;
import org.hl7.fhir.dstu3.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.dstu3.model.Type;

/**
 * @author John Grimes
 */
public class AggregateQueryResult {

  private final List<Grouping> groupings = new ArrayList<>();

  public List<Grouping> getGroupings() {
    return groupings;
  }

  public Parameters toParameters() {
    Parameters parameters = new Parameters();
    groupings.forEach(grouping -> {
      ParametersParameterComponent groupingParameter = new ParametersParameterComponent();
      groupingParameter.setName("grouping");
      grouping.getLabels()
          .forEach(label -> {
            ParametersParameterComponent labelPart = new ParametersParameterComponent();
            labelPart.setName("label");
            labelPart.setValue(label);
            groupingParameter.getPart().add(labelPart);
          });
      grouping.getResults()
          .forEach(result -> {
            ParametersParameterComponent resultPart = new ParametersParameterComponent();
            resultPart.setName("result");
            resultPart.setValue(result);
            groupingParameter.getPart().add(resultPart);
          });
      parameters.getParameter().add(groupingParameter);
    });
    return parameters;
  }

  public static class Grouping {

    private final List<Type> labels = new ArrayList<>();
    private final List<Type> results = new ArrayList<>();

    public List<Type> getLabels() {
      return labels;
    }

    public List<Type> getResults() {
      return results;
    }
  }

}
