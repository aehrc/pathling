/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import org.hl7.fhir.dstu3.model.Parameters;
import org.hl7.fhir.dstu3.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.dstu3.model.Type;

/**
 * Represents the information to be provided as the result of the invocation of the
 * `aggregate-query` operation.
 *
 * @author John Grimes
 */
public class AggregateQueryResult {

  @Nonnull
  private final List<Grouping> groupings = new ArrayList<>();

  @Nonnull
  public List<Grouping> getGroupings() {
    return groupings;
  }

  /**
   * This constructor populates a Parameters resource with the values from this object, based on the
   * definition of the result of the `aggregate-query` operation within the OperationDefinition.
   */
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

    @Nonnull
    private final List<Type> labels = new ArrayList<>();

    @Nonnull
    private final List<Type> results = new ArrayList<>();

    @Nonnull
    public List<Type> getLabels() {
      return labels;
    }

    @Nonnull
    public List<Type> getResults() {
      return results;
    }
  }

}
