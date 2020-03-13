/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.Type;

/**
 * Represents the information to be provided as the result of the invocation of the
 * `aggregate` operation.
 *
 * @author John Grimes
 */
public class AggregateResponse {

  @Nonnull
  private final List<Grouping> groupings = new ArrayList<>();

  @Nonnull
  public List<Grouping> getGroupings() {
    return groupings;
  }

  /**
   * This constructor populates a Parameters resource with the values from this object, based on the
   * definition of the result of the `aggregate` operation within the OperationDefinition.
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
      ParametersParameterComponent drillDownPart = new ParametersParameterComponent();
      drillDownPart.setName("drillDown");
      drillDownPart.setValue(grouping.getDrillDown());
      groupingParameter.getPart().add(drillDownPart);
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
    private StringType drillDown;

    @Nonnull
    public List<Type> getLabels() {
      return labels;
    }

    @Nonnull
    public List<Type> getResults() {
      return results;
    }

    @Nonnull
    public StringType getDrillDown() {
      return drillDown;
    }

    public void setDrillDown(@Nonnull StringType drillDown) {
      this.drillDown = drillDown;
    }
  }

}
