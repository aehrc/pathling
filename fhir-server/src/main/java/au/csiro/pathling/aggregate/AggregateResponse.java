/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.aggregate;

import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Value;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.Type;

/**
 * Represents the information to be provided as the result of the invocation of the "aggregate"
 * operation.
 *
 * @author John Grimes
 */
@Getter
public class AggregateResponse {

  @Nonnull
  private final List<Grouping> groupings;

  /**
   * @param groupings A set of grouped results
   */
  public AggregateResponse(@Nonnull final List<Grouping> groupings) {
    this.groupings = groupings;
  }

  /**
   * Converts this to a {@link Parameters} resource, based on the definition of the result of the
   * "aggregate" operation within the OperationDefinition.
   *
   * @return a new {@link Parameters} object
   */
  public Parameters toParameters() {
    final Parameters parameters = new Parameters();
    groupings.forEach(grouping -> {
      final ParametersParameterComponent groupingParameter = new ParametersParameterComponent();
      groupingParameter.setName("grouping");
      grouping.getLabels()
          .forEach(label -> {
            final ParametersParameterComponent labelPart = new ParametersParameterComponent();
            labelPart.setName("label");
            // A "null" value is represented by the absence of a value within FHIR.
            label.ifPresent(labelPart::setValue);
            groupingParameter.getPart().add(labelPart);
          });
      grouping.getResults()
          .forEach(result -> {
            final ParametersParameterComponent resultPart = new ParametersParameterComponent();
            resultPart.setName("result");
            // A "null" value is represented by the absence of a value within FHIR.
            result.ifPresent(resultPart::setValue);
            groupingParameter.getPart().add(resultPart);
          });
      if (grouping.getDrillDown().isPresent()) {
        final String drillDown = grouping.getDrillDown().get();
        final ParametersParameterComponent drillDownPart = new ParametersParameterComponent();
        drillDownPart.setName("drillDown");
        drillDownPart.setValue(new StringType(drillDown));
        groupingParameter.getPart().add(drillDownPart);
      }
      parameters.getParameter().add(groupingParameter);
    });
    return parameters;
  }

  /**
   * Represents a grouped result within an {@link AggregateResponse}.
   */
  @Value
  public static class Grouping {

    @Nonnull
    // This is a list of Optionals to account for the fact that we can receive null labels here, 
    // which is valid when a grouping expression evaluates to an empty collection for some 
    // resources.
    List<Optional<Type>> labels;

    @Nonnull
    // This is a list of Optionals to account for the fact that we can receive null results of 
    // aggregations.
    List<Optional<Type>> results;

    @Nonnull
    Optional<String> drillDown;

  }

}
