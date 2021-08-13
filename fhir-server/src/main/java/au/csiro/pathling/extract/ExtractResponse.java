/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.extract;

import javax.annotation.Nonnull;
import lombok.Getter;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.UriType;

/**
 * Represents the information to be provided as the result of the invocation of the "extract"
 * operation.
 *
 * @author John Grimes
 */
@Getter
public class ExtractResponse {

  @Nonnull
  private final String url;

  /**
   * @param url A URL at which the result can be retrieved
   */
  public ExtractResponse(@Nonnull final String url) {
    this.url = url;
  }

  /**
   * Converts this to a {@link Parameters} resource, based on the definition of the result of the
   * "extract" operation within the OperationDefinition.
   *
   * @return a new {@link Parameters} object
   */
  public Parameters toParameters() {
    final Parameters parameters = new Parameters();
    final ParametersParameterComponent urlParameter = new ParametersParameterComponent();
    urlParameter.setName("url");
    urlParameter.setValue(new UriType(url));
    parameters.getParameter().add(urlParameter);
    return parameters;
  }

}
