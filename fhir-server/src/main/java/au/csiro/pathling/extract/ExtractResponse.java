/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.extract;

import jakarta.annotation.Nonnull;
import lombok.Getter;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.UrlType;

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
    urlParameter.setValue(new UrlType(url));
    parameters.getParameter().add(urlParameter);
    return parameters;
  }

}
