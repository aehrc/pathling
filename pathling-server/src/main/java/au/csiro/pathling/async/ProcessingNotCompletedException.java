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

package au.csiro.pathling.async;

import ca.uhn.fhir.rest.api.Constants;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import jakarta.annotation.Nonnull;
import org.hl7.fhir.instance.model.api.IBaseOperationOutcome;

/**
 * An exception used to represent the "202 Accepted" HTTP response.
 *
 * @author John Grimes
 */
public class ProcessingNotCompletedException extends BaseServerResponseException {

  private static final long serialVersionUID = -1755375090680736458L;
  private static final int STATUS_CODE = Constants.STATUS_HTTP_202_ACCEPTED;

  /**
   * @param theMessage a descriptive message
   * @param theOperationOutcome an {@link org.hl7.fhir.r4.model.OperationOutcome} describing the
   * response
   */
  public ProcessingNotCompletedException(@Nonnull final String theMessage,
      @Nonnull final IBaseOperationOutcome theOperationOutcome) {
    super(STATUS_CODE, theMessage, theOperationOutcome);
  }

}
