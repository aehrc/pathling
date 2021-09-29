/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.async;

import ca.uhn.fhir.rest.api.Constants;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import javax.annotation.Nonnull;
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
