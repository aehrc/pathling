/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.query.functions;

import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;

/**
 * Provides a home for common validation code used by functions.
 *
 * @author John Grimes
 */
public abstract class FunctionValidations {

  public static void validateNoArgumentInput(String functionName, FunctionInput input) {
    if (!input.getArguments().isEmpty()) {
      throw new InvalidRequestException(
          "Arguments can not be passed to " + functionName + " function: " + input.getExpression());
    }
  }
}
