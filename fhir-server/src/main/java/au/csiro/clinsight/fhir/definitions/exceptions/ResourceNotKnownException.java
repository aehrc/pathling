/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir.definitions.exceptions;

/**
 * This error is thrown when a resource or data type is encountered within a FHIRPath expression
 * that is not present within known definitions.
 *
 * @author John Grimes
 */
@SuppressWarnings("WeakerAccess")
public class ResourceNotKnownException extends RuntimeException {

  public ResourceNotKnownException(String message) {
    super(message);
  }

}
