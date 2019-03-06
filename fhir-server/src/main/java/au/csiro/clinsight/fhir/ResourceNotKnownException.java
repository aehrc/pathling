/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir;

/**
 * @author John Grimes
 */
@SuppressWarnings("WeakerAccess")
public class ResourceNotKnownException extends RuntimeException {

  public ResourceNotKnownException(String message) {
    super(message);
  }
}
