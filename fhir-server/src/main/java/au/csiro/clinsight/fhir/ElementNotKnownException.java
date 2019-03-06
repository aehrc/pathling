/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir;

/**
 * @author John Grimes
 */
@SuppressWarnings("WeakerAccess")
public class ElementNotKnownException extends RuntimeException {

  public ElementNotKnownException(String message) {
    super(message);
  }
}
