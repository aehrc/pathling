/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir.definitions;

/**
 * This error is thrown when an element is encountered within a FHIRPath expression that is not
 * present within known definitions.
 *
 * @author John Grimes
 */
@SuppressWarnings("WeakerAccess")
public class ElementNotKnownException extends RuntimeException {

  public ElementNotKnownException(String message) {
    super(message);
  }

}
