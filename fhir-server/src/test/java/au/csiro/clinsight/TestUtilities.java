/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;

/**
 * @author John Grimes
 */
public abstract class TestUtilities {

  private static final FhirContext fhirContext = FhirContext.forR4();
  private static final IParser jsonParser = fhirContext.newJsonParser();

  public static FhirContext getFhirContext() {
    return fhirContext;
  }

  public static IParser getJsonParser() {
    return jsonParser;
  }

}
