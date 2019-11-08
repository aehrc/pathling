/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import com.cerner.bunsen.FhirEncoders;

/**
 * Uses the FhirEncoders class from Bunsen to create a FhirContext. Used for code that runs on Spark
 * workers.
 *
 * @author John Grimes
 */
public class FreshFhirContextFactory implements FhirContextFactory {

  @Override
  public FhirContext getFhirContext(FhirVersionEnum version) {
    return FhirEncoders.contextFor(version);
  }

}
