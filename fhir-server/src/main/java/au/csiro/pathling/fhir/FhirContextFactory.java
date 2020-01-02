/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.fhir;

import au.csiro.pathling.bunsen.FhirEncoders;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import java.io.Serializable;

/**
 * Uses the FhirEncoders class from Bunsen to create a FhirContext. Used for code that runs on Spark
 * workers.
 *
 * @author John Grimes
 */
public class FhirContextFactory implements Serializable {

  private FhirVersionEnum fhirVersion;

  public FhirContextFactory() {
  }

  public FhirContextFactory(FhirVersionEnum fhirVersion) {
    this.fhirVersion = fhirVersion;
  }

  public FhirContext build() {
    return FhirEncoders.contextFor(fhirVersion);
  }

  public FhirVersionEnum getFhirVersion() {
    return fhirVersion;
  }

  public void setFhirVersion(FhirVersionEnum fhirVersion) {
    this.fhirVersion = fhirVersion;
  }

}
