/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhir;

import au.csiro.pathling.encoders.FhirEncoders;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import java.io.Serializable;

/**
 * Uses the FhirEncoders class to create a FhirContext. Used for code that runs on Spark workers.
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
