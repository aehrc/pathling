/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.fhir;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import java.io.Serializable;

/**
 * Interface used for encapsulating the creation of a FhirContext.
 *
 * @author John Grimes
 */
public interface FhirContextFactory extends Serializable {

  public FhirContext getFhirContext(FhirVersionEnum version);

}
