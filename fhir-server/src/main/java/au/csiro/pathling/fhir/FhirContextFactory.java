/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhir;

import au.csiro.pathling.encoders.FhirEncoders;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import java.io.Serializable;
import javax.annotation.Nonnull;
import org.springframework.stereotype.Component;

/**
 * Uses the FhirEncoders class to create a FhirContext. Used for code that runs on Spark workers.
 *
 * @author John Grimes
 */
@Component
public class FhirContextFactory implements Serializable {

  private static final long serialVersionUID = 3704272891614244206L;

  @Nonnull
  private final FhirVersionEnum fhirVersion;

  public FhirContextFactory(@Nonnull final FhirContext fhirContext) {
    this.fhirVersion = fhirContext.getVersion().getVersion();
  }

  @Nonnull
  public FhirContext build() {
    return FhirEncoders.contextFor(fhirVersion);
  }

}
