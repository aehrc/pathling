/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.assertions;

import au.csiro.pathling.fhirpath.FhirPath;
import javax.annotation.Nonnull;

/**
 * @author John Grimes
 */
public class FhirPathAssertion extends BaseFhirPathAssertion<FhirPathAssertion> {

  FhirPathAssertion(@Nonnull final FhirPath fhirPath) {
    super(fhirPath);
  }

}
