/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.assertions;

import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.fhirpath.element.ElementPath;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * @author John Grimes
 */
public class ElementPathAssertion extends BaseFhirPathAssertion<ElementPathAssertion> {

  @Nonnull
  private final ElementPath fhirPath;

  ElementPathAssertion(@Nonnull final ElementPath fhirPath) {
    super(fhirPath);
    this.fhirPath = fhirPath;
  }

  @Nonnull
  public ElementPathAssertion hasFhirType(@Nonnull final FHIRDefinedType type) {
    assertEquals(type, fhirPath.getFhirType());
    return this;
  }

}
