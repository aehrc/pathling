/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.assertions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.fhirpath.FhirPath;
import javax.annotation.Nonnull;

/**
 * @author Piotr Szul
 * @author John Grimes
 */
public class FhirPathAssertion<T extends FhirPathAssertion> {

  @Nonnull
  private final FhirPath fhirPath;

  FhirPathAssertion(@Nonnull final FhirPath fhirPath) {
    this.fhirPath = fhirPath;
  }

  @Nonnull
  public DatasetAssert selectResult() {
    return new DatasetAssert(fhirPath.getDataset()
        .select(fhirPath.getIdColumn(), fhirPath.getValueColumn())
        .orderBy(fhirPath.getIdColumn(), fhirPath.getValueColumn()));
  }

  @Nonnull
  public T hasExpression(@Nonnull final String expression) {
    assertEquals(expression, fhirPath.getExpression());
    return self();
  }

  public T isSingular() {
    assertTrue(fhirPath.isSingular());
    return self();
  }

  public T isNotSingular() {
    assertFalse(fhirPath.isSingular());
    return self();
  }

  @SuppressWarnings("unchecked")
  private T self() {
    return (T) this;
  }

}
