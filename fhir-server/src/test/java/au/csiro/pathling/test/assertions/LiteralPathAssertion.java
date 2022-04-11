/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.assertions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import au.csiro.pathling.fhirpath.literal.CodingLiteralPath;
import au.csiro.pathling.fhirpath.literal.LiteralPath;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hl7.fhir.r4.model.Coding;

/**
 * @author John Grimes
 */
@SuppressWarnings("UnusedReturnValue")
public class LiteralPathAssertion extends BaseFhirPathAssertion<LiteralPathAssertion> {

  @Nonnull
  private final LiteralPath fhirPath;

  LiteralPathAssertion(@Nonnull final LiteralPath fhirPath) {
    super(fhirPath);
    this.fhirPath = fhirPath;
  }

  @Nonnull
  public LiteralPathAssertion has(@Nullable final Object expected,
      @Nonnull final Function<LiteralPath, Object> function) {
    assertEquals(expected, function.apply(fhirPath));
    return this;
  }

  @Nonnull
  public LiteralPathAssertion hasCodingValue(@Nonnull final Coding coding) {
    assertTrue(fhirPath instanceof CodingLiteralPath);
    final SimpleCoding actualCoding = new SimpleCoding(
        ((CodingLiteralPath) fhirPath).getValue());
    final SimpleCoding expectedCoding = new SimpleCoding(coding);
    assertEquals(expectedCoding, actualCoding);
    return this;
  }

}
