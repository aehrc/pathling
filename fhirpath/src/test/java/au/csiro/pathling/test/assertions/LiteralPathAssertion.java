/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.test.assertions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.fhirpath.literal.CodingLiteralPath;
import au.csiro.pathling.fhirpath.literal.LiteralPath;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.function.Function;
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
  public LiteralPathAssertion hasCodingValue(@Nonnull final Coding expectedCoding) {
    assertTrue(fhirPath instanceof CodingLiteralPath);
    final Coding actualCoding = ((CodingLiteralPath) fhirPath).getValue();
    assertTrue(expectedCoding.equalsDeep(actualCoding));
    return this;
  }
}
