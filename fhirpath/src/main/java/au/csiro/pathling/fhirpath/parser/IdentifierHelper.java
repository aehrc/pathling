/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.fhirpath.parser;

import au.csiro.pathling.fhirpath.literal.StringLiteral;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.IdentifierContext;
import jakarta.annotation.Nonnull;

/**
 * Helper class for extracting identifier values from FHIRPath parser contexts.
 *
 * <p>This class handles both regular identifiers and delimited identifiers (enclosed in backticks),
 * properly unescaping and unquoting delimited identifiers according to FHIRPath rules.
 *
 * @author Piotr Szul
 */
public final class IdentifierHelper {

  private IdentifierHelper() {
    // Utility class
  }

  /**
   * Extracts the identifier value from an {@link IdentifierContext}.
   *
   * <p>If the identifier is delimited (enclosed in backticks), it will be unquoted and unescaped
   * according to FHIRPath string literal rules. Regular identifiers are returned as-is.
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>{@code Patient} → {@code "Patient"}
   *   <li>{@code `Patient`} → {@code "Patient"}
   *   <li>{@code `field-name`} → {@code "field-name"}
   *   <li>{@code `value\nwith\nnewlines`} → {@code "value\nwith\nnewlines"}
   * </ul>
   *
   * @param ctx the identifier context from the parser
   * @return the unquoted and unescaped identifier value
   * @see <a href="https://hl7.org/fhirpath/#string">FHIRPath String Literals</a>
   */
  @Nonnull
  public static String getIdentifierValue(@Nonnull final IdentifierContext ctx) {
    return ctx.DELIMITEDIDENTIFIER() != null
        ? StringLiteral.unquoteFhirIdentifier(ctx.DELIMITEDIDENTIFIER().getText())
        : ctx.IDENTIFIER().getText();
  }
}
