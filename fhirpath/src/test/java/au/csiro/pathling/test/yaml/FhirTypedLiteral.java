/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.test.yaml;

import au.csiro.pathling.definition.FhirType;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import lombok.Value;

@JsonSerialize(using = YamlSupport.FhirTypedLiteralSerializer.class)
@Value(staticConstructor = "of")
public class FhirTypedLiteral {

  @Nonnull FhirType type;
  @Nullable String literal;

  @Nonnull
  public String getTag() {
    return FhirTypedLiteral.toTag(type);
  }

  @Nonnull
  public static String toTag(final FhirType type) {
    return "!fhir." + type.toCode();
  }

  @Nonnull
  public static FhirTypedLiteral toCoding(@Nullable final String literal) {
    return of(FhirType.CODING, literal);
  }

  @Nonnull
  public static FhirTypedLiteral toDateTime(@Nullable final String literal) {
    return of(FhirType.DATETIME, literal);
  }

  @Nonnull
  public static FhirTypedLiteral toDate(@Nullable final String literal) {
    return of(FhirType.DATE, literal);
  }

  @Nonnull
  public static FhirTypedLiteral toTime(@Nullable final String literal) {
    return of(FhirType.TIME, literal);
  }

  /**
   * Creates a String typed literal.
   *
   * @param literal The String value
   * @return A new FhirTypedLiteral
   */
  @Nonnull
  public static FhirTypedLiteral toString(@Nullable final String literal) {
    return of(FhirType.STRING, literal);
  }

  /**
   * Creates an Integer typed literal.
   *
   * @param literal The Integer value as a string
   * @return A new FhirTypedLiteral
   */
  @Nonnull
  public static FhirTypedLiteral toInteger(@Nullable final String literal) {
    return of(FhirType.INTEGER, literal);
  }

  /**
   * Creates a Decimal typed literal.
   *
   * @param literal The Decimal value as a string
   * @return A new FhirTypedLiteral
   */
  @Nonnull
  public static FhirTypedLiteral toDecimal(@Nullable final String literal) {
    return of(FhirType.DECIMAL, literal);
  }

  /**
   * Creates a Boolean typed literal.
   *
   * @param literal The Boolean value as a string
   * @return A new FhirTypedLiteral
   */
  @Nonnull
  public static FhirTypedLiteral toBoolean(@Nullable final String literal) {
    return of(FhirType.BOOLEAN, literal);
  }

  /**
   * Creates a Quantity typed literal from a FHIRPath quantity literal string.
   *
   * <p>Use FHIRPath syntax with quoted units (e.g., "10.5 'mg'", "70 'kg'")
   *
   * @param literal the FHIRPath quantity literal string
   * @return A new FhirTypedLiteral
   */
  @Nonnull
  public static FhirTypedLiteral toQuantity(@Nullable final String literal) {
    return of(FhirType.QUANTITY, literal);
  }
}
