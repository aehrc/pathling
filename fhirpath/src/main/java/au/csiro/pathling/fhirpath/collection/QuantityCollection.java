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

package au.csiro.pathling.fhirpath.collection;

import static au.csiro.pathling.fhirpath.CalendarDurationUtils.parseCalendarDuration;
import static au.csiro.pathling.fhirpath.collection.DecimalCollection.parseLiteral;
import static au.csiro.pathling.fhirpath.collection.StringCollection.parseStringLiteral;

import au.csiro.pathling.encoders.terminology.ucum.Ucum;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.StringCoercible;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.definition.NodeDefinition;
import au.csiro.pathling.fhirpath.encoding.QuantityEncoding;
import au.csiro.pathling.sql.misc.QuantityToLiteral;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.math.BigDecimal;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.spark.sql.Column;
import org.fhir.ucum.UcumService;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Quantity;

/**
 * Represents a FHIRPath expression which refers to an element of type Quantity.
 *
 * @author John Grimes
 */
public class QuantityCollection extends Collection implements StringCoercible {

  private static final Pattern UCUM_PATTERN = Pattern.compile("([0-9.]+) ('[^']+')");

  /**
   * @param columnRepresentation The column representation to use
   * @param type The FHIRPath type
   * @param fhirType The FHIR type
   * @param definition The FHIR definition
   */
  public QuantityCollection(@Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final Optional<FhirPathType> type,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final Optional<? extends NodeDefinition> definition,
      @Nonnull final Optional<Column> extensionMapColumn) {
    super(columnRepresentation, type, fhirType, definition, extensionMapColumn);
  }

  /**
   * Returns a new instance with the specified columnCtx and definition.
   *
   * @param columnRepresentation The columnCtx to use
   * @param definition The definition to use
   * @return A new instance of {@link QuantityCollection}
   */
  @Nonnull
  public static QuantityCollection build(@Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final Optional<? extends NodeDefinition> definition) {
    return new QuantityCollection(columnRepresentation, Optional.of(FhirPathType.QUANTITY),
        Optional.of(FHIRDefinedType.QUANTITY), definition, Optional.empty());
  }

  /**
   * Returns a new instance with the specified columnCtx and unknown definition.
   *
   * @param columnRepresentation The columnCtx to use
   * @return A new instance of {@link QuantityCollection}
   */
  @Nonnull
  public static QuantityCollection build(@Nonnull final ColumnRepresentation columnRepresentation) {
    return build(columnRepresentation, Optional.empty());
  }

  /**
   * Returns a new instance, parsed from a FHIRPath literal.
   *
   * @param fhirPath the FHIRPath representation of the literal
   * @param ucumService a UCUM service for validating the unit within the literal
   * @return A new instance of {@link QuantityCollection}
   * @throws IllegalArgumentException if the literal is malformed
   */
  @Nonnull
  public static QuantityCollection fromUcumString(@Nonnull final String fhirPath,
      @Nonnull final UcumService ucumService) {
    final Matcher matcher = UCUM_PATTERN.matcher(fhirPath);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("UCUM Quantity literal has invalid format: " + fhirPath);
    }
    final String fullPath = matcher.group(0);
    final String value = matcher.group(1);
    final String rawUnit = matcher.group(2);
    final String unit = parseStringLiteral(rawUnit);

    @Nullable final String validationResult = ucumService.validate(unit);
    if (validationResult != null) {
      throw new InvalidUserInputError(
          "Invalid UCUM unit provided within Quantity literal (" + fullPath + "): "
              + validationResult);
    }

    final BigDecimal decimalValue = getQuantityValue(value);
    @Nullable final String display = ucumService.getCommonDisplay(unit);

    return buildLiteralPath(decimalValue, unit, Optional.ofNullable(display));
  }

  /**
   * Returns a new instance, parsed from a FHIRPath literal representing a calendar duration.
   *
   * @param fhirPath the FHIRPath representation of the literal
   * @return A new instance of {@link QuantityCollection}
   * @see <a href="https://hl7.org/fhirpath/#time-valued-quantities">Time-valued quantities</a>
   */
  @Nonnull
  public static QuantityCollection fromCalendarDurationString(@Nonnull final String fhirPath) {
    final Column column = QuantityEncoding.encodeLiteral(parseCalendarDuration(fhirPath));
    return QuantityCollection.build(new DefaultRepresentation(column));
  }

  @Nonnull
  private static BigDecimal getQuantityValue(@Nonnull final String value) {
    final BigDecimal decimalValue;
    try {
      decimalValue = parseLiteral(value);
    } catch (final NumberFormatException e) {
      throw new IllegalArgumentException("Quantity literal has invalid value: " + value);
    }
    return decimalValue;
  }

  @Nonnull
  private static QuantityCollection buildLiteralPath(@Nonnull final BigDecimal decimalValue,
      @Nonnull final String unit, @Nonnull final Optional<String> display) {
    final Quantity quantity = new Quantity();
    quantity.setValue(decimalValue);
    quantity.setSystem(Ucum.SYSTEM_URI);
    quantity.setCode(unit);
    display.ifPresent(quantity::setUnit);
    return QuantityCollection.build(
        new DefaultRepresentation(QuantityEncoding.encodeLiteral(quantity)));
  }

  @Nonnull
  @Override
  public StringCollection asStringPath() {
    return asSingular()
        .map(r -> r.callUdf(QuantityToLiteral.FUNCTION_NAME), StringCollection::build);
  }
}
