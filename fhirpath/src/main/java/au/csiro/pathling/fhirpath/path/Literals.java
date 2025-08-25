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

package au.csiro.pathling.fhirpath.path;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.errors.UnsupportedFhirPathFeatureError;
import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.CodingCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.DecimalCollection;
import au.csiro.pathling.fhirpath.collection.EmptyCollection;
import au.csiro.pathling.fhirpath.collection.IntegerCollection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import jakarta.annotation.Nonnull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.experimental.UtilityClass;

/**
 * Utility class for creating and working with FHIRPath literal values.
 * <p>
 * This class provides factory methods for creating various types of literal values that can be used
 * in FHIRPath expressions, such as strings, booleans, integers, decimals, and codings. It also
 * includes methods for handling unsupported literal types.
 * </p>
 */
@UtilityClass
public class Literals {

  /**
   * Creates a null literal value.
   *
   * @return a new {@link NullLiteral} instance
   */
  @Nonnull
  public static NullLiteral nullLiteral() {
    return new NullLiteral();
  }

  /**
   * Creates a string literal value.
   *
   * @param literalValue the string value
   * @return a new {@link StringLiteral} instance
   */
  @Nonnull
  public static StringLiteral stringLiteral(@Nonnull final String literalValue) {
    return new StringLiteral(literalValue);
  }

  /**
   * Creates a boolean literal value.
   *
   * @param literalValue the boolean value as a string ("true" or "false")
   * @return a new {@link BooleanLiteral} instance
   */
  @Nonnull
  public static BooleanLiteral booleanLiteral(@Nonnull final String literalValue) {
    return new BooleanLiteral(literalValue);
  }

  /**
   * Creates a coding literal value.
   *
   * @param literalValue the coding value as a string
   * @return a new {@link CodingLiteral} instance
   */
  @Nonnull
  public static CodingLiteral codingLiteral(@Nonnull final String literalValue) {
    return new CodingLiteral(literalValue);
  }

  /**
   * Creates a date literal value.
   * <p>
   * Note: This operation is not currently supported and will throw an exception.
   * </p>
   *
   * @param literalValue the date value as a string
   * @return a new date literal instance
   * @throws UnsupportedOperationException always, as date literals are not supported
   */
  @SuppressWarnings("unused")
  public static LiteralPath dateLiteral(@Nonnull final String literalValue) {
    throw new UnsupportedFhirPathFeatureError("Date literals are not supported");
  }

  /**
   * Creates a dateTime literal value.
   * <p>
   * Note: This operation is not currently supported and will throw an exception.
   * </p>
   *
   * @param literalValue the dateTime value as a string
   * @return a new dateTime literal instance
   * @throws UnsupportedOperationException always, as dateTime literals are not supported
   */
  @SuppressWarnings("unused")
  public static LiteralPath dateTimeLiteral(@Nonnull final String literalValue) {
    throw new UnsupportedFhirPathFeatureError("DateTime literals are not supported");
  }

  /**
   * Creates a time literal value.
   * <p>
   * Note: This operation is not currently supported and will throw an exception.
   * </p>
   *
   * @param literalValue the time value as a string
   * @return a new time literal instance
   * @throws UnsupportedOperationException always, as time literals are not supported
   */
  @SuppressWarnings("unused")
  public static LiteralPath timeLiteral(@Nonnull final String literalValue) {
    throw new UnsupportedFhirPathFeatureError("DateTime literals are not supported");
  }

  /**
   * Creates an integer literal value.
   *
   * @param literalValue the integer value as a string
   * @return a new {@link IntegerLiteral} instance
   */
  @SuppressWarnings("WeakerAccess")
  @Nonnull
  public static IntegerLiteral integerLiteral(@Nonnull final String literalValue) {
    return new IntegerLiteral(literalValue);
  }

  /**
   * Creates a decimal literal value.
   *
   * @param literalValue the decimal value as a string
   * @return a new {@link DecimalLiteral} instance
   */
  @SuppressWarnings("WeakerAccess")
  @Nonnull
  public static DecimalLiteral decimalLiteral(@Nonnull final String literalValue) {
    return new DecimalLiteral(literalValue);
  }

  /**
   * Creates a calendar duration literal value.
   * <p>
   * Note: This operation is not currently supported and will throw an exception.
   * </p>
   *
   * @param literalValue the calendar duration value as a string
   * @return a new calendar duration literal instance
   * @throws UnsupportedOperationException always, as calendar duration literals are not supported
   */
  @SuppressWarnings({"WeakerAccess", "unused"})
  public static LiteralPath calendarDurationLiteral(
      @Nonnull final String literalValue) {
    throw new UnsupportedFhirPathFeatureError("Calendar duration literals are not supported");
  }

  /**
   * Creates a UCUM quantity literal value.
   * <p>
   * Note: This operation is not currently supported and will throw an exception.
   * </p>
   *
   * @param literalValue the UCUM quantity value as a string
   * @return a new UCUM quantity literal instance
   * @throws UnsupportedOperationException always, as UCUM quantity literals are not supported
   */
  @SuppressWarnings({"unused", "WeakerAccess"})
  public static LiteralPath ucumQuantityLiteral(@Nonnull final String literalValue) {
    throw new UnsupportedFhirPathFeatureError("Quantity literals are not supported");
  }

  /**
   * Creates a numeric literal value, determining whether it's an integer or decimal.
   * <p>
   * This method attempts to parse the value as an integer first. If that fails, it creates a
   * decimal literal instead.
   * </p>
   *
   * @param literalValue the numeric value as a string
   * @return a new {@link IntegerLiteral} or {@link DecimalLiteral} instance
   */
  public static LiteralPath numericLiteral(@Nonnull final String literalValue) {
    try {
      Integer.parseInt(literalValue);
      return Literals.integerLiteral(literalValue);
    } catch (final NumberFormatException e) {
      return Literals.decimalLiteral(literalValue);
    }
  }

  /**
   * Creates a quantity literal value with a unit.
   * <p>
   * Note: This operation delegates to either {@link #calendarDurationLiteral} or
   * {@link #ucumQuantityLiteral}, both of which are currently unsupported.
   * </p>
   *
   * @param literalValue the numeric value as a string
   * @param unit the unit of measurement
   * @param isUCUM whether the unit is a UCUM unit
   * @return a new quantity literal instance
   * @throws UnsupportedOperationException always, as quantity literals are not supported
   */
  public static LiteralPath quantityLiteral(@Nonnull final String literalValue,
      @Nonnull final String unit, final boolean isUCUM) {
    return isUCUM
           ? ucumQuantityLiteral(String.format("%s %s", literalValue, unit))
           : calendarDurationLiteral(String.format("%s %s", literalValue, unit));
  }

  /**
   * Interface for all literal path implementations.
   * <p>
   * This interface represents a FHIRPath literal value that can be used in expressions.
   * </p>
   */
  public interface LiteralPath extends FhirPath {

  }

  /**
   * Represents a null literal in FHIRPath.
   * <p>
   * This class implements a literal that represents an empty collection.
   * </p>
   */
  @Value
  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  public static class NullLiteral implements LiteralPath {

    @Override
    public Collection apply(@Nonnull final Collection input,
        @Nonnull final EvaluationContext context) {
      return EmptyCollection.getInstance();
    }

    @Nonnull
    @Override
    public String toExpression() {
      return "{}";
    }
  }

  /**
   * Represents a string literal in FHIRPath.
   * <p>
   * This class implements a literal that represents a string value.
   * </p>
   */
  @Value
  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  public static class StringLiteral implements LiteralPath {

    @Nonnull
    String value;

    @Override
    public StringCollection apply(@Nonnull final Collection input,
        @Nonnull final EvaluationContext context) {
      return StringCollection.fromLiteral(value);
    }

    @Nonnull
    @Override
    public String toExpression() {
      return value;
    }
  }

  /**
   * Represents a boolean literal in FHIRPath.
   * <p>
   * This class implements a literal that represents a boolean value (true or false).
   * </p>
   */
  @Value
  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  public static class BooleanLiteral implements LiteralPath {

    @Nonnull
    String value;

    @Override
    public BooleanCollection apply(@Nonnull final Collection input,
        @Nonnull final EvaluationContext context) {
      return BooleanCollection.fromLiteral(value);
    }

    @Nonnull
    @Override
    public String toExpression() {
      return value;
    }
  }

  /**
   * Represents a coding literal in FHIRPath.
   * <p>
   * This class implements a literal that represents a FHIR Coding value.
   * </p>
   */
  @Value
  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  public static class CodingLiteral implements LiteralPath {

    @Nonnull
    String value;

    @Override
    public CodingCollection apply(@Nonnull final Collection input,
        @Nonnull final EvaluationContext context) {
      try {
        return CodingCollection.fromLiteral(value);
      } catch (final IllegalArgumentException e) {
        throw new InvalidUserInputError(e.getMessage(), e);
      }
    }

    @Nonnull
    @Override
    public String toExpression() {
      return value;
    }
  }

  /**
   * Represents an integer literal in FHIRPath.
   * <p>
   * This class implements a literal that represents an integer value.
   * </p>
   */
  @Value
  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  public static class IntegerLiteral implements LiteralPath {

    @Nonnull
    String value;

    @Override
    public IntegerCollection apply(@Nonnull final Collection input,
        @Nonnull final EvaluationContext context) {
      return IntegerCollection.fromLiteral(value);
    }

    @Nonnull
    @Override
    public String toExpression() {
      return value;
    }
  }

  /**
   * Represents a decimal literal in FHIRPath.
   * <p>
   * This class implements a literal that represents a decimal (floating-point) value.
   * </p>
   */
  @Value
  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  public static class DecimalLiteral implements LiteralPath {

    @Nonnull
    String value;

    @Override
    public DecimalCollection apply(@Nonnull final Collection input,
        @Nonnull final EvaluationContext context) {
      return DecimalCollection.fromLiteral(value);
    }

    @Nonnull
    @Override
    public String toExpression() {
      return value;
    }
  }
}
