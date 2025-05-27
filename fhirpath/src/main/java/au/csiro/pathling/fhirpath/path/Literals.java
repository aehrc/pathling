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

@UtilityClass
public class Literals {
  
  public static NullLiteral nullLiteral() {
    return new NullLiteral();
  }

  public static StringLiteral stringLiteral(@Nonnull final String literalValue) {
    return new StringLiteral(literalValue);
  }

  public static BooleanLiteral booleanLiteral(@Nonnull final String literalValue) {
    return new BooleanLiteral(literalValue);
  }

  public static CodingLiteral codingLiteral(@Nonnull final String literalValue) {
    return new CodingLiteral(literalValue);
  }

  @SuppressWarnings("unused")
  public static LiteralPath dateLiteral(@Nonnull final String literalValue) {
    throw new UnsupportedOperationException("Date literals are not supported");
  }

  @SuppressWarnings("unused")
  public static LiteralPath dateTimeLiteral(@Nonnull final String literalValue) {
    throw new UnsupportedOperationException("DateTime literals are not supported");
  }

  @SuppressWarnings("unused")
  public static LiteralPath timeLiteral(@Nonnull final String literalValue) {
    throw new UnsupportedOperationException("DateTime literals are not supported");
  }

  public static IntegerLiteral integerLiteral(@Nonnull final String literalValue) {
    return new IntegerLiteral(literalValue);
  }

  public static DecimalLiteral decimalLiteral(@Nonnull final String literalValue) {
    return new DecimalLiteral(literalValue);
  }

  @SuppressWarnings("unused")
  public static LiteralPath calendarDurationLiteral(
      @Nonnull final String literalValue) {
    throw new UnsupportedOperationException("CalendarDurationLiteral literals are not supported");
  }

  @SuppressWarnings("unused")
  public static LiteralPath ucumQuantityLiteral(@Nonnull final String literalValue) {
    throw new UnsupportedOperationException("UcumQuantityLiteral literals are not supported");
  }

  public static LiteralPath numericLiteral(@Nonnull final String literalValue) {
    try {
      Integer.parseInt(literalValue);
      return Literals.integerLiteral(literalValue);
    } catch (final NumberFormatException e) {
      return Literals.decimalLiteral(literalValue);
    }
  }

  public static LiteralPath quantityLiteral(@Nonnull final String literalValue,
      @Nonnull final String unit, boolean isUCUM) {
    return isUCUM
           ? calendarDurationLiteral(String.format("%s %s", literalValue, unit))
           : ucumQuantityLiteral(String.format("%s %s", literalValue, unit));
  }

  public interface LiteralPath extends FhirPath {

  }

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
   * Integer literal.
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
   * Decimal literal.
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
