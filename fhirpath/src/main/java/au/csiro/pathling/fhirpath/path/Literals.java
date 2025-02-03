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

import au.csiro.pathling.encoders.terminology.ucum.Ucum;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.CodingCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.DateCollection;
import au.csiro.pathling.fhirpath.collection.DateTimeCollection;
import au.csiro.pathling.fhirpath.collection.DecimalCollection;
import au.csiro.pathling.fhirpath.collection.EmptyCollection;
import au.csiro.pathling.fhirpath.collection.IntegerCollection;
import au.csiro.pathling.fhirpath.collection.QuantityCollection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.collection.TimeCollection;
import jakarta.annotation.Nonnull;
import lombok.Value;
import lombok.experimental.UtilityClass;
import org.fhir.ucum.UcumException;
import java.text.ParseException;

@UtilityClass
public class Literals {

  public interface LiteralPath extends FhirPath {

  }


  @Value
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

  @Value
  public static class CalendarDurationLiteral implements LiteralPath {

    @Nonnull
    String value;

    @Override
    public QuantityCollection apply(@Nonnull final Collection input,
        @Nonnull final EvaluationContext context) {
      return QuantityCollection.fromCalendarDurationString(value);
    }

    @Nonnull
    @Override
    public String toExpression() {
      return value;
    }
  }

  @Value
  public static class UcumQuantityLiteral implements LiteralPath {

    @Nonnull
    String value;

    @Override
    public QuantityCollection apply(@Nonnull final Collection input,
        @Nonnull final EvaluationContext context) {
      try {
        return QuantityCollection.fromUcumString(value, Ucum.service());
      } catch (final UcumException e) {
        throw new RuntimeException(e);
      }
    }

    @Nonnull
    @Override
    public String toExpression() {
      return value;
    }
  }

  /**
   * Date literal.
   */
  @Value
  public static class DateLiteral implements LiteralPath {

    @Nonnull
    String value;

    @Override
    public DateCollection apply(@Nonnull final Collection input,
        @Nonnull final EvaluationContext context) {
      try {
        return DateCollection.fromLiteral(value);
      } catch (final ParseException e) {
        throw new InvalidUserInputError("Unable to parse date format: " + value);
      }
    }

    @Nonnull
    @Override
    public String toExpression() {
      return value;
    }
  }

  /**
   * DateTime literal.
   */
  @Value
  public static class DateTimeLiteral implements LiteralPath {

    @Nonnull
    String value;

    @Override
    public DateTimeCollection apply(@Nonnull final Collection input,
        @Nonnull final EvaluationContext context) {
      try {
        return DateTimeCollection.fromLiteral(value);
      } catch (final ParseException e) {
        throw new InvalidUserInputError("Unable to parse date/time format: " + value);
      }
    }

    @Nonnull
    @Override
    public String toExpression() {
      return value;
    }
  }

  /**
   * Time literal.
   */
  @Value
  public static class TimeLiteral implements LiteralPath {

    @Nonnull
    String value;

    @Override
    public TimeCollection apply(@Nonnull final Collection input,
        @Nonnull final EvaluationContext context) {
      return TimeCollection.fromLiteral(value);
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
