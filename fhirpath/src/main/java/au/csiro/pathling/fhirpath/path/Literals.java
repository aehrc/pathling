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
import au.csiro.pathling.fhirpath.collection.EmptyCollection;
import au.csiro.pathling.fhirpath.collection.QuantityCollection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import jakarta.annotation.Nonnull;
import lombok.Value;
import org.fhir.ucum.UcumException;

public final class Literals {

  private Literals() {
  }


  @Value
  public static class NullLiteral implements FhirPath {

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
  public static class StringLiteral implements FhirPath {

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
      // TODO: use a better conversion
      return "'" + value + "'";
    }
  }

  @Value
  public static class BooleanLiteral implements FhirPath {

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
  public static class CodingLiteral implements FhirPath {

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
  public static class CalendarDurationLiteral implements FhirPath {

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
  public static class UcumQuantityLiteral implements FhirPath {

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

}
