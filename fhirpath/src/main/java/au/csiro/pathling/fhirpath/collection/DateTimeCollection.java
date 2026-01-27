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

package au.csiro.pathling.fhirpath.collection;

import static org.apache.spark.sql.functions.date_format;

import au.csiro.pathling.annotations.UsedByReflection;
import au.csiro.pathling.fhirpath.FhirPathDateTime;
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.StringCoercible;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.definition.NodeDefinition;
import jakarta.annotation.Nonnull;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.InstantType;

/**
 * Represents a collection of DateTime-typed elements.
 *
 * @author John Grimes
 */
public class DateTimeCollection extends Collection
    implements StringCoercible, Materializable, DateTimeComparable {

  private static final String SPARK_FHIRPATH_DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";

  /**
   * Creates a new DateTimeCollection.
   *
   * @param columnRepresentation the column representation for this collection
   * @param type the FhirPath type
   * @param fhirType the FHIR type
   * @param definition the node definition
   * @param extensionMapColumn the extension map column
   */
  protected DateTimeCollection(
      @Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final Optional<FhirPathType> type,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final Optional<? extends NodeDefinition> definition,
      @Nonnull final Optional<Column> extensionMapColumn) {
    super(columnRepresentation, type, fhirType, definition, extensionMapColumn);
  }

  /**
   * Returns a new instance with the specified column representation and definition.
   *
   * @param columnRepresentation The column representation to use
   * @param definition The definition to use
   * @return A new instance of {@link DateTimeCollection}
   */
  @Nonnull
  public static DateTimeCollection build(
      @Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final Optional<NodeDefinition> definition) {
    return new DateTimeCollection(
        columnRepresentation,
        Optional.of(FhirPathType.DATETIME),
        Optional.of(FHIRDefinedType.DATETIME),
        definition,
        Optional.empty());
  }

  /**
   * Returns a new instance with the specified column representation and no definition.
   *
   * @param columnRepresentation The column representation to use
   * @return A new instance of {@link DateTimeCollection}
   */
  @Nonnull
  public static DateTimeCollection build(@Nonnull final ColumnRepresentation columnRepresentation) {
    return DateTimeCollection.build(columnRepresentation, Optional.empty());
  }

  /**
   * Returns a new instance based upon a {@link DateTimeType}.
   *
   * @param value The value to use
   * @return A new instance of {@link DateTimeCollection}
   */
  @UsedByReflection
  @Nonnull
  public static DateTimeCollection fromValue(@Nonnull final DateTimeType value) {
    return DateTimeCollection.build(DefaultRepresentation.literal(value.getValueAsString()));
  }

  /**
   * Returns a new instance based upon a {@link InstantType}.
   *
   * @param value The value to use
   * @return A new instance of {@link DateTimeCollection}
   */
  @UsedByReflection
  @Nonnull
  public static DateTimeCollection fromValue(@Nonnull final InstantType value) {
    final Timestamp timestamp = new Timestamp(value.getValue().toInstant().toEpochMilli());
    final ColumnRepresentation column = new DefaultRepresentation(functions.lit(timestamp));
    return new DateTimeCollection(
        column,
        Optional.of(FhirPathType.DATETIME),
        Optional.of(FHIRDefinedType.INSTANT),
        Optional.empty(),
        Optional.empty());
  }

  /**
   * Returns a new instance, parsed from a FHIRPath literal.
   *
   * @param dateTimeLiteral The FHIRPath representation of the literal
   * @return A new instance of {@link DateTimeCollection}
   * @throws ParseException if the literal is malformed
   */
  @Nonnull
  public static DateTimeCollection fromLiteral(@Nonnull final String dateTimeLiteral)
      throws ParseException {
    final String dateTimeString = dateTimeLiteral.replaceFirst("^@", "");
    if (!FhirPathDateTime.isDateTimeValue(dateTimeString)) {
      throw new ParseException("Invalid dateTime literal: " + dateTimeLiteral, 0);
    }
    return DateTimeCollection.build(DefaultRepresentation.literal(dateTimeString));
  }

  @Nonnull
  @Override
  public StringCollection asStringPath() {
    final ColumnRepresentation valueColumn;
    final Optional<FHIRDefinedType> fhirType = getFhirType();
    if (fhirType.isPresent() && fhirType.get() == FHIRDefinedType.INSTANT) {
      valueColumn = getColumn().call(c -> date_format(c, SPARK_FHIRPATH_DATETIME_FORMAT));
    } else {
      valueColumn = getColumn();
    }
    return StringCollection.build(valueColumn);
  }
}
