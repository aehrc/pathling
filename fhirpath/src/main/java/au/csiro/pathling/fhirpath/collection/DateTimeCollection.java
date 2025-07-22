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

import static org.apache.spark.sql.functions.date_format;

import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.StringCoercible;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.comparison.ColumnComparator;
import au.csiro.pathling.fhirpath.comparison.Comparable;
import au.csiro.pathling.fhirpath.definition.NodeDefinition;
import au.csiro.pathling.fhirpath.operator.DateTimeComparator;
import au.csiro.pathling.fhirpath.operator.DefaultComparator;
import jakarta.annotation.Nonnull;
import java.sql.Timestamp;
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
public class DateTimeCollection extends Collection implements StringCoercible, Materializable,
    Comparable {

  private static final String SPARK_FHIRPATH_DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
  private static final ColumnComparator DATE_TIME_COMPARATOR = new DateTimeComparator();
  private static final ColumnComparator INSTANT_COMPARATOR = new DefaultComparator();

  protected DateTimeCollection(@Nonnull final ColumnRepresentation columnRepresentation,
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
  public static DateTimeCollection build(@Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final Optional<NodeDefinition> definition) {
    return new DateTimeCollection(columnRepresentation, Optional.of(FhirPathType.DATETIME),
        Optional.of(FHIRDefinedType.DATETIME), definition, Optional.empty());
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
  @Nonnull
  public static DateTimeCollection fromValue(@Nonnull final DateTimeType value) {
    return DateTimeCollection.build(
        DefaultRepresentation.literal(value.getValueAsString()));
  }

  /**
   * Returns a new instance based upon a {@link InstantType}.
   *
   * @param value The value to use
   * @return A new instance of {@link DateTimeCollection}
   */
  @Nonnull
  public static DateTimeCollection fromValue(@Nonnull final InstantType value) {
    final Timestamp timestamp = new Timestamp(
        value.getValue().toInstant().toEpochMilli());
    final ColumnRepresentation column = new DefaultRepresentation(functions.lit(timestamp));
    return new DateTimeCollection(
        column,
        Optional.of(FhirPathType.DATETIME),
        Optional.of(FHIRDefinedType.INSTANT),
        Optional.empty(), Optional.empty());
  }

  @Nonnull
  @Override
  public StringCollection asStringPath() {
    final ColumnRepresentation valueColumn;
    final Optional<FHIRDefinedType> fhirType = getFhirType();
    if (fhirType.isPresent() && fhirType.get() == FHIRDefinedType.INSTANT) {
      valueColumn = getColumn().call(
          c -> date_format(c, SPARK_FHIRPATH_DATETIME_FORMAT));
    } else {
      valueColumn = getColumn();
    }
    return StringCollection.build(valueColumn);
  }

  @Nonnull
  @Override
  public ColumnComparator getComparator() {
    final Optional<FHIRDefinedType> fhirType = getFhirType();
    if (fhirType.isPresent() && fhirType.get() == FHIRDefinedType.INSTANT) {
      return INSTANT_COMPARATOR;
    }
    return DATE_TIME_COMPARATOR;
  }

  @Override
  public boolean isComparableTo(@Nonnull final Comparable path) {
    if (!(path instanceof final DateTimeCollection other)) {
      return false;
    }

    final Optional<FHIRDefinedType> thisFhirType = getFhirType();
    final Optional<FHIRDefinedType> otherFhirType = other.getFhirType();

    // Both must have FHIR types defined.
    if (thisFhirType.isEmpty() || otherFhirType.isEmpty()) {
      return false;
    }

    // DateTime and Instant types cannot be compared to each other. This is because we currently
    // represent Instant using the TIMESTAMP type in Spark, which involves changing the precision
    // from the original source data. The original precision is necessary for correct comparison.
    return thisFhirType.get() == otherFhirType.get();
  }
}
