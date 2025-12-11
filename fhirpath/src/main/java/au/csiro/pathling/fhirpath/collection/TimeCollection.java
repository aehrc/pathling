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

package au.csiro.pathling.fhirpath.collection;

import au.csiro.pathling.annotations.UsedByReflection;
import au.csiro.pathling.fhirpath.FhirPathTime;
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.StringCoercible;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.comparison.ColumnComparator;
import au.csiro.pathling.fhirpath.comparison.Comparable;
import au.csiro.pathling.fhirpath.comparison.TemporalComparator;
import au.csiro.pathling.fhirpath.definition.NodeDefinition;
import jakarta.annotation.Nonnull;
import java.text.ParseException;
import java.util.Optional;
import org.apache.jena.reasoner.rulesys.Rule.ParserException;
import org.apache.spark.sql.Column;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.TimeType;

/**
 * Represents a FHIRPath expression which refers to a time typed element.
 *
 * @author John Grimes
 */
public class TimeCollection extends Collection implements StringCoercible, Materializable,
    Comparable {

  /**
   * Creates a new TimeCollection with the specified parameters.
   *
   * @param columnRepresentation the column representation for this collection
   * @param type the FHIRPath type of this collection
   * @param fhirType the FHIR defined type of this collection
   * @param definition the node definition for this collection
   * @param extensionMapColumn the extension map column for this collection
   */
  protected TimeCollection(@Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final Optional<FhirPathType> type,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final Optional<? extends NodeDefinition> definition,
      @Nonnull final Optional<Column> extensionMapColumn) {
    super(columnRepresentation, type, fhirType, definition, extensionMapColumn);
  }

  /**
   * Returns a new instance with the specified column and definition.
   *
   * @param columnRepresentation The column to use
   * @param definition The definition to use
   * @return A new instance of {@link TimeCollection}
   */
  @Nonnull
  public static TimeCollection build(@Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final Optional<NodeDefinition> definition) {
    return new TimeCollection(columnRepresentation, Optional.of(FhirPathType.TIME),
        Optional.of(FHIRDefinedType.TIME), definition, Optional.empty());
  }

  /**
   * Returns a new instance with the specified column and nno definition.
   *
   * @param columnRepresentation The column to use
   * @return A new instance of {@link TimeCollection}
   */
  @Nonnull
  public static TimeCollection build(@Nonnull final ColumnRepresentation columnRepresentation) {
    return build(columnRepresentation, Optional.empty());
  }


  /**
   * Returns a new instance, parsed from a FHIRPath literal.
   *
   * @param literal The FHIRPath representation of the literal
   * @return A new instance of {@link TimeCollection}
   * @throws ParserException if the literal is malformed
   * @throws ParseException if the literal cannot be parsed to a valid time
   */
  @Nonnull
  public static TimeCollection fromLiteral(@Nonnull final String literal) throws ParseException {
    final String timeString = literal.replaceFirst("^@T", "");
    if (!FhirPathTime.isTimeValue(timeString)) {
      throw new ParseException("Invalid dateTime literal: " + literal, 0);
    }
    return TimeCollection.build(DefaultRepresentation.literal(timeString));
  }

  /**
   * Returns a new instance based upon a {@link TimeType}.
   *
   * @param value The value to use
   * @return A new instance of {@link TimeCollection}
   */
  @UsedByReflection
  @Nonnull
  public static TimeCollection fromValue(@Nonnull final TimeType value) {
    return TimeCollection.build(DefaultRepresentation.literal(value.getValueAsString()));
  }

  /**
   * Converts this time collection to a string collection using element-wise transformation.
   *
   * @return StringCollection representation of this time collection
   */
  @Nonnull
  public StringCollection asStringCollection() {
    return map(ColumnRepresentation::asString, StringCollection::build);
  }

  @Nonnull
  @Override
  public StringCollection asStringPath() {
    return map(ColumnRepresentation::asString, StringCollection::build);
  }

  @Override
  @Nonnull
  public ColumnComparator getComparator() {
    return TemporalComparator.forTime();
  }

  @Override
  public boolean isComparableTo(@Nonnull final Collection target) {
    return target instanceof TimeCollection;
  }

}
