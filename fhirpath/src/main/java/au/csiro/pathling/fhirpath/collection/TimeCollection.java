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

import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.StringCoercible;
import au.csiro.pathling.fhirpath.annotations.SqlPrimitive;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.definition.NodeDefinition;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.TimeType;

/**
 * Represents a FHIRPath expression which refers to a time typed element.
 *
 * @author John Grimes
 */
@SqlPrimitive
public class TimeCollection extends Collection implements StringCoercible {

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
   */
  @Nonnull
  public static TimeCollection fromLiteral(@Nonnull final String literal) {
    final String timeString = literal.replaceFirst("^@T", "");
    return TimeCollection.build(DefaultRepresentation.literal(timeString));
  }

  /**
   * Returns a new instance based upon a {@link TimeType}.
   *
   * @param value The value to use
   * @return A new instance of {@link TimeCollection}
   */
  @Nonnull
  public static TimeCollection fromValue(@Nonnull final TimeType value) {
    return TimeCollection.build(DefaultRepresentation.literal(value.getValueAsString()));
  }

  @Nonnull
  @Override
  public StringCollection asStringPath() {
    return map(ColumnRepresentation::asString, StringCollection::build);
  }
}
