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
import au.csiro.pathling.fhirpath.FhirPathDateTime;
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.StringCoercible;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.definition.NodeDefinition;
import jakarta.annotation.Nonnull;
import java.text.ParseException;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.hl7.fhir.r4.model.DateType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents a collection of Date-typed elements.
 *
 * @author John Grimes
 */
@Slf4j
public class DateCollection extends Collection implements StringCoercible, Materializable,
    DateTimeComparable {
  
  /**
   * Creates a new DateCollection.
   *
   * @param columnRepresentation the column representation for this collection
   * @param type the FhirPath type
   * @param fhirType the FHIR type
   * @param definition the node definition
   * @param extensionMapColumn the extension map column
   */
  protected DateCollection(@Nonnull final ColumnRepresentation columnRepresentation,
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
   * @return A new instance of {@link DateCollection}
   */
  @Nonnull
  public static DateCollection build(@Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final Optional<NodeDefinition> definition) {
    return new DateCollection(columnRepresentation, Optional.of(FhirPathType.DATE),
        Optional.of(FHIRDefinedType.DATE), definition, Optional.empty());
  }

  @Nonnull
  private static DateCollection build(@Nonnull final ColumnRepresentation columnRepresentation) {
    return DateCollection.build(columnRepresentation, Optional.empty());
  }

  /**
   * Returns a new instance based upon a {@link DateType}.
   *
   * @param value The value to use
   * @return A new instance of {@link DateCollection}
   */
  @UsedByReflection
  @Nonnull
  public static DateCollection fromValue(@Nonnull final DateType value) {
    return DateCollection.build(DefaultRepresentation.literal(value.getValueAsString()));
  }

  /**
   * Returns a new instance, parsed from a FHIRPath literal.
   *
   * @param dateLiteral The FHIRPath representation of the literal
   * @return A new instance of {@link DateCollection}
   * @throws ParseException if the literal is malformed
   */
  @Nonnull
  public static DateCollection fromLiteral(@Nonnull final String dateLiteral)
      throws ParseException {
    if (!FhirPathDateTime.isDateLiteral(dateLiteral)) {
      throw new ParseException("Invalid dateTime literal: " + dateLiteral, 0);
    }
    final String dateString = dateLiteral.replaceFirst("^@", "");
    return DateCollection.build(DefaultRepresentation.literal(dateString));
  }

  @Nonnull
  @Override
  public StringCollection asStringPath() {
    return Collection.defaultAsStringPath(this);
  }

}
