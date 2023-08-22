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

import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.fhirpath.Comparable;
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.Materializable;
import au.csiro.pathling.fhirpath.StringCoercible;
import au.csiro.pathling.fhirpath.definition.NodeDefinition;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.TimeType;

/**
 * Represents a FHIRPath expression which refers to a time typed element.
 *
 * @author John Grimes
 */
public class TimeCollection extends Collection implements Materializable<TimeType>,
    Comparable, StringCoercible {

  public TimeCollection(@Nonnull final Column column,
      @Nonnull final Optional<NodeDefinition> definition) {
    super(column, Optional.of(FhirPathType.TIME), Optional.of(FHIRDefinedType.TIME), definition);
  }

  @Nonnull
  public static TimeCollection build(@Nonnull final Column column,
      @Nonnull final Optional<NodeDefinition> definition) {
    return new TimeCollection(column, definition);
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
    return TimeCollection.build(lit(timeString), Optional.empty());
  }

  @Nonnull
  @Override
  public Optional<TimeType> getFhirValueFromRow(@Nonnull final Row row, final int columnNumber) {
    if (row.isNullAt(columnNumber)) {
      return Optional.empty();
    }
    return Optional.of(new TimeType(row.getString(columnNumber)));
  }

  @Nonnull
  @Override
  public Collection asStringPath() {
    return StringCollection.build(getColumn().cast(DataTypes.StringType), Optional.empty());
  }

}
