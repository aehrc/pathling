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
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.definition.NodeDefinition;
import au.csiro.pathling.sql.misc.QuantityToLiteral;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents a FHIRPath expression which refers to an element of type Quantity.
 *
 * @author John Grimes
 */
public class QuantityCollection extends Collection implements StringCoercible {

  /**
   * @param columnRepresentation The column representation to use
   * @param type The FHIRPath type
   * @param fhirType The FHIR type
   * @param definition The FHIR definition
   */
  public QuantityCollection(@Nonnull final ColumnRepresentation columnRepresentation,
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
   * @return A new instance of {@link QuantityCollection}
   */
  @Nonnull
  public static QuantityCollection build(@Nonnull final ColumnRepresentation columnRepresentation,
      @Nonnull final Optional<? extends NodeDefinition> definition) {
    return new QuantityCollection(columnRepresentation, Optional.of(FhirPathType.QUANTITY),
        Optional.of(FHIRDefinedType.QUANTITY), definition, Optional.empty());
  }

  /**
   * Returns a new instance with the specified columnCtx and unknown definition.
   *
   * @param columnRepresentation The columnCtx to use
   * @return A new instance of {@link QuantityCollection}
   */
  @Nonnull
  public static QuantityCollection build(@Nonnull final ColumnRepresentation columnRepresentation) {
    return build(columnRepresentation, Optional.empty());
  }

  @Nonnull
  @Override
  public StringCollection asStringPath() {
    return asSingular()
        .map(r -> r.callUdf(QuantityToLiteral.FUNCTION_NAME), StringCollection::build);
  }
}
