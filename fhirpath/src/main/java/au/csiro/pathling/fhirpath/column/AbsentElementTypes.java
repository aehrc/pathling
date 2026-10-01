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

package au.csiro.pathling.fhirpath.column;

import au.csiro.pathling.definition.ElementDefinition;
import au.csiro.pathling.schema.PrimitiveTypes;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import lombok.experimental.UtilityClass;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * The types of the null that traversal yields for an element absent from the input schema (FR-055).
 *
 * <p>The type is the one the definitions give the element wherever that type is unambiguous, and
 * the bottom type wherever a concrete shape would over-constrain later combination: a singular
 * primitive takes the storage type of its FHIR type, a repeating primitive an array of it, a
 * singular complex element the null type, and a repeating complex element an array of the null
 * type.
 *
 * <p>A decimal takes the text it is stored as, because the traversal expression normalises the
 * previous layout's decimals to that text too (T094b), and every branch of the traversal yields the
 * same type. The engine decodes the text to {@code DECIMAL(32,6)} after traversal (FR-035).
 *
 * @author Piotr Szul
 */
@UtilityClass
public class AbsentElementTypes {

  /**
   * Gets the type of the null that stands for an absent element.
   *
   * @param definition the definition of the element
   * @return the fallback type, per FR-055
   */
  @Nonnull
  public static DataType of(@Nonnull final ElementDefinition definition) {
    final DataType singular = singular(definition.getFhirType());
    return definition.isRepeating() ? DataTypes.createArrayType(singular) : singular;
  }

  /**
   * Gets the type of the null that stands for an absent singular element of the given FHIR type.
   *
   * @param fhirType the FHIR type of the element, if it is known
   * @return the storage type of a primitive, or the null type for a complex or unknown type
   */
  @Nonnull
  public static DataType singular(@Nonnull final Optional<FHIRDefinedType> fhirType) {
    return fhirType.flatMap(AbsentElementTypes::primitiveType).orElse(DataTypes.NullType);
  }

  @Nonnull
  private static Optional<DataType> primitiveType(@Nonnull final FHIRDefinedType fhirType) {
    // A type with no code, such as the null type, is not a primitive.
    return Optional.ofNullable(fhirType.toCode())
        .flatMap(code -> PrimitiveTypes.storageTypeOf(fhirType));
  }
}
