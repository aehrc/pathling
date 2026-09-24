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

package au.csiro.pathling.schema;

import au.csiro.pathling.definition.DefinitionContext;
import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Fixtures shared by the canonical structure tests: the R4 definitions, and the hand-built
 * structures that stand in for an inferred read schema.
 *
 * <p>The structures are hand-built rather than inferred from data, because this module may not
 * depend on anything that can run a query.
 */
final class SchemaFixtures {

  /** The FHIR R4 definitions, built once because the HAPI context is expensive to create. */
  @Nonnull
  static final DefinitionContext DEFINITIONS = FhirDefinitionContext.of(FhirContext.forR4());

  private SchemaFixtures() {}

  @Nonnull
  static StructType struct(@Nonnull final StructField... fields) {
    return new StructType(fields);
  }

  @Nonnull
  static StructField field(@Nonnull final String name, @Nonnull final DataType type) {
    return new StructField(name, type, true, Metadata.empty());
  }

  @Nonnull
  static ArrayType array(@Nonnull final DataType elementType) {
    return DataTypes.createArrayType(elementType, true);
  }

  /** Returns the names of the fields of a structure, in the order the structure carries them. */
  @Nonnull
  static List<String> names(@Nonnull final StructType structure) {
    return Stream.of(structure.fields()).map(StructField::name).toList();
  }

  /**
   * Returns the type reached by following a path of field names from a structure, unwrapping an
   * array wherever one is met, so that a caller can name elements without naming cardinality.
   */
  @Nonnull
  static DataType at(@Nonnull final StructType structure, @Nonnull final String... path) {
    DataType current = structure;
    for (final String name : path) {
      final StructType here = (StructType) elementTypeOf(current);
      current =
          Stream.of(here.fields())
              .filter(f -> f.name().equals(name))
              .findFirst()
              .orElseThrow(
                  () -> new AssertionError("No field named '" + name + "' in " + names(here)))
              .dataType();
    }
    return current;
  }

  /** Returns the structure reached by following a path of field names, unwrapping arrays. */
  @Nonnull
  static StructType structAt(@Nonnull final StructType structure, @Nonnull final String... path) {
    return (StructType) elementTypeOf(at(structure, path));
  }

  /** Returns the element type of an array, or the type itself where it is not an array. */
  @Nonnull
  static DataType elementTypeOf(@Nonnull final DataType type) {
    return type instanceof final ArrayType array ? array.elementType() : type;
  }
}
