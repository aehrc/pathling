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

import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

/**
 * The filter that restricts a derived schema to the elements the data populates.
 *
 * <p>It is a view over the schema of the source, which is the inferred schema of the incoming
 * documents rather than the data itself; presence is therefore a property of a schema and is
 * decided once, at planning time. The derivation asks it two things at every node: whether a field
 * was observed here, and what was observed beneath it.
 *
 * <p>It answers nothing about types or cardinality. Those come from the definitions, whatever shape
 * the source happened to take.
 */
public final class SchemaPruner {

  @Nonnull private final StructType observed;

  private SchemaPruner(@Nonnull final StructType observed) {
    this.observed = observed;
  }

  /**
   * Returns a filter over an observed structure.
   *
   * @param observed the structure observed in the source
   * @return the filter
   */
  @Nonnull
  public static SchemaPruner of(@Nonnull final StructType observed) {
    return new SchemaPruner(observed);
  }

  /**
   * Returns whether a complex element derived as the given structure survives pruning.
   *
   * <p>A structure with no fields is not a type anything can be read into, so a complex element
   * appears only where some descendant leaf is populated (FR-011).
   *
   * @param derived the structure derived for the element
   * @return true where the element survives
   */
  public static boolean retainsStructure(@Nonnull final StructType derived) {
    return derived.fields().length > 0;
  }

  /**
   * Returns whether a field was observed at this node.
   *
   * @param name the name of the field
   * @return true where the source carried it
   */
  public boolean retains(@Nonnull final String name) {
    return Stream.of(observed.fields()).anyMatch(field -> field.name().equals(name));
  }

  /**
   * Returns the filter for what was observed beneath a field, unwrapping an array so that a caller
   * need not know the cardinality the source happened to use.
   *
   * @param name the name of the field to descend into
   * @return the filter for that field, or empty where the field was not observed as a structure
   */
  @Nonnull
  public Optional<SchemaPruner> descend(@Nonnull final String name) {
    return Stream.of(observed.fields())
        .filter(field -> field.name().equals(name))
        .findFirst()
        .map(field -> elementTypeOf(field.dataType()))
        .filter(StructType.class::isInstance)
        .map(StructType.class::cast)
        .map(SchemaPruner::new);
  }

  @Nonnull
  private static DataType elementTypeOf(@Nonnull final DataType type) {
    return type instanceof final ArrayType array ? elementTypeOf(array.elementType()) : type;
  }
}
