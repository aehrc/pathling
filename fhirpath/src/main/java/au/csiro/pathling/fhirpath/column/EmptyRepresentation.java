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

package au.csiro.pathling.fhirpath.column;

import static org.apache.spark.sql.functions.lit;

import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.function.UnaryOperator;
import org.apache.spark.sql.Column;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * A column representation that represents an empty/null value. Unlike
 * {@link DefaultRepresentation#empty()}, this representation handles all operations gracefully
 * without attempting to access Spark columns that would cause errors.
 * <p>
 * This is used for cross-resource references that don't exist in single-resource evaluation.
 * When a FHIRPath expression references a resource that isn't the subject resource, this
 * representation ensures all field accesses return null values instead of throwing errors.
 * <p>
 * All traversal and field access operations return another EmptyRepresentation, ensuring that
 * any chain of operations on an empty resource returns null.
 */
public class EmptyRepresentation extends ColumnRepresentation {

  private static final EmptyRepresentation INSTANCE = new EmptyRepresentation();
  private static final Column NULL_COLUMN = lit(null);

  private EmptyRepresentation() {
    // Private constructor - use getInstance()
  }

  /**
   * Gets the singleton instance of EmptyRepresentation.
   *
   * @return the singleton instance
   */
  @Nonnull
  public static EmptyRepresentation getInstance() {
    return INSTANCE;
  }

  @Override
  @Nonnull
  public Column getValue() {
    return NULL_COLUMN;
  }

  @Override
  @Nonnull
  public ColumnRepresentation copyOf(@Nonnull final Column newValue) {
    // Ignore the new value and return this - empty stays empty
    return this;
  }

  @Override
  @Nonnull
  public ColumnRepresentation vectorize(
      @Nonnull final UnaryOperator<Column> arrayExpression,
      @Nonnull final UnaryOperator<Column> singularExpression) {
    // Empty representation stays empty
    return this;
  }

  @Override
  @Nonnull
  public ColumnRepresentation flatten() {
    // Empty representation stays empty
    return this;
  }

  @Override
  @Nonnull
  public ColumnRepresentation traverse(@Nonnull final String fieldName) {
    // Traversing an empty representation returns another empty representation
    // This avoids the Spark error of trying to get a field from a null struct
    return this;
  }

  @Override
  @Nonnull
  public ColumnRepresentation traverse(@Nonnull final String fieldName,
      @Nonnull final Optional<FHIRDefinedType> fhirType) {
    // Traversing an empty representation returns another empty representation
    return this;
  }

  @Override
  @Nonnull
  public ColumnRepresentation getField(@Nonnull final String fieldName) {
    // Getting a field from an empty representation returns another empty representation
    return this;
  }

  @Override
  @Nonnull
  public ColumnRepresentation removeNulls() {
    // Already null/empty, no change needed
    return this;
  }

  @Override
  @Nonnull
  public ColumnRepresentation transform(@Nonnull final UnaryOperator<Column> transform) {
    // Transforming empty returns empty - don't apply the transform
    return this;
  }

  @Override
  @Nonnull
  public ColumnRepresentation asCanonical() {
    // Empty representation is already canonical
    return this;
  }
}
