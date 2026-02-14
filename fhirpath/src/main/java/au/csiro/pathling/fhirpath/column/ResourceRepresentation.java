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

import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.function.UnaryOperator;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents a FHIR resource at the root level of a flat schema dataset where top-level fields are
 * accessed directly via {@code col(fieldName)} rather than through nested struct access.
 *
 * <p>This representation is used when generating SparkSQL Column expressions that work directly on
 * Pathling-encoded flat datasets, eliminating the need for schema wrapping/unwrapping operations.
 *
 * <p>The representation holds an "existence column" (typically the {@code id} column) that can be
 * used to check whether a resource exists. Key behaviors:
 *
 * <ul>
 *   <li>{@link #getValue()} returns the existence column - this represents whether the resource
 *       exists (non-NULL for valid resources, NULL for empty collections)
 *   <li>{@link #existenceColumn} returns the id column for existence checks
 *   <li>{@link #vectorize(UnaryOperator, UnaryOperator)} applies the singular expression and
 *       returns a new ResourceRepresentation (resources are always singular)
 *   <li>{@link #flatten()} returns {@code this} unchanged since resources are already flat
 *   <li>{@link #traverse(String)} returns a {@link DefaultRepresentation} with {@code
 *       col(fieldName)} - subsequent traversals use {@code getField()}
 * </ul>
 *
 * <p>Example column generation differences:
 *
 * <pre>
 * Expression          | Nested Schema                              | Resource (Flat) Schema
 * --------------------|--------------------------------------------|--------------------------
 * Patient.name        | col("Patient").getField("name")            | col("name")
 * Patient.name.family | col("Patient").getField("name")            | col("name").getField("family")
 *                     |    .getField("family")                     |
 * </pre>
 *
 * @author Piotr Szul
 * @see DefaultRepresentation
 */
@EqualsAndHashCode(callSuper = false)
public final class ResourceRepresentation extends ColumnRepresentation {

  /** Default name for the existence column (resource id). */
  public static final String DEFAULT_EXISTENCE_COLUMN = "id";

  /**
   * The existence column that represents whether a resource exists. This is typically the id column
   * - non-NULL for valid resources, NULL for empty collections.
   */
  @Nonnull @Getter private final Column existenceColumn;

  /**
   * Private constructor for creating instances with a specific existence column.
   *
   * @param existenceColumn the column representing resource existence
   */
  private ResourceRepresentation(@Nonnull final Column existenceColumn) {
    this.existenceColumn = existenceColumn;
  }

  /**
   * Creates a ResourceRepresentation with the specified existence column.
   *
   * @param existenceColumn the column representing resource existence (typically the id column)
   * @return a new ResourceRepresentation
   */
  @Nonnull
  public static ResourceRepresentation of(@Nonnull final Column existenceColumn) {
    return new ResourceRepresentation(existenceColumn);
  }

  /**
   * Creates a ResourceRepresentation using the standard id column as the existence column.
   *
   * <p>This is the most common factory method for creating instances that work with standard
   * Pathling-encoded flat datasets where {@code col("id")} represents resource existence.
   *
   * @return a new ResourceRepresentation with {@code col("id")} as existence column
   */
  @Nonnull
  public static ResourceRepresentation withIdColumn() {
    return new ResourceRepresentation(functions.col(DEFAULT_EXISTENCE_COLUMN));
  }

  /**
   * Creates a ResourceRepresentation for contexts where each row represents exactly one resource.
   *
   * <p>This factory method uses {@code lit(true)} as the existence column, meaning all field
   * accesses are unconditional. This is appropriate for single-resource evaluation contexts where
   * every row in the dataset represents a valid resource, even if the resource doesn't have an
   * {@code id} element defined.
   *
   * @return a new ResourceRepresentation with {@code lit(true)} as existence column
   */
  @Nonnull
  public static ResourceRepresentation alwaysPresent() {
    return new ResourceRepresentation(functions.lit(true));
  }

  /**
   * Returns the existence column as the value for this representation.
   *
   * <p>In flat schema, there is no single column representing the whole resource structure.
   * However, the existence column (typically the id column) serves as a value that indicates
   * whether the resource exists (non-NULL) or not (NULL). This is used when operations like {@code
   * asSingular()} need to materialize a column value for the resource.
   *
   * @return the existence column
   */
  @Override
  @Nonnull
  public Column getValue() {
    return existenceColumn;
  }

  /**
   * Creates a copy of this representation with a new existence column value.
   *
   * @param newValue the new existence column value
   * @return a new ResourceRepresentation with the specified column
   */
  @Override
  @Nonnull
  public ColumnRepresentation copyOf(@Nonnull final Column newValue) {
    return new ResourceRepresentation(newValue);
  }

  /**
   * Applies the singular expression to the existence column and returns a new
   * ResourceRepresentation.
   *
   * <p>In flat schema, resources are inherently singular (one row = one resource), so the singular
   * expression is always applied (not the array expression). The result is a new
   * ResourceRepresentation with the transformed column, ensuring subsequent operations (like {@link
   * #traverse(String)}) continue to use ResourceRepresentation behavior.
   *
   * @param arrayExpression the expression to apply for array values (not used)
   * @param singularExpression the expression to apply for singular values
   * @return a new ResourceRepresentation with the transformed existence column
   */
  @Override
  @Nonnull
  public ColumnRepresentation vectorize(
      @Nonnull final UnaryOperator<Column> arrayExpression,
      @Nonnull final UnaryOperator<Column> singularExpression) {
    // Always singular - one row = one resource, apply the singular expression
    // Return a new ResourceRepresentation (via copyOf) so that subsequent operations
    // (like traverse) continue to use ResourceRepresentation behavior.
    return copyOf(singularExpression.apply(existenceColumn));
  }

  /**
   * Flattens this representation.
   *
   * <p>Since flat schema representation is already "flat" (fields accessed directly as columns),
   * this returns the representation unchanged. Operations that need a column value should traverse
   * to a specific field first.
   *
   * @return this ResourceRepresentation unchanged
   */
  @Override
  @Nonnull
  public ColumnRepresentation flatten() {
    // Already flat - return this so subsequent operations use ResourceRepresentation behavior
    return this;
  }

  /**
   * Traverses from the root to a top-level field in the flat schema.
   *
   * <p>Unlike nested schema traversal where we use {@code col("ResourceType").getField(fieldName)},
   * this method creates a direct column reference using {@code col(fieldName)}.
   *
   * <p>When the existence column has been modified (e.g., via filtering with {@code where()}), the
   * field access is conditional on the existence column being non-null. For the default case
   * (existence column is {@code col("id")}), the field is accessed directly without additional null
   * checks.
   *
   * <p>The returned representation is a {@link DefaultRepresentation}, so subsequent traversals
   * will use the standard {@code getField()} method for nested access.
   *
   * @param fieldName the name of the field to traverse to
   * @return a {@link DefaultRepresentation} wrapping the field access
   */
  @Override
  @Nonnull
  public ColumnRepresentation traverse(@Nonnull final String fieldName) {
    // at the root level, access the field directly via col(fieldName)
    // removeNulls() filters out NULL values from arrays to match the behavior of
    // DefaultRepresentation.traverse() - but we don't flatten here
    return getField(fieldName).removeNulls();
  }

  /**
   * Traverses from the root to a top-level field in the flat schema, with FHIR type awareness.
   *
   * <p>This method delegates to {@link #traverse(String)} for the actual traversal, then applies
   * type-specific handling for special FHIR types like base64Binary.
   *
   * @param fieldName the name of the field to traverse to
   * @param fhirType the FHIR type of the field
   * @return a {@link ColumnRepresentation} for the field, with appropriate type handling
   */
  @Override
  @Nonnull
  public ColumnRepresentation traverse(
      @Nonnull final String fieldName, @Nonnull final Optional<FHIRDefinedType> fhirType) {
    if (fhirType.filter(FHIRDefinedType.BASE64BINARY::equals).isPresent()) {
      // If the field is a base64Binary, represent it using binary column handling
      return DefaultRepresentation.fromBinaryColumn(traverse(fieldName).getValue());
    }
    return traverse(fieldName);
  }

  /**
   * Gets a top-level field from the flat schema without flattening.
   *
   * <p>Similar to {@link #traverse(String)} but does not apply removeNulls() or flatten(),
   * preserving the nested structure of the field.
   *
   * @param fieldName the name of the field to get
   * @return a {@link DefaultRepresentation} wrapping the field access
   */
  @Override
  @Nonnull
  public ColumnRepresentation getField(@Nonnull final String fieldName) {
    return new DefaultRepresentation(
        functions.when(existenceColumn.isNotNull(), functions.col(fieldName)));
  }
}
