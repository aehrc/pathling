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

import au.csiro.pathling.encoders.ColumnFunctions;
import au.csiro.pathling.fhirpath.collection.QuantityCollection;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import java.util.function.UnaryOperator;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
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
 *   <li>{@link #traverse(String)} returns a {@link DefaultRepresentation} with a tolerant reference
 *       to the table column - subsequent traversals use the tolerant traversal expression
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
   * Traverses from the root to a top-level field in the flat schema, yielding a null of the given
   * type where the input does not have the column.
   *
   * <p>Unlike nested schema traversal where we use {@code col("ResourceType").getField(fieldName)},
   * this method creates a direct reference to the table column. The reference is tolerant (decision
   * 75). When the existence column has been modified (e.g., via filtering with {@code where()}),
   * the field access is conditional on the existence column being non-null.
   *
   * <p>removeNulls() filters out NULL values from arrays to match the behavior of {@link
   * DefaultRepresentation#traverse(String)}, but the result is not flattened.
   *
   * @param fieldName the name of the field to traverse to
   * @param fhirType the FHIR type of the field
   * @param fallback the type of the null that stands for the column where it is absent
   * @return a {@link ColumnRepresentation} for the field, with appropriate type handling
   */
  @Override
  @Nonnull
  public ColumnRepresentation traverse(
      @Nonnull final String fieldName,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final DataType fallback) {
    return DefaultRepresentation.decodeField(
        readField(fieldName, fhirType, fallback).removeNulls(), fhirType, this, fieldName);
  }

  /**
   * Reads a top-level field in the shape the new layout gives it, on either layout, normalising the
   * previous layout's shape where the field's type is stored differently there.
   */
  @Nonnull
  private ColumnRepresentation readField(
      @Nonnull final String fieldName,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final DataType fallback) {
    if (fhirType.filter(FHIRDefinedType.DECIMAL::equals).isPresent()) {
      // A decimal is read as text, with the previous layout's value and scale columns normalised
      // to it.
      return existing(ColumnFunctions.decimalColumnOrNull(fieldName, fallback));
    }
    if (fhirType.filter(QuantityCollection.QUANTITY_TYPES::contains).isPresent()) {
      // A quantity, of any of the quantity types, is read with the previous layout's canonical
      // form and value scale normalised away.
      return existing(ColumnFunctions.quantityColumnOrNull(fieldName, fallback));
    }
    if (fhirType.filter(FHIRDefinedType.INSTANT::equals).isPresent()) {
      // An instant is read as text, with the previous layout's timestamp normalised to its text in
      // UTC (decision 81).
      return existing(ColumnFunctions.instantColumnOrNull(fieldName, fallback));
    }
    return getField(fieldName, fallback);
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
    return getField(fieldName, DataTypes.NullType);
  }

  @Nonnull
  private ColumnRepresentation getField(
      @Nonnull final String fieldName, @Nonnull final DataType fallback) {
    return existing(ColumnFunctions.columnOrNull(fieldName, fallback));
  }

  /** Makes a reference to a table column conditional on the resource existing. */
  @Nonnull
  private ColumnRepresentation existing(@Nonnull final Column column) {
    return new DefaultRepresentation(functions.when(existenceColumn.isNotNull(), column));
  }

  /**
   * Traverses to the extensions of the resource itself, on either layout (decision 75). The
   * resource's {@code extension}, {@code _fid} and {@code _extension} columns are all reached
   * through the tolerant table-column reference.
   *
   * @return a {@link DefaultRepresentation} holding the extensions of the resource
   */
  @Override
  @Nonnull
  public ColumnRepresentation traverseExtension() {
    return new DefaultRepresentation(
            functions.when(existenceColumn.isNotNull(), ColumnFunctions.traverseRootExtension()))
        .removeNulls();
  }
}
