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

package au.csiro.pathling.fhirpath.collection;

import au.csiro.pathling.errors.UnsupportedFhirPathFeatureError;
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.definition.ChildDefinition;
import au.csiro.pathling.fhirpath.definition.NodeDefinition;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents a collection of resolved references that expose type information only.
 *
 * <p>This collection is returned by the limited {@code resolve()} function and supports type
 * checking via the {@code is} operator, but does not allow traversal to child elements. Any attempt
 * to traverse will result in an {@link UnsupportedFhirPathFeatureError}.
 *
 * @author Piotr Szul
 * @see <a href="https://hl7.org/fhir/R4/references.html">FHIR Resource References</a>
 * @see <a href="https://build.fhir.org/fhirpath.html#functions">FHIRPath resolve() function</a>
 */
public class ResolvedReferenceCollection extends Collection {

  private static final String TRAVERSAL_ERROR_MESSAGE =
      "Traversal after resolve() is not supported. The resolve() function returns type "
          + "information only. Field access like 'resolve().fieldName' is not allowed.";

  /**
   * Creates a new ResolvedReferenceCollection.
   *
   * @param column the column representation
   * @param type the FhirPath type
   * @param fhirType the FHIR type
   * @param definition the node definition
   * @param extensionMapColumn the extension map column
   */
  protected ResolvedReferenceCollection(
      @Nonnull final ColumnRepresentation column,
      @Nonnull final Optional<FhirPathType> type,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final Optional<? extends NodeDefinition> definition,
      @Nonnull final Optional<Column> extensionMapColumn) {
    super(column, type, fhirType, definition, extensionMapColumn);
  }

  /**
   * Builds a new ResolvedReferenceCollection from a column representation and FHIR type.
   *
   * <p>The column should contain a value (typically a literal) that indicates the presence of a
   * resolved reference. Null values in the column indicate that the reference could not be
   * resolved.
   *
   * @param column the column representation
   * @param fhirType the FHIR resource type of the resolved reference
   * @return a new ResolvedReferenceCollection
   */
  @Nonnull
  public static ResolvedReferenceCollection build(
      @Nonnull final ColumnRepresentation column, @Nonnull final FHIRDefinedType fhirType) {
    final Optional<FhirPathType> fhirPathType = FhirPathType.forFhirType(fhirType);
    return new ResolvedReferenceCollection(
        column, fhirPathType, Optional.of(fhirType), Optional.empty(), Optional.empty());
  }

  /**
   * Builds a new ResolvedReferenceCollection from a column representation where the type is encoded
   * in the column itself.
   *
   * <p>The column should contain the string representation of the FHIR type (e.g., "Patient",
   * "Organization"), or null if the reference could not be resolved.
   *
   * @param column the column representation containing type strings
   * @return a new ResolvedReferenceCollection with dynamic type information
   */
  @Nonnull
  public static ResolvedReferenceCollection build(@Nonnull final ColumnRepresentation column) {
    // For dynamic typing, we don't know the type statically, so we leave it empty.
    // The actual type checking will be done at runtime using the column values.
    return new ResolvedReferenceCollection(
        column, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
  }

  @Nonnull
  @Override
  public Collection copyWith(@Nonnull final ColumnRepresentation newValue) {
    return new ResolvedReferenceCollection(
        newValue, getType(), getFhirType(), getDefinition(), getExtensionMapColumn());
  }

  /**
   * Filters this collection to only include elements of the specified type.
   *
   * <p>This method supports type checking with the {@code is} operator. If the resolved reference's
   * type matches the specified type, this collection is returned; otherwise, an empty collection is
   * returned.
   *
   * @param type the type specifier to filter by
   * @return this collection if the type matches, otherwise an empty collection
   */
  @Nonnull
  @Override
  public Collection filterByType(@Nonnull final TypeSpecifier type) {
    // If we have a static FHIR type, use the standard filtering logic
    if (getFhirType().isPresent()) {
      return super.filterByType(type);
    }

    // For dynamic typing (when type is in the column), we need runtime type checking
    // This is handled by comparing the column value (type string) with the requested type
    if (TypeSpecifier.FHIR_NAMESPACE.equals(type.getNamespace())) {
      final String requestedType = type.getTypeName();
      // Filter rows where the column value equals the requested type
      return copyWith(getColumn().filter(col -> col.equalTo(requestedType)));
    }

    // System types are not supported for resolved references
    return EmptyCollection.getInstance();
  }

  /**
   * Traversal is not supported for resolved references.
   *
   * <p>The {@code resolve()} function returns type information only, not actual resource data.
   * Therefore, attempting to access fields like {@code resolve().name} will throw an error.
   *
   * @param elementName the name of the child element (ignored)
   * @return never returns normally
   * @throws UnsupportedFhirPathFeatureError always thrown to prevent traversal
   */
  @Nonnull
  @Override
  public Optional<Collection> traverse(@Nonnull final String elementName) {
    throw new UnsupportedFhirPathFeatureError(TRAVERSAL_ERROR_MESSAGE);
  }

  /**
   * Traversal is not supported for resolved references.
   *
   * @param childDefinition the child definition (ignored)
   * @return never returns normally
   * @throws UnsupportedFhirPathFeatureError always thrown to prevent traversal
   */
  @Nonnull
  @Override
  public Collection traverseChild(@Nonnull final ChildDefinition childDefinition) {
    throw new UnsupportedFhirPathFeatureError(TRAVERSAL_ERROR_MESSAGE);
  }
}
