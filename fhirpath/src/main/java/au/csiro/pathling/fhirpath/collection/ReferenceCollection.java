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

import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.ReferenceValue;
import au.csiro.pathling.fhirpath.definition.NodeDefinition;
import au.csiro.pathling.fhirpath.function.ColumnTransform;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import org.apache.spark.sql.Column;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * Represents a collection of Reference elements.
 *
 * @author John Grimes
 * @see <a href="https://hl7.org/fhir/R4/references.html#Reference">Resource References</a>
 */
public class ReferenceCollection extends Collection {

  private static final String REFERENCE_ELEMENT_NAME = "reference";
  private static final String TYPE_ELEMENT_NAME = "type";

  /**
   * Creates a new ReferenceCollection.
   *
   * @param column the column representation
   * @param type the FhirPath type
   * @param fhirType the FHIR type
   * @param definition the node definition
   * @param extensionMapColumn the extension map column
   */
  protected ReferenceCollection(
      @Nonnull final ColumnRepresentation column,
      @Nonnull final Optional<FhirPathType> type,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final Optional<? extends NodeDefinition> definition,
      @Nonnull final Optional<Column> extensionMapColumn) {
    super(column, type, fhirType, definition, extensionMapColumn);
  }

  /**
   * @param typeSpecifier The type specifier to filter by
   * @return a {@link Collection} containing the keys of the references in this collection, suitable
   *     for joining with resource keys
   */
  @Nonnull
  public Collection getKeyCollection(@Nonnull final Optional<TypeSpecifier> typeSpecifier) {
    return typeSpecifier
        // If a type was specified, create a regular expression that matches references of this
        // type.
        .map(ts -> ts.toFhirType().toCode() + "/.+")
        // Get a ColumnTransform that filters the reference column based on the regular expression.
        .map(this::keyFilter)
        // Apply the filter to the reference column.
        .map(this::filter)
        // Return a StringCollection of the reference elements.s
        .flatMap(c -> c.traverse(REFERENCE_ELEMENT_NAME))
        // If no type was specified, return the reference column as is.
        .or(() -> this.traverse(REFERENCE_ELEMENT_NAME))
        // If the reference column is not present, return an empty collection.
        .orElse(EmptyCollection.getInstance());
  }

  @Nonnull
  private ColumnTransform keyFilter(@Nonnull final String pattern) {
    return col ->
        col.traverse(REFERENCE_ELEMENT_NAME, Optional.of(FHIRDefinedType.STRING)).like(pattern);
  }

  /**
   * Performs a limited resolution of this Reference, extracting type information only.
   *
   * <p>This implementation:
   *
   * <ul>
   *   <li>Returns type information from {@code Reference.type} field (priority) or parsed reference
   *       string
   *   <li>Supports the {@code is} operator for type checking
   *   <li>Does NOT support traversal (throws error on field access)
   *   <li>Does NOT perform actual resource resolution/joining
   * </ul>
   *
   * <p>Type extraction:
   *
   * <ul>
   *   <li>If {@code Reference.type} field is present, it is used regardless of reference format
   *   <li>If {@code Reference.type} is absent, type is parsed from {@code Reference.reference}
   *   <li>Type field always takes precedence over parsed type from reference string
   * </ul>
   *
   * <p>Supported reference formats (when type field is absent):
   *
   * <ul>
   *   <li>Relative: {@code Patient/123}
   *   <li>Absolute: {@code http://example.org/fhir/Patient/123}
   *   <li>Canonical: {@code http://hl7.org/fhir/ValueSet/my-valueset}
   * </ul>
   *
   * <p>Returns empty when type cannot be determined:
   *
   * <ul>
   *   <li>Contained references without type field: {@code #local-id}
   *   <li>Logical references without type field (identifier-only)
   *   <li>Malformed reference strings
   * </ul>
   *
   * @return A {@link ResolvedReferenceCollection} containing type information, or {@link
   *     EmptyCollection} if type cannot be determined
   * @see <a href="https://build.fhir.org/fhirpath.html#functions">FHIRPath resolve() function</a>
   * @see <a href="https://hl7.org/fhir/R4/references.html">FHIR Resource References</a>
   */
  @Nonnull
  public Collection resolve() {
    final ColumnRepresentation referenceColumn = getColumn().getField(REFERENCE_ELEMENT_NAME);
    final ColumnRepresentation typeColumn = getColumn().getField(TYPE_ELEMENT_NAME);

    // Extract type information using ReferenceValue
    final ColumnRepresentation extractedType =
        ReferenceValue.of(referenceColumn, typeColumn).extractType();

    // Remove null values (unresolvable references) from the extracted type column
    final ColumnRepresentation filteredType = extractedType.removeNulls();

    // Return a ResolvedReferenceCollection with dynamic type information
    return ResolvedReferenceCollection.build(filteredType);
  }
}
