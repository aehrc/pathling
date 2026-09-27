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

import au.csiro.pathling.fhirpath.FhirPathType;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import lombok.Getter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * The representation of a primitive element reached by traversal, which retains the representation
 * of its parent and its own name, so that a named sibling of the element within the parent can be
 * resolved (T094, R-014).
 *
 * <p>The layout stores a primitive's id and extensions in a metadata group beside it, named after
 * it with a leading underscore, and every annotation is likewise a sibling of the element it
 * annotates. Sibling resolution serves both. Neither is populated before M5, so nothing resolves a
 * sibling yet.
 *
 * <p>The parent is retained only by the traversal itself. Any operation on the element yields an
 * ordinary representation, so a sibling is resolved from the element as traversal reached it.
 * Equality is that of the value, as for any {@link DefaultRepresentation}: the parent is a means of
 * resolving siblings, not part of what is represented.
 *
 * @author Piotr Szul
 */
@Getter
@SuppressWarnings("java:S2160")
public class ElementRepresentation extends DefaultRepresentation {

  /** The representation of the structure, or structures, that the element was traversed from. */
  @Nonnull private final ColumnRepresentation parent;

  /** The name of the element within its parent. */
  @Nonnull private final String elementName;

  /**
   * Creates the representation of a primitive element reached by traversal.
   *
   * @param value the element, as traversal reached it
   * @param parent the representation the element was traversed from
   * @param elementName the name of the element within its parent
   */
  public ElementRepresentation(
      @Nonnull final Column value,
      @Nonnull final ColumnRepresentation parent,
      @Nonnull final String elementName) {
    super(value);
    this.parent = parent;
    this.elementName = elementName;
  }

  /**
   * Retains the parent of an element reached by traversal where the element is a primitive, and
   * otherwise returns the element as it is.
   *
   * @param element the element, as traversal reached it
   * @param fhirType the FHIR type of the element, if known
   * @param parent the representation the element was traversed from
   * @param elementName the name of the element within its parent
   * @return the element, retaining its parent if it is a primitive
   */
  @Nonnull
  public static ColumnRepresentation ofPrimitive(
      @Nonnull final ColumnRepresentation element,
      @Nonnull final Optional<FHIRDefinedType> fhirType,
      @Nonnull final ColumnRepresentation parent,
      @Nonnull final String elementName) {
    final boolean primitive =
        fhirType
            .flatMap(FhirPathType::forFhirType)
            .map(type -> !(type.getSqlDataType() instanceof StructType))
            .orElse(false);
    return primitive ? new ElementRepresentation(element.getValue(), parent, elementName) : element;
  }

  /**
   * Resolves a named sibling of this element within its parent, through the tolerant traversal, so
   * that a sibling the schema does not carry is a null. The sibling has the cardinality a traversal
   * from the parent gives it, as the element does.
   *
   * @param siblingName the name of the sibling, such as {@code _birthDate} for the metadata group
   *     of {@code birthDate}
   * @return the sibling
   */
  @Nonnull
  public ColumnRepresentation traverseSibling(@Nonnull final String siblingName) {
    return parent.traverse(siblingName);
  }
}
