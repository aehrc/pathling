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

package au.csiro.pathling.io.transform;

import au.csiro.pathling.definition.FhirType;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataType;

/**
 * The storage of extensions, which this layout carries inline (FR-003).
 *
 * <p>Inline means as the element the definitions describe, on the structure carrying it and
 * recursing as far as the source does. The previous layout could not do that: it hoisted every
 * extension into a map at the root of the resource, keyed by an identifier it had to add to every
 * composite, because a scalar column had nowhere to carry one. Neither the map nor the identifier
 * belongs to this layout, and neither is emitted.
 *
 * <p>An extension on a primitive element is the other half of FR-003 and does not arrive here: it
 * belongs in the metadata group beside the element, which the derivation provides and the transform
 * does not yet populate.
 */
public final class ExtensionTransform {

  private ExtensionTransform() {}

  /**
   * Returns whether an element of the given type carries extension content, and is therefore stored
   * inline.
   *
   * @param type the FHIR type of the element
   * @return true where it does
   */
  public static boolean isInlineExtension(@Nonnull final FhirType type) {
    return FhirType.EXTENSION.equals(type);
  }

  /**
   * Returns the stored value of an extension-bearing element, which is the ordinary structure the
   * definitions describe, in the place they describe it.
   *
   * @param structures the mapping that carries an ordinary structure across
   * @param source the column the source was read into
   * @param target the type the element is stored as
   * @param observed the type the source was read as
   * @return the stored value
   */
  @Nonnull
  public static Column inline(
      @Nonnull final StructureMapping structures,
      @Nonnull final Column source,
      @Nonnull final DataType target,
      @Nonnull final DataType observed) {
    return structures.map(source, target, observed);
  }
}
