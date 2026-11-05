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
package au.csiro.pathling.utilities;

import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

/**
 * A lazily navigable description of canonical structure, supplying the information that {@link
 * StructureMerge} cannot derive from the structures it merges: the order in which fields belong.
 *
 * <p>Two subsequences of a total order do not determine that order, since {@code [id, family]} and
 * {@code [id, given]} do not say which of {@code family} and {@code given} comes first. The
 * canonical order is therefore an input to the merge rather than something it infers.
 *
 * <p>It answers two questions at any node: the canonical field order here, and the structure under
 * a given field name. Navigation is one named field at a time and must not expand children eagerly,
 * because the graph a canonical structure describes may be cyclic. FHIR's definition graph is:
 * extensions are self-recursive, and a reference carries an identifier that carries a reference.
 * The expanded tree is therefore infinite, and the depth that is actually needed is set by the
 * structures being merged rather than being known statically.
 */
public interface CanonicalStructure {

  /**
   * Returns the canonical order of the fields at this node.
   *
   * <p>This is the full order at this node, not restricted to the fields any particular structure
   * carries. It names fields only, and must not force the expansion of any child.
   *
   * @return the canonical field order at this node
   */
  @Nonnull
  List<String> fieldOrder();

  /**
   * Returns the structure beneath the named field, expanded only when this method is called.
   *
   * <p>Where the field is an array of structures, this describes the element structure rather than
   * the array, so that a caller need not know the cardinality of the field to ask about its
   * contents.
   *
   * <p>An empty result means that no structure is known here, which covers both a field that has no
   * structure, such as a primitive, and a name this node does not recognise. A caller that needs an
   * order beneath such a field must supply one of its own.
   *
   * @param name the name of the field to descend into
   * @return the structure beneath the named field, or empty if none is known
   */
  @Nonnull
  Optional<CanonicalStructure> field(@Nonnull String name);
}
