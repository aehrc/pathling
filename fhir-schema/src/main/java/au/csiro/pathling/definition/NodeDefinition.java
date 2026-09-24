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

package au.csiro.pathling.definition;

import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

/**
 * Base class for FHIR-like schema definitions. The schema is capable or representing a subset all
 * FHIR schema concepts such as elements, choices, and resources.
 */
public interface NodeDefinition {

  /**
   * Returns the child element of this definition with the specified name.
   *
   * <p>The default looks it up among {@link #getChildren()} by name, which suits an implementation
   * that already holds its children as a list; one backed by a definition set too large to hold in
   * a list overrides this with a direct lookup instead.
   *
   * @param name the name of the child element
   * @return a new {@link NodeDefinition} describing the child
   */
  @Nonnull
  default Optional<ChildDefinition> getChildElement(@Nonnull final String name) {
    return getChildren().stream().filter(child -> child.getName().equals(name)).findFirst();
  }

  /**
   * Returns the children of this definition, in the order the definitions declare them. A choice
   * element appears once, as a choice, rather than expanded into one child per type.
   *
   * @return the child definitions of this node
   */
  @Nonnull
  List<ChildDefinition> getChildren();

  /**
   * Returns a value that identifies the type this node describes, so that a traversal can recognise
   * a type it has already expanded.
   *
   * <p>It is not the FHIR type code, because every backbone element reports the same code while
   * describing entirely different children. Nor is it the node itself, because a node is built
   * afresh each time a child is resolved. The default is the node itself, which is correct but
   * never recognises anything; an implementation backed by a definition set overrides it with the
   * identity of the underlying definition.
   *
   * @return a value identifying the type this node describes
   */
  @Nonnull
  default Object getTypeIdentity() {
    return this;
  }

  /**
   * Returns whether this definition originates from a FHIR model (as opposed to a synthetic
   * definition created for literals or internal use).
   *
   * @return {@code true} if this definition represents a FHIR model element or resource
   */
  default boolean isFhirDefinition() {
    return false;
  }
}
