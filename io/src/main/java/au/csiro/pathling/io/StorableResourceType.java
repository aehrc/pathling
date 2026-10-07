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

package au.csiro.pathling.io;

import au.csiro.pathling.definition.DefinitionContext;
import jakarta.annotation.Nonnull;

/**
 * Decides whether a resource type may be stored in this layout, which every route into it asks
 * before anything is read (FR-007, decision 84).
 *
 * <p>A bundle is a transport carrier and never a resource type of its own, so {@code Bundle} is
 * refused even though the definitions describe it. A name the definitions do not describe as a
 * resource is refused too. The check is made where a read begins, so that it fails before Spark
 * runs a job to infer a schema, and again by the transform, so that it holds whatever route reaches
 * the layout.
 */
final class StorableResourceType {

  /** The resource type a bundle is, and which is never stored. */
  @Nonnull static final String BUNDLE = "Bundle";

  private StorableResourceType() {}

  /**
   * Refuses a resource type that may not be stored.
   *
   * @param definitions the definitions that say which resource types exist
   * @param resourceType the resource type to check
   * @throws IllegalArgumentException if the type is {@code Bundle} or is not a resource type
   */
  static void require(
      @Nonnull final DefinitionContext definitions, @Nonnull final String resourceType) {
    if (BUNDLE.equals(resourceType)) {
      throw new IllegalArgumentException("A bundle is never stored as a resource type");
    }
    try {
      definitions.findResourceDefinition(resourceType);
    } catch (final RuntimeException e) {
      // The definitions report an unknown name in their own way, which for HAPI's is not an
      // IllegalArgumentException, so every route reports it the same way here.
      throw new IllegalArgumentException("Not a resource type: " + resourceType, e);
    }
  }
}
