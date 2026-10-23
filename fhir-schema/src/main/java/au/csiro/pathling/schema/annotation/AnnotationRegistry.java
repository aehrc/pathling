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

package au.csiro.pathling.schema.annotation;

import au.csiro.pathling.schema.LayoutEntry;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Maps the annotation entries of the canonical structure to the kinds that compute them.
 *
 * <p>It takes no order of its own: which annotations an element carries, and in what order, is
 * fixed by {@link au.csiro.pathling.schema.LayoutFields} and reached through the canonical
 * structure's annotation entries. The registry only says which kind computes an entry. An entry no
 * kind computes is not an annotation Pathling stores.
 */
public final class AnnotationRegistry {

  @Nonnull private final Map<String, AnnotationKind> kindsByField;

  private AnnotationRegistry(@Nonnull final Map<String, AnnotationKind> kindsByField) {
    this.kindsByField = kindsByField;
  }

  /**
   * Returns the registry of the kinds Pathling defines, which is what every public entry point
   * uses. No kind is defined yet.
   *
   * @return the registry of the built-in kinds
   */
  @Nonnull
  public static AnnotationRegistry builtIn() {
    return of(List.of());
  }

  /**
   * Returns a registry of the given kinds.
   *
   * <p>This is how tests supply stub kinds. No public entry point takes a registry, so the set of
   * kinds that reaches data stays the built-in one.
   *
   * @param kinds the kinds, each carried in fields no other kind is carried in
   * @return the registry
   * @throws IllegalStateException if two kinds are carried in the same field
   */
  @Nonnull
  public static AnnotationRegistry of(@Nonnull final List<AnnotationKind> kinds) {
    return new AnnotationRegistry(
        kinds.stream()
            .flatMap(kind -> kind.getFields().stream().map(field -> Map.entry(field, kind)))
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue)));
  }

  /**
   * Returns the kind that computes an annotation entry of the canonical structure.
   *
   * @param entry the entry
   * @return the kind, or empty where the entry is not an annotation or no kind computes it
   */
  @Nonnull
  public Optional<AnnotationKind> kindFor(@Nonnull final LayoutEntry entry) {
    // Every field a kind is carried in is an annotation's, so no other entry is found.
    return Optional.ofNullable(kindsByField.get(entry.getName()));
  }
}
