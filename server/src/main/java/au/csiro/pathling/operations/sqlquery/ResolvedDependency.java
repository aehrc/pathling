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

package au.csiro.pathling.operations.sqlquery;

import jakarta.annotation.Nonnull;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A resolved node in a SQL on FHIR dependency graph: a {@link ResolvedViewDefinition} leaf, a
 * {@link ResolvedExternalTable} leaf or a {@link ResolvedSqlView}. Each node is identified by a
 * stable canonical key that is the basis of its request-scoped temp-view name and of diamond
 * deduplication.
 *
 * @author John Grimes
 */
public interface ResolvedDependency {

  /**
   * Encodes one free-text component of a cache-key description unambiguously: the decimal character
   * length of the value, a ':' separator, then the raw value. A reader that knows the grammar
   * always knows exactly how many characters belong to the component, so no value - however crafted
   * - can forge a structural delimiter and shift the parse of the key.
   *
   * <p>The encoding is unambiguous only because the character that follows each encoded value is
   * fixed by its position in the grammar (':' after a name or value, '=' after a map key, ','
   * between entries): the length prefix tells the reader where the value ends, and the fixed
   * character that must follow confirms where the next component begins. A component that is merely
   * concatenated without its length prefix keeps no such guarantee, so every client-controlled
   * value in the key must pass through this method.
   *
   * @param value the component value
   * @return the encoded component
   */
  @Nonnull
  static String encode(@Nonnull final String value) {
    return value.length() + ":" + value;
  }

  /**
   * Renders a map's entries unambiguously: sorted by key, with each key and value individually
   * {@link #encode(String) encoded} and joined as {@code <key>=<value>} pairs separated by ','.
   * Sorting keeps the rendering stable across map iteration orders, so identical kick-offs
   * deduplicate onto the same job regardless of how their maps were built.
   *
   * @param entries the map entries to render
   * @return the encoded map rendering
   */
  @Nonnull
  static String encodeEntries(@Nonnull final Map<String, ?> entries) {
    return entries.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .map(entry -> encode(entry.getKey()) + "=" + encode(String.valueOf(entry.getValue())))
        .collect(Collectors.joining(","));
  }

  /**
   * Returns the stable canonical identity of the resolved resource. Two references to the same
   * resource share a key, so a node is materialised only once per request, and the key cannot
   * collide with a different resource even when both are reached under the same table label.
   *
   * @return the canonical key
   */
  @Nonnull
  String getCanonicalKey();

  /**
   * Returns a deterministic description of everything about this node that decides the rows it
   * produces. Two nodes sharing a canonical key but differing in content - which happens whenever a
   * client inlines a different body at a canonical URL it has used before - must describe
   * themselves differently, so that a caller keying on the resolved graph can tell them apart.
   *
   * @return the content description
   */
  @Nonnull
  String describeContent();
}
