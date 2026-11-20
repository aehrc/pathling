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

import jakarta.annotation.Nonnull;

/**
 * The name of an XML attribute: its prefix, its local name and its namespace, each the empty string
 * where it has none.
 *
 * @param prefix the prefix
 * @param localName the local name
 * @param namespace the namespace
 */
record QualifiedAttribute(
    @Nonnull String prefix, @Nonnull String localName, @Nonnull String namespace) {

  /**
   * Returns the name as it is written in a start tag.
   *
   * @return the qualified name
   */
  @Nonnull
  String qualifiedName() {
    return prefix.isEmpty() ? localName : prefix + ":" + localName;
  }
}
