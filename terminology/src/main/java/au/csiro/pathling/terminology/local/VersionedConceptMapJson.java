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

package au.csiro.pathling.terminology.local;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

/**
 * The resource JSON of an imported concept map, with the version it was imported under. Its string
 * form is the version alone, so that version resolution messages that list the candidates never
 * carry the content of a map.
 *
 * @author John Grimes
 */
final class VersionedConceptMapJson {

  @Nullable private final String version;

  @Nonnull private final String json;

  VersionedConceptMapJson(@Nullable final String version, @Nonnull final String json) {
    this.version = version;
    this.json = json;
  }

  @Nullable
  String getVersion() {
    return version;
  }

  @Nonnull
  String getJson() {
    return json;
  }

  @Override
  @Nonnull
  public String toString() {
    return String.valueOf(version);
  }
}
