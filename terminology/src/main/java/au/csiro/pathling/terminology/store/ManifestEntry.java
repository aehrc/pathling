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

package au.csiro.pathling.terminology.store;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.Serial;
import java.io.Serializable;
import java.time.Instant;
import lombok.Value;

/**
 * One entry of the store manifest, describing an imported code system, value set or concept map and
 * carrying the store format version.
 *
 * @author John Grimes
 */
@Value
public class ManifestEntry implements Serializable {

  @Serial private static final long serialVersionUID = 1L;

  /** The store format version at the time this entry was written. */
  int storeFormatVersion;

  /** The kind of entry: {@code code_system}, {@code value_set} or {@code concept_map}. */
  @Nonnull String entryType;

  /** The canonical URL (code system URI or resource canonical URL). */
  @Nonnull String canonicalUrl;

  /** The version of the entry, or null if unversioned. */
  @Nullable String version;

  /** The provenance of the entry (original file or package name), or null if unknown. */
  @Nullable String source;

  /** When the entry was imported, or null if not recorded. */
  @Nullable Instant importedAt;
}
