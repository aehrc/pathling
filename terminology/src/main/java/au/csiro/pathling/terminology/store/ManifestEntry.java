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

  @Serial private static final long serialVersionUID = 2L;

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

  /** The SHA-256 of the source file bytes as lowercase hexadecimal, or null if not recorded. */
  @Nullable String sourceSha256;

  /** The name of the package the entry came from, or null if it did not come from one. */
  @Nullable String packageName;

  /** The version of the package the entry came from, or null if it did not come from one. */
  @Nullable String packageVersion;

  /** The registry verification outcome of the package, or null if the source was not a package. */
  @Nullable PackageVerification packageVerification;

  /** The registry that vouched for the package bytes, set only when the package was verified. */
  @Nullable String packageRegistry;

  /**
   * Creates an entry describing a resource written by an import, taking the provenance values from
   * the import that wrote it.
   *
   * @param entryType the kind of entry: {@code code_system}, {@code value_set} or {@code
   *     concept_map}
   * @param canonicalUrl the canonical URL of the entry
   * @param version the version of the entry, or null if unversioned
   * @param provenance the provenance of the import that wrote the entry
   * @param importedAt when the entry was imported
   * @return the manifest entry
   */
  @Nonnull
  public static ManifestEntry forImport(
      @Nonnull final String entryType,
      @Nonnull final String canonicalUrl,
      @Nullable final String version,
      @Nonnull final ImportProvenance provenance,
      @Nonnull final Instant importedAt) {
    return new ManifestEntry(
        TerminologyStoreSchema.STORE_FORMAT_VERSION,
        entryType,
        canonicalUrl,
        version,
        provenance.getSource(),
        importedAt,
        provenance.getSourceSha256(),
        provenance.getPackageName(),
        provenance.getPackageVersion(),
        provenance.getPackageVerification(),
        provenance.getPackageRegistry());
  }
}
