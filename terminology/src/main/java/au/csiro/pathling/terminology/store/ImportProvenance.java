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
import lombok.Value;

/**
 * Everything an import records about where its content came from: the source as the caller supplied
 * it, the digest of the source bytes, and the identity and verification outcome of the package it
 * came from. Every manifest row written by one import carries the same values.
 *
 * @author John Grimes
 */
@Value
public class ImportProvenance implements Serializable {

  @Serial private static final long serialVersionUID = 1L;

  /** The source path or archive, as passed to the import. */
  @Nonnull String source;

  /** The SHA-256 of the source file bytes as lowercase hexadecimal, or null for a directory. */
  @Nullable String sourceSha256;

  /** The package name from the tarball's {@code package.json}, or null if not a named package. */
  @Nullable String packageName;

  /** The package version from the tarball's {@code package.json}, or null if not versioned. */
  @Nullable String packageVersion;

  /** The registry verification outcome, or null if the source was not a package. */
  @Nullable PackageVerification packageVerification;

  /** The registry that vouched for the bytes, set only when the package was verified. */
  @Nullable String packageRegistry;

  /**
   * Creates the provenance of a source that is not a FHIR NPM package, which carries no package
   * identity and no verification outcome.
   *
   * @param source the source path, as passed to the import
   * @param sourceSha256 the SHA-256 of the source file bytes, or null for a directory
   * @return the provenance of the source
   */
  @Nonnull
  public static ImportProvenance of(
      @Nonnull final String source, @Nullable final String sourceSha256) {
    return new ImportProvenance(source, sourceSha256, null, null, null, null);
  }
}
