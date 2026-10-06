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

/**
 * The outcome of checking an imported FHIR NPM package against the checksum its registry publishes.
 * A source that is not a package records no status at all.
 *
 * @author John Grimes
 */
public enum PackageVerification {

  /** The tarball's SHA-1 equals the {@code dist.shasum} the registry publishes for the version. */
  VERIFIED("verified"),

  /**
   * The source is a package, but no registry checksum could be compared: the package could not be
   * identified, the registry could not be consulted or did not list the package or version, or the
   * version carries no checksum.
   */
  UNVERIFIED("unverified"),

  /** The source is a package, but the caller disabled verification. */
  SKIPPED("skipped");

  @Nonnull private final String code;

  PackageVerification(@Nonnull final String code) {
    this.code = code;
  }

  /**
   * Returns the code this status is recorded as in the store manifest.
   *
   * @return the manifest code
   */
  @Nonnull
  public String getCode() {
    return code;
  }

  /**
   * Resolves a status from the code recorded in the store manifest.
   *
   * @param code the manifest code
   * @return the corresponding status
   * @throws IllegalArgumentException if the code is not a known status
   */
  @Nonnull
  public static PackageVerification fromCode(@Nonnull final String code) {
    for (final PackageVerification value : values()) {
      if (value.code.equals(code)) {
        return value;
      }
    }
    throw new IllegalArgumentException("Unknown package verification status: " + code);
  }
}
