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
import lombok.Value;

/**
 * The outcome of consulting a registry about a package, short of a mismatch: a mismatch is not a
 * result but a thrown {@link TerminologyImportException}.
 *
 * @author John Grimes
 */
@Value
public class PackageVerificationResult {

  /** The verification outcome. */
  @Nonnull PackageVerification status;

  /** The registry that vouched for the bytes, set only when the status is verified. */
  @Nullable String registry;

  /** Why the package could not be verified, for the warning; null when it was verified. */
  @Nullable String reason;

  /**
   * Creates the result of a successful comparison against a registry's checksum.
   *
   * @param registry the normalised registry URL that published the checksum
   * @return the verified result
   */
  @Nonnull
  public static PackageVerificationResult verified(@Nonnull final String registry) {
    return new PackageVerificationResult(PackageVerification.VERIFIED, registry, null);
  }

  /**
   * Creates the result of a lookup that could not establish a checksum to compare.
   *
   * @param reason why no comparison was possible
   * @return the unverified result
   */
  @Nonnull
  public static PackageVerificationResult unverified(@Nonnull final String reason) {
    return new PackageVerificationResult(PackageVerification.UNVERIFIED, null, reason);
  }
}
