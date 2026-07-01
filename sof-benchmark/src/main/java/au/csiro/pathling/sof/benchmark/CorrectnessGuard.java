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

package au.csiro.pathling.sof.benchmark;

import jakarta.annotation.Nonnull;
import java.util.Optional;

/**
 * Computes a case's correctness status from the checkfile assertion and the observed output row
 * count.
 *
 * <p>When the assertion is present and equal the status is {@code ok}; present and unequal is
 * {@code count_mismatch}; absent is {@code ok}. When the case permits count variance (its reference
 * is resolved in {@code where}/{@code forEach} position where empty-vs-null-vs-error is
 * engine-specific), a divergence is not flagged and the status is {@code ok}.
 */
public final class CorrectnessGuard {

  /** The correctness status of a case whose output row count matches or has no expectation. */
  @Nonnull public static final String OK = "ok";

  /** The correctness status of a case whose output row count differs from a present assertion. */
  @Nonnull public static final String COUNT_MISMATCH = "count_mismatch";

  private CorrectnessGuard() {}

  /**
   * Determines the correctness status for a case.
   *
   * @param expectedCount the checkfile assertion for this case and size, if any
   * @param outputRows the observed output row count
   * @param countVariancePermitted whether a count divergence must not be flagged
   * @return {@link #OK} or {@link #COUNT_MISMATCH}
   */
  @Nonnull
  public static String status(
      @Nonnull final Optional<Integer> expectedCount,
      final long outputRows,
      final boolean countVariancePermitted) {
    if (countVariancePermitted) {
      return OK;
    }
    return expectedCount
        .map(expected -> expected.longValue() == outputRows ? OK : COUNT_MISMATCH)
        .orElse(OK);
  }
}
