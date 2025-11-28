/*
 * Copyright 2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.operations.bulksubmit;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Map;

/**
 * Represents an error file in a bulk submission result containing OperationOutcome resources.
 *
 * @param type Always "OperationOutcome" for error files.
 * @param url The URL where the error file can be retrieved.
 * @param countBySeverity A map of severity codes to counts of issues with that severity.
 * @author John Grimes
 * @see <a href="https://hackmd.io/@argonaut/rJoqHZrPle">Argonaut $bulk-submit Specification</a>
 */
public record ErrorFile(
    @Nonnull String type,
    @Nonnull String url,
    @Nullable Map<String, Long> countBySeverity
) {

  /**
   * Creates an error file with the standard "OperationOutcome" type.
   *
   * @param url The URL where the error file can be retrieved.
   * @param countBySeverity A map of severity codes to counts.
   * @return A new ErrorFile instance.
   */
  @Nonnull
  public static ErrorFile create(
      @Nonnull final String url,
      @Nullable final Map<String, Long> countBySeverity
  ) {
    return new ErrorFile("OperationOutcome", url, countBySeverity);
  }

}
