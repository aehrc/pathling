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

package au.csiro.pathling.config;

import au.csiro.pathling.operations.bulksubmit.SubmitterIdentifier;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;

/**
 * Configuration for the $bulk-submit operation.
 *
 * @author John Grimes
 * @see <a href="https://hackmd.io/@argonaut/rJoqHZrPle">Argonaut $bulk-submit Specification</a>
 */
@Data
public class BulkSubmitConfiguration {

  /**
   * The list of allowed submitters that can use the $bulk-submit operation.
   */
  @Nonnull
  private List<SubmitterIdentifier> allowedSubmitters = new ArrayList<>();

  /**
   * URL prefixes that are allowed as sources for manifest and file URLs.
   */
  @Nonnull
  private List<String> allowableSources = new ArrayList<>();

  /**
   * The directory to use for staging downloaded files before import.
   */
  @Nonnull
  private String stagingDirectory = "/usr/local/staging/bulk-submit-fetch";

  /**
   * Checks if a submitter is allowed to use the $bulk-submit operation.
   *
   * @param submitter The submitter to check.
   * @return true if the submitter is allowed, false otherwise.
   */
  public boolean isSubmitterAllowed(@Nonnull final SubmitterIdentifier submitter) {
    return allowedSubmitters.stream()
        .anyMatch(allowed -> allowed.system().equals(submitter.system())
            && allowed.value().equals(submitter.value()));
  }

}
