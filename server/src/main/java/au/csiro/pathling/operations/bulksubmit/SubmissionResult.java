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
import java.util.List;

/**
 * Represents the result of a completed bulk submission.
 * <p>
 * The transactionTime is stored as an ISO-8601 string for JSON serialisation compatibility with the
 * shaded Jackson library.
 *
 * @param submissionId The unique identifier of the submission.
 * @param transactionTime The time when the submission was processed (ISO-8601 format).
 * @param outputFiles The list of successfully processed output files.
 * @param requiresAccessToken Whether an access token is required to retrieve result files.
 * @author John Grimes
 * @see <a href="https://hackmd.io/@argonaut/rJoqHZrPle">Argonaut $bulk-submit Specification</a>
 */
public record SubmissionResult(
    @Nonnull String submissionId,
    @Nonnull String transactionTime,
    @Nonnull List<OutputFile> outputFiles,
    boolean requiresAccessToken
) {

}
