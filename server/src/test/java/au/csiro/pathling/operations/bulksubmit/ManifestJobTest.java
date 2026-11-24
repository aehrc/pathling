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

package au.csiro.pathling.operations.bulksubmit;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for the type mismatch file name carried by {@link ManifestJob}.
 *
 * @author John Grimes
 */
class ManifestJobTest {

  private static final String MISMATCH_FILE = "OperationOutcome.job-1-type-mismatch.ndjson";

  private static ManifestJob pending() {
    return ManifestJob.createPending("job-1", "https://example.org/manifest.json", null, null);
  }

  @Test
  void pendingJobHasNoTypeMismatchFile() {
    assertThat(pending().typeMismatchFileName()).isNull();
  }

  @Test
  void withTypeMismatchFileNameSetsFileWithoutChangingState() {
    // Recording a type mismatch does not fail the job, because the matching resources are still
    // imported.
    final ManifestJob job =
        pending().withState(ManifestJobState.PROCESSING).withTypeMismatchFileName(MISMATCH_FILE);

    assertThat(job.typeMismatchFileName()).isEqualTo(MISMATCH_FILE);
    assertThat(job.state()).isEqualTo(ManifestJobState.PROCESSING);
    assertThat(job.errorFileName()).isNull();
  }

  @Test
  void typeMismatchFileIsPreservedByOtherCopyMethods() {
    final ManifestJob job = pending().withTypeMismatchFileName(MISMATCH_FILE);

    assertThat(job.withState(ManifestJobState.COMPLETED).typeMismatchFileName())
        .isEqualTo(MISMATCH_FILE);
    assertThat(job.withJobId("async-job").typeMismatchFileName()).isEqualTo(MISMATCH_FILE);
    assertThat(job.withDownloadedFiles(List.of()).typeMismatchFileName()).isEqualTo(MISMATCH_FILE);
    assertThat(job.withError("Import failed", "error.ndjson").typeMismatchFileName())
        .isEqualTo(MISMATCH_FILE);
  }
}
