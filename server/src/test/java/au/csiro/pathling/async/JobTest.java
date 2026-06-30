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

package au.csiro.pathling.async;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Parameters;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link Job}, covering the creation-time start timestamp used by the SQL on FHIR
 * export manifest's timing fields.
 *
 * @author John Grimes
 */
class JobTest {

  @Test
  void recordsNonNullStartTimeAtCreation() {
    final Instant before = Instant.now();
    final Future<IBaseResource> result = CompletableFuture.completedFuture(new Parameters());
    final Job<Object> job = new Job<>("job-1", "viewdefinition-export", result, Optional.empty());
    final Instant after = Instant.now();

    // The job records a creation timestamp that falls within the window in which it was created.
    assertThat(job.getStartTime()).isNotNull();
    assertThat(job.getStartTime()).isBetween(before, after);
  }
}
