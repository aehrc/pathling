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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Parameters;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link JobStatus} derivation from a job's {@link Future}.
 *
 * @author John Grimes
 */
class JobStatusTest {

  /** A running (not yet complete) future derives to in progress. */
  @Test
  void derivesInProgressWhenNotDone() {
    final CompletableFuture<IBaseResource> future = new CompletableFuture<>();

    assertThat(JobStatus.fromResult(future)).isEqualTo(JobStatus.IN_PROGRESS);
  }

  /** A future that has completed normally derives to completed. */
  @Test
  void derivesCompletedWhenDoneNormally() {
    final CompletableFuture<IBaseResource> future = new CompletableFuture<>();
    future.complete(new Parameters());

    assertThat(JobStatus.fromResult(future)).isEqualTo(JobStatus.COMPLETED);
  }

  /** A future whose background work threw derives to failed. */
  @Test
  void derivesFailedWhenExecutionThrew() {
    final CompletableFuture<IBaseResource> future = new CompletableFuture<>();
    future.completeExceptionally(new RuntimeException("boom"));

    assertThat(JobStatus.fromResult(future)).isEqualTo(JobStatus.FAILED);
  }

  /** A cancelled future derives to cancelled, checked ahead of the done branches. */
  @Test
  void derivesCancelledWhenCancelled() {
    final CompletableFuture<IBaseResource> future = new CompletableFuture<>();
    future.cancel(false);

    assertThat(JobStatus.fromResult(future)).isEqualTo(JobStatus.CANCELLED);
  }

  /** Derivation must never block on an incomplete future by calling {@code get()}. */
  @Test
  @SuppressWarnings("unchecked")
  void neverCallsGetOnIncompleteFuture() throws InterruptedException, ExecutionException {
    final Future<IBaseResource> future = mock(Future.class);
    when(future.isCancelled()).thenReturn(false);
    when(future.isDone()).thenReturn(false);

    assertThat(JobStatus.fromResult(future)).isEqualTo(JobStatus.IN_PROGRESS);
    verify(future, never()).get();
  }

  /** Each status exposes its wire code for the operation response. */
  @Test
  void exposesWireCodes() {
    assertThat(JobStatus.IN_PROGRESS.getCode()).isEqualTo("in-progress");
    assertThat(JobStatus.COMPLETED.getCode()).isEqualTo("completed");
    assertThat(JobStatus.FAILED.getCode()).isEqualTo("failed");
    assertThat(JobStatus.CANCELLED.getCode()).isEqualTo("cancelled");
  }
}
