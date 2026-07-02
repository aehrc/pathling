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
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Optional;

/**
 * The measured outcome of a single benchmark case: its correctness status, input and output row
 * counts, the timed execute+extract samples (one per measured iteration), and the separately
 * recorded load duration of its subject.
 *
 * <p>A case whose load, preparation, or evaluation raised is represented by {@link #failed}: its
 * status is {@link #EXECUTION_ERROR}, it carries an advisory {@code message}, and it has no timing
 * samples. Per-case failure isolation records such a case and continues the run rather than
 * aborting it.
 */
public class CaseResult {

  /**
   * The status of a case whose load, preparation, or evaluation raised. This is the
   * always-conformant default for any such failure; the finer {@code timeout} and {@code malformed}
   * refinements are not produced.
   */
  @Nonnull public static final String EXECUTION_ERROR = "execution_error";

  @Nonnull private final String id;

  @Nonnull private final String status;

  @Nullable private final String message;

  private final long inputRows;

  private final long outputRows;

  @Nonnull private final List<Double> executeExtractSamplesMs;

  private final double loadMs;

  /**
   * Constructs a measured case result with no advisory message.
   *
   * @param id the stable case id (the report's per-case key)
   * @param status {@code ok} or {@code count_mismatch}
   * @param inputRows the input row count of the subject
   * @param outputRows the output row count of the view
   * @param executeExtractSamplesMs the timed execute+extract samples in milliseconds
   * @param loadMs the load duration of the subject in milliseconds
   */
  public CaseResult(
      @Nonnull final String id,
      @Nonnull final String status,
      final long inputRows,
      final long outputRows,
      @Nonnull final List<Double> executeExtractSamplesMs,
      final double loadMs) {
    this(id, status, null, inputRows, outputRows, executeExtractSamplesMs, loadMs);
  }

  private CaseResult(
      @Nonnull final String id,
      @Nonnull final String status,
      @Nullable final String message,
      final long inputRows,
      final long outputRows,
      @Nonnull final List<Double> executeExtractSamplesMs,
      final double loadMs) {
    this.id = id;
    this.status = status;
    this.message = message;
    this.inputRows = inputRows;
    this.outputRows = outputRows;
    this.executeExtractSamplesMs = executeExtractSamplesMs;
    this.loadMs = loadMs;
  }

  /**
   * Constructs a failed case result: a case whose load, preparation, or evaluation raised. It has
   * {@link #EXECUTION_ERROR} status, an advisory message, and no timing samples, so the run can
   * record it and continue.
   *
   * @param id the stable case id
   * @param message an advisory human-readable explanation of the failure
   * @return the failed case result
   */
  @Nonnull
  public static CaseResult failed(@Nonnull final String id, @Nonnull final String message) {
    return new CaseResult(id, EXECUTION_ERROR, message, 0L, 0L, List.of(), 0.0);
  }

  /**
   * Returns the stable case id.
   *
   * @return the case id
   */
  @Nonnull
  public String getId() {
    return id;
  }

  /**
   * Returns the case status: {@code ok} or {@code count_mismatch} for a measured case, or {@link
   * #EXECUTION_ERROR} for a failed case.
   *
   * @return the status
   */
  @Nonnull
  public String getStatus() {
    return status;
  }

  /**
   * Returns the advisory failure message, present only for a failed (non-{@code ok}) case.
   *
   * @return the advisory message, if any
   */
  @Nonnull
  public Optional<String> getMessage() {
    return Optional.ofNullable(message);
  }

  /**
   * Returns the input row count of the subject.
   *
   * @return the input row count
   */
  public long getInputRows() {
    return inputRows;
  }

  /**
   * Returns the output row count of the view.
   *
   * @return the output row count
   */
  public long getOutputRows() {
    return outputRows;
  }

  /**
   * Returns the timed execute+extract samples in milliseconds, one per measured iteration.
   *
   * @return the execute+extract samples
   */
  @Nonnull
  public List<Double> getExecuteExtractSamplesMs() {
    return executeExtractSamplesMs;
  }

  /**
   * Returns the load duration of the subject in milliseconds.
   *
   * @return the load duration
   */
  public double getLoadMs() {
    return loadMs;
  }
}
