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

import au.csiro.pathling.library.io.source.DatasetSource;
import jakarta.annotation.Nonnull;

/**
 * The result of the load phase for one subject resource type: a cached, FHIR-encoded {@link
 * DatasetSource} ready to be queried, the number of input rows materialized, and the wall-clock
 * cost of loading and encoding it.
 *
 * <p>Loading is performed once per distinct subject and shared by every case over that subject, so
 * the cost is amortized and excluded from the timed execute+extract region.
 */
public class LoadedSubject {

  @Nonnull private final DatasetSource source;

  private final long inputRows;

  private final double loadMs;

  /**
   * Constructs a loaded subject.
   *
   * @param source the cached, FHIR-encoded dataset source
   * @param inputRows the number of input rows materialized during loading
   * @param loadMs the wall-clock load duration in milliseconds
   */
  public LoadedSubject(
      @Nonnull final DatasetSource source, final long inputRows, final double loadMs) {
    this.source = source;
    this.inputRows = inputRows;
    this.loadMs = loadMs;
  }

  /**
   * Returns the cached dataset source.
   *
   * @return the dataset source
   */
  @Nonnull
  public DatasetSource getSource() {
    return source;
  }

  /**
   * Returns the number of input rows materialized during loading.
   *
   * @return the input row count
   */
  public long getInputRows() {
    return inputRows;
  }

  /**
   * Returns the wall-clock load duration in milliseconds.
   *
   * @return the load duration
   */
  public double getLoadMs() {
    return loadMs;
  }
}
