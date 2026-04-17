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

package au.csiro.pathling.fhirpath;

import au.csiro.pathling.sql.TraceCollector;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@link TraceCollector} backed by a plain {@link ArrayList}. This implementation is
 * intentionally not serializable — it will fail with a clear error if used in a distributed Spark
 * context. Use this for single-resource evaluation in local mode.
 *
 * @author John Grimes
 */
public class ListTraceCollector implements TraceCollector {

  private final List<TraceEntry> entries = new ArrayList<>();

  @Override
  public void add(
      @Nonnull final String label, @Nonnull final String fhirType, @Nullable final Object value) {
    entries.add(new TraceEntry(label, fhirType, value));
  }

  /**
   * Returns all collected trace entries.
   *
   * @return an unmodifiable view of the collected entries
   */
  @Nonnull
  public List<TraceEntry> getEntries() {
    return List.copyOf(entries);
  }

  /** Removes all collected trace entries, resetting the collector to its initial state. */
  public void clear() {
    entries.clear();
  }

  /**
   * A single trace entry captured during evaluation.
   *
   * @param label the trace label
   * @param fhirType the FHIR type code
   * @param value the traced value
   */
  public record TraceEntry(
      @Nonnull String label, @Nonnull String fhirType, @Nullable Object value) {}
}
