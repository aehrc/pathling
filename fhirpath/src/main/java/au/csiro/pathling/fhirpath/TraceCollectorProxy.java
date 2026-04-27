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
import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A serializable {@link TraceCollector} proxy that delegates to a registered collector via a static
 * registry. Only the key is serialized — the actual collector stays on the driver JVM.
 *
 * <p>Usage:
 *
 * <ol>
 *   <li>Create the real collector (e.g., {@link ListTraceCollector}).
 *   <li>Create a proxy via {@link #create(TraceCollector)}.
 *   <li>Pass the proxy into the evaluation (it survives Spark serialization).
 *   <li>After materialization, read from the real collector.
 *   <li>Call {@link #close()} to remove the registry entry.
 * </ol>
 *
 * @author John Grimes
 */
public class TraceCollectorProxy implements TraceCollector, Serializable, AutoCloseable {

  private static final long serialVersionUID = 1L;

  private static final ConcurrentHashMap<String, TraceCollector> REGISTRY =
      new ConcurrentHashMap<>();

  @Nonnull private final String key;

  private TraceCollectorProxy(@Nonnull final String key) {
    this.key = key;
  }

  /**
   * Creates a new proxy that delegates to the given collector.
   *
   * @param delegate the real collector to delegate to
   * @return a serializable proxy
   */
  @Nonnull
  public static TraceCollectorProxy create(@Nonnull final TraceCollector delegate) {
    final String key = UUID.randomUUID().toString();
    REGISTRY.put(key, delegate);
    return new TraceCollectorProxy(key);
  }

  @Override
  public void add(
      @Nonnull final String label, @Nonnull final String fhirType, @Nullable final Object value) {
    final TraceCollector delegate = REGISTRY.get(key);
    if (delegate != null) {
      delegate.add(label, fhirType, value);
    }
  }

  /** Removes this proxy's entry from the registry. Call after materialization is complete. */
  @Override
  public void close() {
    REGISTRY.remove(key);
  }
}
