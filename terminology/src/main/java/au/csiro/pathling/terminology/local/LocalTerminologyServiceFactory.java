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

package au.csiro.pathling.terminology.local;

import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.utilities.ObjectHolder;
import jakarta.annotation.Nonnull;
import java.io.Serial;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

/**
 * A serialisable {@link TerminologyServiceFactory} that builds a {@link LocalTerminologyService} on
 * each executor JVM, memoised once per distinct store configuration within the JVM. Unlike the
 * remote factory, which permits only one configuration per process, local mode may legitimately
 * work with more than one store in a single JVM (for example, when a process queries two stores in
 * turn), so each distinct configuration keeps its own service.
 *
 * <p>The factory carries only serialisable state: the terminology configuration (including the
 * local store path and cache settings) and a snapshot of the driver's Hadoop configuration, which
 * carries {@code spark.hadoop.*} settings and credential providers so the store's filesystem is
 * reachable on executors where a Hadoop {@code Configuration} is neither available nor
 * serialisable.
 *
 * @author John Grimes
 */
@Slf4j
@EqualsAndHashCode
public final class LocalTerminologyServiceFactory implements TerminologyServiceFactory {

  @Serial private static final long serialVersionUID = 6210349769947958142L;

  @Nonnull
  private static final ObjectHolder<LocalTerminologyServiceFactory, TerminologyService>
      terminologyServiceHolder =
          ObjectHolder.perConfiguration(LocalTerminologyServiceFactory::createService);

  @Nonnull private final TerminologyConfiguration configuration;
  @Nonnull private final Map<String, String> hadoopConfiguration;

  /**
   * The per-instance memo of the built service. The UDFs call {@link #build()} once per row, and
   * the per-JVM holder looks the service up by this factory's full state (including the Hadoop
   * configuration snapshot) on every call, so the resolved service is cached here after the first
   * call. The memo is transient and rebuilt after deserialisation on each executor.
   */
  @EqualsAndHashCode.Exclude private transient volatile TerminologyService service;

  /**
   * Creates a factory.
   *
   * @param configuration the terminology configuration, including the local store settings
   * @param hadoopConfiguration a snapshot of the driver's Hadoop configuration
   */
  public LocalTerminologyServiceFactory(
      @Nonnull final TerminologyConfiguration configuration,
      @Nonnull final Map<String, String> hadoopConfiguration) {
    this.configuration = configuration;
    this.hadoopConfiguration = hadoopConfiguration;
  }

  /**
   * Resets the cached local terminology services. Useful for testing or when configuration changes
   * require a fresh instance.
   */
  public static synchronized void reset() {
    log.info("Resetting local terminology services");
    terminologyServiceHolder.reset();
  }

  @Nonnull
  @Override
  public TerminologyService build() {
    TerminologyService cached = service;
    if (cached == null) {
      cached = terminologyServiceHolder.getOrCreate(this);
      service = cached;
    }
    return cached;
  }

  @Nonnull
  @Override
  public TerminologyConfiguration getConfiguration() {
    return configuration;
  }

  /**
   * Returns the snapshot of the driver's Hadoop configuration carried by this factory.
   *
   * @return the Hadoop configuration snapshot
   */
  @Nonnull
  public Map<String, String> hadoopConfiguration() {
    return hadoopConfiguration;
  }

  @Nonnull
  private TerminologyService createService() {
    log.debug(
        "Creating LocalTerminologyService for store: {}",
        configuration.getLocal() == null ? null : configuration.getLocal().getStoragePath());
    return new LocalTerminologyService(configuration, hadoopConfiguration);
  }
}
