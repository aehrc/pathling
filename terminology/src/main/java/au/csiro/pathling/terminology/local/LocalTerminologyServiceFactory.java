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
import lombok.extern.slf4j.Slf4j;

/**
 * A serialisable {@link TerminologyServiceFactory} that builds a {@link LocalTerminologyService} on
 * each executor JVM, memoised per JVM exactly as the remote factory is.
 *
 * <p>The factory carries only serialisable state: the terminology configuration (including the
 * local store path and cache settings) and a snapshot of the driver's Hadoop configuration, which
 * carries {@code spark.hadoop.*} settings and credential providers so the store's filesystem is
 * reachable on executors where a Hadoop {@code Configuration} is neither available nor
 * serialisable.
 *
 * @param configuration the terminology configuration, including the local store settings
 * @param hadoopConfiguration a snapshot of the driver's Hadoop configuration
 * @author John Grimes
 */
@Slf4j
public record LocalTerminologyServiceFactory(
    @Nonnull TerminologyConfiguration configuration,
    @Nonnull Map<String, String> hadoopConfiguration)
    implements TerminologyServiceFactory {

  @Serial private static final long serialVersionUID = 6210349769947958142L;

  @Nonnull
  private static final ObjectHolder<LocalTerminologyServiceFactory, TerminologyService>
      terminologyServiceHolder =
          ObjectHolder.singleton(LocalTerminologyServiceFactory::createService);

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
    return terminologyServiceHolder.getOrCreate(this);
  }

  @Nonnull
  @Override
  public TerminologyConfiguration getConfiguration() {
    return configuration;
  }

  @Nonnull
  private TerminologyService createService() {
    log.debug(
        "Creating LocalTerminologyService for store: {}",
        configuration.getLocal() == null ? null : configuration.getLocal().getStoragePath());
    return new LocalTerminologyService(configuration, hadoopConfiguration);
  }
}
