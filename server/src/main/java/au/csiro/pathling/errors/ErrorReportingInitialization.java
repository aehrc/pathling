/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.errors;

import au.csiro.pathling.PathlingVersion;
import au.csiro.pathling.config.ServerConfiguration;
import io.sentry.Sentry;
import jakarta.annotation.Nonnull;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Initialize Sentry error reporting upon the startup of the application.
 *
 * @author John Grimes
 */
@Component
public class ErrorReportingInitialization implements ApplicationListener<ApplicationReadyEvent> {

  @Nonnull
  private final ServerConfiguration configuration;

  @Nonnull
  private final PathlingVersion version;

  /**
   * @param configuration A {@link ServerConfiguration} object to control the behaviour of the
   * listener
   * @param version A {@link PathlingVersion} object containing version information about the
   * server
   */
  public ErrorReportingInitialization(@Nonnull final ServerConfiguration configuration,
      @Nonnull final PathlingVersion version) {
    this.configuration = configuration;
    this.version = version;
  }

  @Override
  @EventListener(ApplicationReadyEvent.class)
  public void onApplicationEvent(@Nonnull final ApplicationReadyEvent event) {
    // Configure Sentry.
    configuration.getSentryDsn().ifPresent(dsn -> Sentry.init(options -> {
      options.setDsn(dsn);
      version.getDescriptiveVersion().ifPresent(options::setRelease);
      configuration.getSentryEnvironment().ifPresent(options::setEnvironment);
    }));
  }

}
