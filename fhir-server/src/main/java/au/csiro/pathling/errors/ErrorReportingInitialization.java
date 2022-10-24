/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.errors;

import au.csiro.pathling.PathlingVersion;
import au.csiro.pathling.config.Configuration;
import io.sentry.Sentry;
import javax.annotation.Nonnull;
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
  private final Configuration configuration;

  @Nonnull
  private final PathlingVersion version;

  /**
   * @param configuration A {@link Configuration} object to control the behaviour of the listener
   * @param version A {@link PathlingVersion} object containing version information about the
   * server
   */
  public ErrorReportingInitialization(@Nonnull final Configuration configuration,
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
