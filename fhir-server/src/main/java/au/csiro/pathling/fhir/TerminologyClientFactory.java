/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhir;

import au.csiro.pathling.encoders.FhirEncoders;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import java.io.Serializable;
import javax.annotation.Nonnull;
import org.slf4j.Logger;

/**
 * Uses the FhirEncoders class to create a FhirContext, then creates a TerminologyClient with some
 * configuration. Used for code that runs on Spark workers.
 *
 * @author John Grimes
 */
public class TerminologyClientFactory implements Serializable {

  private static final long serialVersionUID = 8862251697418622614L;

  @Nonnull
  private final FhirVersionEnum fhirVersion;

  @Nonnull
  private final String terminologyServerUrl;

  private final int socketTimeout;

  private final boolean verboseRequestLogging;

  /**
   * @param fhirContext the {@link FhirContext} used to build the client
   * @param terminologyServerUrl the URL of the terminology server this client will communicate
   * with
   * @param socketTimeout the number of milliseconds to wait for response data
   * @param verboseRequestLogging whether to log out verbose details of each request
   */
  public TerminologyClientFactory(@Nonnull final FhirContext fhirContext,
      @Nonnull final String terminologyServerUrl, final int socketTimeout,
      final boolean verboseRequestLogging) {
    this.fhirVersion = fhirContext.getVersion().getVersion();
    this.terminologyServerUrl = terminologyServerUrl;
    this.socketTimeout = socketTimeout;
    this.verboseRequestLogging = verboseRequestLogging;
  }

  /**
   * Builds a new instance.
   *
   * @param logger a {@link Logger} to use for logging
   * @return a shiny new TerminologyClient instance
   */
  @Nonnull
  public TerminologyClient build(@Nonnull final Logger logger) {
    return TerminologyClient
        .build(FhirEncoders.contextFor(fhirVersion), terminologyServerUrl, socketTimeout,
            verboseRequestLogging, logger);
  }

}
