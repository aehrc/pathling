/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhir;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.terminology.DefaultTerminologyService;
import au.csiro.pathling.terminology.TerminologyService;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import javax.annotation.Nonnull;
import org.slf4j.Logger;

/**
 * Uses the FhirEncoders class to create a FhirContext, then creates a TerminologyClient with some
 * configuration. Used for code that runs on Spark workers.
 *
 * @author John Grimes
 */
public class DefaultTerminologyClientFactory implements TerminologyClientFactory {

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
  public DefaultTerminologyClientFactory(@Nonnull final FhirContext fhirContext,
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
  @Override
  @Deprecated
  public TerminologyClient build(@Nonnull final Logger logger) {
    return TerminologyClient
        .build(FhirEncoders.contextFor(fhirVersion), terminologyServerUrl, socketTimeout,
            verboseRequestLogging, logger);
  }

  /**
   * Builds a new instance.
   *
   * @param logger a {@link Logger} to use for logging
   * @return a shiny new TerminologyService instance
   */
  @Nonnull
  @Override
  public TerminologyService buildService(@Nonnull final Logger logger) {
    return new DefaultTerminologyService(FhirEncoders.contextFor(fhirVersion), build(logger));
  }


}
