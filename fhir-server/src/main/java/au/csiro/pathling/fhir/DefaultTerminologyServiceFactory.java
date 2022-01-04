/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhir;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.terminology.DefaultTerminologyService;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.UUIDFactory;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import java.util.UUID;
import javax.annotation.Nonnull;
import org.slf4j.Logger;

/**
 * Default implementation of {@link TerminologyServiceFactory} providing {@link TerminologyService}
 * implemented using {@link TerminologyClient} with with given configuration.
 *
 * @author John Grimes
 * @author Piotr Szul
 */
public class DefaultTerminologyServiceFactory implements TerminologyServiceFactory {

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
  public DefaultTerminologyServiceFactory(@Nonnull final FhirContext fhirContext,
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
   * @return a shiny new TerminologyService instance
   */
  @Nonnull
  @Override
  public TerminologyService buildService(@Nonnull final Logger logger) {
    return buildService(logger, UUID::randomUUID);
  }


  /**
   * Builds a new instance.
   *
   * @param logger a {@link Logger} to use for logging
   * @param uuidFactory the {@link UUIDFactory to use for UUID generation}
   * @return a shiny new TerminologyService instance =
   */
  @Nonnull
  public TerminologyService buildService(@Nonnull final Logger logger,
      @Nonnull final UUIDFactory uuidFactory) {
    final TerminologyClient terminologyClient = TerminologyClient
        .build(FhirEncoders.contextFor(fhirVersion), terminologyServerUrl, socketTimeout,
            verboseRequestLogging, logger);
    return new DefaultTerminologyService(FhirEncoders.contextFor(fhirVersion), terminologyClient,
        uuidFactory);
  }


}
