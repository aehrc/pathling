/*
 * Copyright 2022 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.fhir;

import au.csiro.pathling.config.TerminologyAuthConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.terminology.DefaultTerminologyService;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.UUIDFactory;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import java.util.UUID;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.slf4j.Logger;

/**
 * Default implementation of {@link TerminologyServiceFactory} providing {@link TerminologyService}
 * implemented using {@link TerminologyClient} with given configuration.
 *
 * @author John Grimes
 * @author Piotr Szul
 */
@Getter
public class DefaultTerminologyServiceFactory implements TerminologyServiceFactory {

  private static final long serialVersionUID = 8862251697418622614L;

  @Nonnull
  private final FhirVersionEnum fhirVersion;

  @Nonnull
  private final String terminologyServerUrl;

  private final int socketTimeout;

  private final boolean verboseRequestLogging;

  @Nonnull
  private final TerminologyAuthConfiguration authConfig;

  /**
   * @param fhirContext the {@link FhirContext} used to build the client
   * @param terminologyServerUrl the URL of the terminology server this client will communicate
   * with
   * @param socketTimeout the number of milliseconds to wait for response data
   * @param verboseRequestLogging whether to log out verbose details of each request
   */
  public DefaultTerminologyServiceFactory(@Nonnull final FhirContext fhirContext,
      @Nonnull final String terminologyServerUrl, final int socketTimeout,
      final boolean verboseRequestLogging, @Nonnull final TerminologyAuthConfiguration authConfig) {
    this.fhirVersion = fhirContext.getVersion().getVersion();
    this.terminologyServerUrl = terminologyServerUrl;
    this.socketTimeout = socketTimeout;
    this.verboseRequestLogging = verboseRequestLogging;
    this.authConfig = authConfig;
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
            verboseRequestLogging, authConfig, logger);
    return new DefaultTerminologyService(FhirEncoders.contextFor(fhirVersion), terminologyClient,
        uuidFactory);
  }

}
