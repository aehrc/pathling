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
import au.csiro.pathling.terminology.TerminologyFunctions;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.UUIDFactory;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.MDC;

import static au.csiro.pathling.utilities.Preconditions.checkPresent;
import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.when;

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

  @Nullable
  private static TerminologyService terminologyService = null;

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

  @Nonnull
  @Override
  public Result memberOf(@Nonnull final Dataset<Row> dataset, @Nonnull final Column value,
      final String valueSetUri) {

    final Column arrayColumn = when(value.isNotNull(), array(value))
        .otherwise(functions.lit(null));

    // Prepare the data which will be used within the map operation. All of these things must be
    // Serializable.
    final Dataset<Row> resultDataset = TerminologyFunctions.memberOf(arrayColumn, valueSetUri,
        dataset, "result", this, MDC.get("requestId"));
    final Column resultColumn = functions.col("result");
    return new Result(resultDataset, resultColumn);
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
    if (terminologyService == null) {
      final TerminologyClient terminologyClient = TerminologyClient
          .build(FhirEncoders.contextFor(fhirVersion), terminologyServerUrl, socketTimeout,
              verboseRequestLogging, authConfig, logger);
      terminologyService = new DefaultTerminologyService(FhirEncoders.contextFor(fhirVersion),
          terminologyClient, uuidFactory);
    }
    return terminologyService;
  }

}