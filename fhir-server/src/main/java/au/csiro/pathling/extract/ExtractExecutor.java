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

package au.csiro.pathling.extract;

import static au.csiro.pathling.security.SecurityAspect.getCurrentUserId;

import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.io.ResultWriter;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Profile;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

/**
 * @author John Grimes
 */
@Component
@Profile("core")
@Getter
@Slf4j
public class ExtractExecutor extends ExtractQueryExecutor {

  @Nonnull
  private final ResultWriter resultWriter;

  @Nonnull
  private final ResultRegistry resultRegistry;

  /**
   * @param configuration a {@link QueryConfiguration} object to control the behaviour of the
   * executor
   * @param fhirContext a {@link FhirContext} for doing FHIR stuff
   * @param sparkSession a {@link SparkSession} for resolving Spark queries
   * @param database a {@link Database} for retrieving resources
   * @param terminologyClientFactory a {@link TerminologyServiceFactory} for resolving terminology
   * @param resultWriter a {@link ResultWriter} for writing results for later retrieval
   * @param resultRegistry a {@link ResultRegistry} for storing the mapping between request ID and
   * result URL
   */
  public ExtractExecutor(@Nonnull final QueryConfiguration configuration,
      @Nonnull final FhirContext fhirContext, @Nonnull final SparkSession sparkSession,
      @Nonnull final Database database,
      @Nonnull final Optional<TerminologyServiceFactory> terminologyClientFactory,
      @Nonnull final ResultWriter resultWriter,
      @Nonnull final ResultRegistry resultRegistry) {
    super(configuration, fhirContext, sparkSession, database,
        terminologyClientFactory);
    this.resultWriter = resultWriter;
    this.resultRegistry = resultRegistry;
  }

  /**
   * Executes an extract request.
   *
   * @param query an {@link ExtractRequest}
   * @param serverBase the base URL of this server, used to construct result URLs
   * @param requestId the ID of the request
   * @return an {@link ExtractResponse}
   */
  @Nonnull
  public ExtractResponse execute(@Nonnull final ExtractRequest query,
      @Nonnull final String serverBase, @Nonnull final String requestId) {
    log.info("Executing request: {}", query);
    final Dataset<Row> result = buildQuery(query);

    // Write the result and get the URL.
    final String resultUrl = resultWriter.write(result, requestId);

    // Get the current user, if authenticated, and store alongside the result for later 
    // authorization.
    final Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
    final Optional<String> currentUserId = getCurrentUserId(authentication);

    // Store a mapping between the request ID and the result URL, for later retrieval via the result
    // operation.
    resultRegistry.put(requestId, new Result(resultUrl, currentUserId));

    return new ExtractResponse(serverBase + "/$result?id=" + requestId);
  }
}
