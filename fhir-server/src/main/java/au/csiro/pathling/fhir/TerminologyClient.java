/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhir;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.annotation.Transaction;
import ca.uhn.fhir.rest.annotation.TransactionParam;
import ca.uhn.fhir.rest.client.api.IRestfulClient;
import ca.uhn.fhir.rest.client.api.IRestfulClientFactory;
import ca.uhn.fhir.rest.client.api.ServerValidationModeEnum;
import java.util.List;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.StringType;
import org.slf4j.Logger;

/**
 * @author John Grimes
 */
public interface TerminologyClient extends IRestfulClient {

  /**
   * Invokes a "batch" request against the terminology server
   *
   * @param input a {@link Bundle} resource containing the requested operations
   * @return a {@link Bundle} containing the responses to the operations
   * @see <a href="https://www.hl7.org/fhir/R4/http.html#transaction">batch/transaction</a>
   */
  @Transaction
  Bundle batch(@TransactionParam Bundle input);

  /**
   * Invokes the "closure" operation against the ConceptMap resource.
   *
   * @param name The name that defines the particular context for the subsumption based closure
   * table
   * @param concept Concepts to add to the closure table
   * @param version A request to resynchronise - request to send all new entries since the nominated
   * version was sent by the server
   * @return A list of new entries (code / system --> code/system) that the client should add to its
   * closure table. The only kind of entry mapping equivalences that can be returned are equal,
   * specializes, subsumes and unmatched
   * @see <a href="https://www.hl7.org/fhir/R4/conceptmap-operation-closure.html">Operation $closure
   * on ConceptMap</a>
   */
  @Operation(name = "$closure")
  ConceptMap closure(@OperationParam(name = "name") StringType name,
      @OperationParam(name = "concept") List<Coding> concept,
      @OperationParam(name = "version") StringType version);

  /**
   * Build a new instance using the supplied {@link FhirContext} and configuration options.
   *
   * @param fhirContext the {@link FhirContext} used to build the client
   * @param terminologyServerUrl the URL of the terminology server this client will communicate
   * with
   * @param socketTimeout the number of milliseconds to wait for response data
   * @param verboseRequestLogging whether to log out verbose details of each request
   * @param logger a {@link Logger} to use for logging
   * @return a shiny new TerminologyClient instance
   */
  @Nonnull
  static TerminologyClient build(@Nonnull final FhirContext fhirContext,
      @Nonnull final String terminologyServerUrl, final int socketTimeout,
      final boolean verboseRequestLogging, @Nonnull final Logger logger) {
    final IRestfulClientFactory restfulClientFactory = fhirContext.getRestfulClientFactory();
    restfulClientFactory.setSocketTimeout(socketTimeout);
    restfulClientFactory.setServerValidationMode(ServerValidationModeEnum.NEVER);

    final TerminologyClient terminologyClient = restfulClientFactory
        .newClient(TerminologyClient.class, terminologyServerUrl);

    final ca.uhn.fhir.rest.client.interceptor.LoggingInterceptor loggingInterceptor =
        new ca.uhn.fhir.rest.client.interceptor.LoggingInterceptor();
    loggingInterceptor.setLogger(logger);
    loggingInterceptor.setLogRequestSummary(true);
    loggingInterceptor.setLogResponseSummary(true);
    if (verboseRequestLogging) {
      loggingInterceptor.setLogRequestHeaders(true);
      loggingInterceptor.setLogRequestBody(true);
      loggingInterceptor.setLogResponseHeaders(true);
      loggingInterceptor.setLogResponseBody(true);
    }
    terminologyClient.registerInterceptor(loggingInterceptor);

    return terminologyClient;
  }

}
