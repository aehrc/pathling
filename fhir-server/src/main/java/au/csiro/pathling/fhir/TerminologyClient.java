/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhir;

import static au.csiro.pathling.utilities.Preconditions.checkNotNull;

import au.csiro.pathling.config.TerminologyAuthConfiguration;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.annotation.Elements;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.annotation.RequiredParam;
import ca.uhn.fhir.rest.annotation.Search;
import ca.uhn.fhir.rest.annotation.Transaction;
import ca.uhn.fhir.rest.annotation.TransactionParam;
import ca.uhn.fhir.rest.client.api.IRestfulClient;
import ca.uhn.fhir.rest.client.api.IRestfulClientFactory;
import ca.uhn.fhir.rest.client.api.ServerValidationModeEnum;
import ca.uhn.fhir.rest.param.UriParam;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClients;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.CodeSystem;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.ValueSet;
import org.slf4j.Logger;

/**
 * A HAPI "annotation client" for communicating with a R4 FHIR terminology service.
 *
 * @author John Grimes
 * @see <a href="https://hapifhir.io/hapi-fhir/docs/client/annotation_client.html">Annotation
 * Client</a>
 */
public interface TerminologyClient extends IRestfulClient {

  /**
   * Performs a CodeSystem search using a single URI.
   *
   * @param uri a URI string
   * @param elements a set of elements to return within the response
   * @return a list of {@link CodeSystem} objects
   */
  @Search
  @Nullable
  List<CodeSystem> searchCodeSystems(@Nonnull @RequiredParam(name = CodeSystem.SP_URL) UriParam uri,
      @Nonnull @Elements Set<String> elements);

  /**
   * Invokes an "expand" request against the terminology server, using an inline ValueSet resource
   *
   * @param valueSet a {@link ValueSet} resource to be expanded by the terminology server
   * @param count the number of results to return
   * @return a ValueSet containing the expansion result
   * @see <a href="https://www.hl7.org/fhir/R4/valueset-operation-expand.html">Operation $expand on
   * ValueSet</a>
   */
  @Operation(name = "$expand", type = ValueSet.class)
  @Nullable
  ValueSet expand(@Nonnull @OperationParam(name = "valueSet") ValueSet valueSet,
      @Nonnull @OperationParam(name = "count") IntegerType count);

  /**
   * Invokes the "closure" operation against the ConceptMap resource.
   *
   * @param name The name that defines the particular context for the subsumption based closure
   * table
   * @return an empty {@link ConceptMap}
   * @see <a href="https://www.hl7.org/fhir/R4/conceptmap-operation-closure.html">Operation $closure
   * on ConceptMap</a>
   */
  @Operation(name = "$closure")
  @SuppressWarnings("UnusedReturnValue")
  ConceptMap initialiseClosure(@Nonnull @OperationParam(name = "name") StringType name);

  /**
   * Invokes the "closure" operation against the ConceptMap resource.
   *
   * @param name the name that defines the particular context for the subsumption based closure
   * table
   * @param concept concepts to add to the closure table
   * @return a list of new entries ({@code code / system --> code/system}) that the client should
   * add to its closure table. The only kind of entry mapping equivalences that can be returned are
   * equal, specializes, subsumes and unmatched
   * @see <a href="https://www.hl7.org/fhir/R4/conceptmap-operation-closure.html">Operation $closure
   * on ConceptMap</a>
   */
  @Operation(name = "$closure")
  @Nullable
  ConceptMap closure(@Nonnull @OperationParam(name = "name") StringType name,
      @Nonnull @OperationParam(name = "concept") List<Coding> concept);

  /**
   * Invokes the transaction/batch operation with the provided bundle.
   *
   * @param inputBundle a bundle describing the batch/transaction to execute
   * @return the response bundle
   * @see <a href="https://www.hl7.org/fhir/http.html#transaction">Batch/Transaction</a>
   */
  @Transaction
  @Nullable
  Bundle batch(@Nonnull @TransactionParam Bundle inputBundle);

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
      final boolean verboseRequestLogging, @Nonnull final TerminologyAuthConfiguration authConfig,
      @Nonnull final Logger logger) {
    final IRestfulClientFactory restfulClientFactory = fhirContext.getRestfulClientFactory();
    restfulClientFactory.setHttpClient(HttpClients.custom().setRetryHandler(
        new DefaultHttpRequestRetryHandler()).build());
    restfulClientFactory.setSocketTimeout(socketTimeout);
    restfulClientFactory.setServerValidationMode(ServerValidationModeEnum.NEVER);

    final TerminologyClient terminologyClient = restfulClientFactory
        .newClient(TerminologyClient.class, terminologyServerUrl);

    final ca.uhn.fhir.rest.client.interceptor.LoggingInterceptor loggingInterceptor =
        new ca.uhn.fhir.rest.client.interceptor.LoggingInterceptor();
    loggingInterceptor.setLogger(logger);
    loggingInterceptor.setLogRequestSummary(true);
    loggingInterceptor.setLogResponseSummary(true);
    loggingInterceptor.setLogRequestHeaders(false);
    loggingInterceptor.setLogResponseHeaders(false);
    if (verboseRequestLogging) {
      loggingInterceptor.setLogRequestBody(true);
      loggingInterceptor.setLogResponseBody(true);
    }
    terminologyClient.registerInterceptor(loggingInterceptor);

    if (authConfig.isEnabled()) {
      checkNotNull(authConfig.getTokenEndpoint());
      checkNotNull(authConfig.getClientId());
      checkNotNull(authConfig.getClientSecret());
      final ClientAuthInterceptor clientAuthInterceptor = new ClientAuthInterceptor(
          authConfig.getTokenEndpoint(), authConfig.getClientId(), authConfig.getClientSecret(),
          authConfig.getScope());
      terminologyClient.registerInterceptor(clientAuthInterceptor);
    }

    return terminologyClient;
  }

}
