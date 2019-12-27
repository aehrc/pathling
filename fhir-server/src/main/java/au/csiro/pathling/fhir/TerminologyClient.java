/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.fhir;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.annotation.*;
import ca.uhn.fhir.rest.client.api.IRestfulClient;
import ca.uhn.fhir.rest.client.api.IRestfulClientFactory;
import ca.uhn.fhir.rest.client.api.ServerValidationModeEnum;
import java.util.List;
import org.hl7.fhir.r4.model.*;
import org.slf4j.Logger;

/**
 * @author John Grimes
 */
public interface TerminologyClient extends IRestfulClient {

  @Metadata
  CapabilityStatement getServerMetadata();

  @Transaction
  Bundle batch(@TransactionParam Bundle input);

  @Operation(name = "$closure")
  ConceptMap closure(@OperationParam(name = "name") StringType name,
      @OperationParam(name = "concept") List<Coding> concept,
      @OperationParam(name = "version") StringType version);

  static TerminologyClient build(FhirContext fhirContext, String terminologyServerUrl,
      int socketTimeout, boolean verboseRequestLogging, Logger logger) {
    IRestfulClientFactory restfulClientFactory = fhirContext.getRestfulClientFactory();
    restfulClientFactory.setSocketTimeout(socketTimeout);
    restfulClientFactory.setServerValidationMode(ServerValidationModeEnum.NEVER);

    TerminologyClient terminologyClient = restfulClientFactory
        .newClient(TerminologyClient.class, terminologyServerUrl);

    ca.uhn.fhir.rest.client.interceptor.LoggingInterceptor loggingInterceptor =
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
