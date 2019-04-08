/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir;

import static au.csiro.clinsight.utilities.Configuration.copyStringProps;

import au.csiro.clinsight.query.QueryExecutor;
import au.csiro.clinsight.query.QueryExecutorConfiguration;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.api.EncodingEnum;
import ca.uhn.fhir.rest.server.RestfulServer;
import ca.uhn.fhir.rest.server.interceptor.CorsInterceptor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nonnull;
import javax.servlet.ServletException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.cors.CorsConfiguration;

/**
 * A HAPI RestfulServer that understands how to satisfy aggregate queries over a set of STU3 FHIR
 * resources.
 *
 * @author John Grimes
 */
public class AnalyticsServer extends RestfulServer {

  private static final Logger logger = LoggerFactory.getLogger(AnalyticsServer.class);
  private final AnalyticsServerConfiguration configuration;
  private QueryExecutor queryExecutor;

  public AnalyticsServer(@Nonnull AnalyticsServerConfiguration configuration) {
    super(buildFhirContext());

    logger.info("Creating new AnalyticsServer: " + configuration);
    this.configuration = configuration;
    setServerConformanceProvider(new AnalyticsServerCapabilities());
  }

  @Nonnull
  private static FhirContext buildFhirContext() {
    return FhirContext.forDstu3();
  }

  @Override
  protected void initialize() throws ServletException {
    super.initialize();

    // Set default response encoding to JSON.
    setDefaultResponseEncoding(EncodingEnum.JSON);

    initializeQueryExecutor();
    declareProviders();
    defineCorsConfiguration();
  }

  /**
   * Initialise a new query executor, and pass through the relevant configuration parameters.
   */
  private void initializeQueryExecutor() {
    QueryExecutorConfiguration executorConfig = new QueryExecutorConfiguration();
    copyStringProps(configuration, executorConfig, Arrays
        .asList("sparkMasterUrl", "warehouseDirectory", "metastoreUrl", "metastoreUser",
            "metastorePassword", "databaseName", "executorMemory", "terminologyServerUrl"));
    executorConfig.setExplainQueries(configuration.getExplainQueries());
    if (configuration.getTerminologyClient() != null) {
      executorConfig.setTerminologyClient(configuration.getTerminologyClient());
    }
    if (configuration.getSparkSession() != null) {
      executorConfig.setSparkSession(configuration.getSparkSession());
    }

    queryExecutor = new QueryExecutor(executorConfig, getFhirContext());
  }

  /**
   * Declare the providers which will handle requests to this server. This server only knows how to
   * do one thing - satisfy `aggregate-query` requests. This is a system-wide operation, so it is
   * delivered using a plain provider.
   */
  private void declareProviders() {
    List<Object> plainProviders = new ArrayList<>();
    plainProviders.add(new QueryOperationProvider(queryExecutor));
    setPlainProviders(plainProviders);
  }

  /**
   * Declare a CORS interceptor, using the CorsConfiguration from Spring. This is required to enable
   * web-based applications hosted on different domains to communicate with this server.
   */
  private void defineCorsConfiguration() {
    CorsConfiguration corsConfig = new CorsConfiguration();
    corsConfig.addAllowedHeader("x-fhir-starter");
    corsConfig.addAllowedHeader("Origin");
    corsConfig.addAllowedHeader("Accept");
    corsConfig.addAllowedHeader("X-Requested-With");
    corsConfig.addAllowedHeader("Content-Type");

    corsConfig.addAllowedOrigin("*");

    corsConfig.addExposedHeader("Location");
    corsConfig.addExposedHeader("Content-Location");
    corsConfig.setAllowedMethods(Arrays.asList("GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"));

    CorsInterceptor interceptor = new CorsInterceptor(corsConfig);
    registerInterceptor(interceptor);
  }

}
