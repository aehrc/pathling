/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir;

import static au.csiro.clinsight.utilities.Configuration.copyStringProps;

import au.csiro.clinsight.query.QueryExecutor;
import au.csiro.clinsight.query.spark.SparkQueryExecutor;
import au.csiro.clinsight.query.spark.SparkQueryExecutorConfiguration;
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
 * @author John Grimes
 */
public class AnalyticsServer extends RestfulServer {

  private static final Logger logger = LoggerFactory.getLogger(AnalyticsServer.class);
  private final AnalyticsServerConfiguration configuration;
  private QueryExecutor queryExecutor;

  public AnalyticsServer(AnalyticsServerConfiguration configuration) {
    super(buildFhirContext());

    logger.info("Creating new AnalyticsServer: " + configuration);
    this.configuration = configuration;
    setServerConformanceProvider(new AnalyticsServerCapabilities());
  }

  @Nonnull
  private static FhirContext buildFhirContext() {
    FhirContext fhirContext = FhirContext.forDstu3();
    return fhirContext;
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

  private void initializeQueryExecutor() {
    SparkQueryExecutorConfiguration executorConfig = new SparkQueryExecutorConfiguration();
    copyStringProps(configuration, executorConfig, Arrays
        .asList("sparkMasterUrl", "warehouseDirectory", "metastoreUrl", "metastoreUser",
            "metastorePassword", "databaseName", "executorMemory", "terminologyServerUrl"));
    if (configuration.getTerminologyClient() != null) {
      executorConfig.setTerminologyClient(configuration.getTerminologyClient());
    }
    if (configuration.getSparkSession() != null) {
      executorConfig.setSparkSession(configuration.getSparkSession());
    }

    queryExecutor = new SparkQueryExecutor(executorConfig, getFhirContext());
  }

  private void declareProviders() {
    List<Object> plainProviders = new ArrayList<>();
    plainProviders.add(new QueryOperationProvider(queryExecutor));
    setPlainProviders(plainProviders);
  }

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
