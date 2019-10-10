/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir;

import static au.csiro.clinsight.utilities.Configuration.copyStringProps;

import au.csiro.clinsight.query.AggregateExecutor;
import au.csiro.clinsight.query.AggregateExecutorConfiguration;
import au.csiro.clinsight.query.ResourceReader;
import au.csiro.clinsight.update.ImportProvider;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.api.EncodingEnum;
import ca.uhn.fhir.rest.server.RestfulServer;
import ca.uhn.fhir.rest.server.interceptor.CorsInterceptor;
import com.cerner.bunsen.FhirEncoders;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nonnull;
import javax.servlet.ServletException;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.cors.CorsConfiguration;

/**
 * A HAPI RestfulServer that understands how to satisfy aggregate queries over a set of FHIR
 * resources.
 *
 * @author John Grimes
 */
public class AnalyticsServer extends RestfulServer {

  private static final Logger logger = LoggerFactory.getLogger(AnalyticsServer.class);
  private final AnalyticsServerConfiguration configuration;
  private final AnalyticsServerCapabilities serverCapabilities;
  private SparkSession spark;
  private TerminologyClient terminologyClient;
  private FhirEncoders fhirEncoders;
  private AggregateExecutor aggregateExecutor;

  public AnalyticsServer(@Nonnull AnalyticsServerConfiguration configuration) {
    super(buildFhirContext());
    logger.info("Creating new AnalyticsServer: " + configuration);
    this.configuration = configuration;
    this.serverCapabilities = new AnalyticsServerCapabilities(configuration);
    setServerConformanceProvider(serverCapabilities);
  }

  @Nonnull
  private static FhirContext buildFhirContext() {
    logger.info("Creating R4 FHIR context");
    return FhirContext.forR4();
  }

  @Override
  protected void initialize() throws ServletException {
    super.initialize();

    // Set default response encoding to JSON.
    setDefaultResponseEncoding(EncodingEnum.JSON);

    initializeSpark();
    initializeTerminologyClient();
    initializeFhirEncoders();
    initializeAggregateExecutor();
    declareProviders();
    defineCorsConfiguration();
  }

  private void initializeSpark() {
    logger.info("Initializing Spark session");
    spark = SparkSession.builder()
        .appName("clinsight-server")
        .config("spark.master", configuration.getSparkMasterUrl())
        .config("spark.executor.memory", configuration.getExecutorMemory())
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.shuffle.service.enabled", "true")
        .config("spark.scheduler.mode", "FAIR")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.shuffle.partitions", "36")
        .getOrCreate();
  }

  private void initializeTerminologyClient() {
    logger.info("Creating FHIR terminology client");
    terminologyClient = getFhirContext()
        .newRestfulClient(TerminologyClient.class, configuration.getTerminologyServerUrl());
  }

  private void initializeFhirEncoders() {
    logger.info("Creating R4 FHIR encoders");
    fhirEncoders = FhirEncoders.forR4().getOrCreate();
  }

  /**
   * Initialise a new aggregate executor, and pass through the relevant configuration parameters.
   */
  private void initializeAggregateExecutor() {
    ResourceReader resourceReader = new ResourceReader(spark, configuration.getWarehouseUrl(),
        configuration.getDatabaseName());
    AggregateExecutorConfiguration executorConfig = new AggregateExecutorConfiguration(spark,
        terminologyClient, resourceReader);
    copyStringProps(configuration, executorConfig,
        Arrays.asList("version", "warehouseUrl", "databaseName", "executorMemory"));
    executorConfig.setExplainQueries(configuration.isExplainQueries());
    executorConfig.setShufflePartitions(configuration.getShufflePartitions());
    executorConfig.setLoadPartitions(configuration.getLoadPartitions());

    aggregateExecutor = new AggregateExecutor(executorConfig);
    serverCapabilities.setAggregateExecutor(aggregateExecutor);
  }

  /**
   * Declare the providers which will handle requests to this server.
   */
  private void declareProviders() {
    List<Object> plainProviders = new ArrayList<>();
    plainProviders.add(new AggregateOperationProvider(aggregateExecutor));
    plainProviders.add(new ImportProvider(configuration, spark, fhirEncoders));
    registerProviders(plainProviders);
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
