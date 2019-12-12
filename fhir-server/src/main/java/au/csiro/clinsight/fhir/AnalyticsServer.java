/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir;

import static au.csiro.clinsight.utilities.Configuration.copyStringProps;

import au.csiro.clinsight.bunsen.FhirEncoders;
import au.csiro.clinsight.query.AggregateExecutor;
import au.csiro.clinsight.query.AggregateExecutorConfiguration;
import au.csiro.clinsight.query.AggregateOperationProvider;
import au.csiro.clinsight.query.ResourceReader;
import au.csiro.clinsight.update.ImportProvider;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.api.EncodingEnum;
import ca.uhn.fhir.rest.server.RestfulServer;
import ca.uhn.fhir.rest.server.interceptor.CorsInterceptor;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.conf.Configuration;
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
  private SparkSession spark;
  private TerminologyClient terminologyClient;
  private FhirEncoders fhirEncoders;
  private AggregateExecutor aggregateExecutor;

  public AnalyticsServer(@Nonnull AnalyticsServerConfiguration configuration) {
    super(buildFhirContext());
    logger.info("Creating new AnalyticsServer: " + configuration);
    this.configuration = configuration;
  }

  @Nonnull
  private static FhirContext buildFhirContext() {
    logger.info("Creating R4 FHIR context");
    return FhirContext.forR4();
  }

  @Override
  protected void initialize() throws ServletException {
    super.initialize();

    try {
      // Set default response encoding to JSON.
      setDefaultResponseEncoding(EncodingEnum.JSON);

      initializeSpark();

      initializeTerminologyClient();
      initializeFhirEncoders();
      initializeAggregateExecutor();
      declareProviders();
      defineCorsConfiguration();

      // Initialise the capability statement.
      AnalyticsServerCapabilities serverCapabilities = new AnalyticsServerCapabilities(
          configuration);
      serverCapabilities.setAggregateExecutor(aggregateExecutor);
      setServerConformanceProvider(serverCapabilities);

      logger.info("Ready for query");
    } catch (Exception e) {
      throw new ServletException("Error initializing AnalyticsServer", e);
    }
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
        .config("spark.sql.shuffle.partitions", configuration.getShufflePartitions())
        .getOrCreate();
    if (configuration.getAwsAccessKeyId() != null
        && configuration.getAwsSecretAccessKey() != null) {
      Configuration hadoopConfiguration = spark.sparkContext().hadoopConfiguration();
      hadoopConfiguration.set("fs.s3a.access.key", configuration.getAwsAccessKeyId());
      hadoopConfiguration.set("fs.s3a.secret.key", configuration.getAwsSecretAccessKey());
    }
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
  private void initializeAggregateExecutor() throws IOException, URISyntaxException {
    ResourceReader resourceReader = new ResourceReader(spark, configuration.getWarehouseUrl(),
        configuration.getDatabaseName());
    AggregateExecutorConfiguration executorConfig = new AggregateExecutorConfiguration(spark,
        getFhirContext(), new FreshFhirContextFactory(), terminologyClient, resourceReader);
    copyStringProps(configuration, executorConfig,
        Arrays.asList("version", "warehouseUrl", "databaseName", "executorMemory"));
    executorConfig.setExplainQueries(configuration.isExplainQueries());
    executorConfig.setShufflePartitions(configuration.getShufflePartitions());

    aggregateExecutor = new AggregateExecutor(executorConfig);
  }

  /**
   * Declare the providers which will handle requests to this server.
   */
  private void declareProviders() {
    FhirContextFactory fhirContextFactory = new FreshFhirContextFactory();
    List<Object> providers = new ArrayList<>();
    providers.add(new AggregateOperationProvider(aggregateExecutor));
    providers.add(new ImportProvider(configuration, spark, fhirEncoders, fhirContextFactory));
    providers.add(new StructureDefinitionProvider(getFhirContext()));
    providers.add(new OperationDefinitionProvider(getFhirContext()));
    registerProviders(providers);
  }

  /**
   * Declare a CORS interceptor, using the CorsConfiguration from Spring. This is required to enable
   * web-based applications hosted on different domains to communicate with this server.
   */
  private void defineCorsConfiguration() {
    CorsConfiguration corsConfig = new CorsConfiguration();
    corsConfig.setAllowedOrigins(configuration.getCorsAllowedOrigins());
    corsConfig.setAllowedMethods(Arrays.asList("GET", "POST"));
    corsConfig.setAllowedHeaders(Collections.singletonList("Content-Type"));
    corsConfig.setMaxAge(600L);

    CorsInterceptor interceptor = new CorsInterceptor(corsConfig);
    registerInterceptor(interceptor);
  }

  @Override
  public void addHeadersToResponse(HttpServletResponse theHttpResponse) {
    // This removes the information-leaking `X-Powered-By` header from responses. We will need to
    // keep an eye on this to make sure that we don't disable and future functionality placed
    // within this method in the super.
  }
}
