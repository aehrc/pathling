/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhir;

import static au.csiro.pathling.utilities.Configuration.copyStringProps;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.query.*;
import au.csiro.pathling.update.ImportExecutor;
import au.csiro.pathling.update.ImportProvider;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.rest.api.EncodingEnum;
import ca.uhn.fhir.rest.server.ApacheProxyAddressStrategy;
import ca.uhn.fhir.rest.server.FifoMemoryPagingProvider;
import ca.uhn.fhir.rest.server.IResourceProvider;
import ca.uhn.fhir.rest.server.RestfulServer;
import ca.uhn.fhir.rest.server.interceptor.CorsInterceptor;
import ca.uhn.fhir.rest.server.interceptor.LoggingInterceptor;
import ca.uhn.fhir.rest.server.interceptor.ResponseHighlighterInterceptor;
import java.io.IOException;
import java.net.MalformedURLException;
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
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.ResourceType;
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

  private static final int DEFAULT_PAGE_SIZE = 100;
  private static final int MAX_PAGE_SIZE = Integer.MAX_VALUE;
  private static final int SEARCH_MAP_SIZE = 10;

  private static final Logger logger = LoggerFactory.getLogger(AnalyticsServer.class);
  private final AnalyticsServerConfiguration configuration;

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

      printLogo(logger);

      logger.info("Initializing Spark session");
      SparkSession spark = buildSpark(configuration);
      ResourceReader resourceReader = buildResourceReader(configuration, spark);

      logger.info("Creating R4 FHIR encoders");
      FhirEncoders fhirEncoders = buildFhirEncoders();

      logger.info("Creating FHIR terminology client: " + configuration.getTerminologyServerUrl());
      TerminologyClient terminologyClient = buildTerminologyClient(configuration, getFhirContext(),
          logger);
      ExecutorConfiguration executorConfig = buildExecutorConfiguration(configuration,
          spark, getFhirContext(), fhirEncoders, resourceReader, terminologyClient);

      // Use a proxy address strategy, which allows proxies to control the server base address with
      // the use of the X-Forwarded-Host and X-Forwarded-Proto headers.
      setServerAddressStrategy(ApacheProxyAddressStrategy.forHttp());

      // Register the import provider.
      FhirContextFactory fhirContextFactory = new FhirContextFactory(FhirVersionEnum.R4);
      ImportExecutor importExecutor = new ImportExecutor(configuration, spark, fhirEncoders,
          fhirContextFactory, executorConfig.getResourceReader());
      ImportProvider importProvider = new ImportProvider(importExecutor);
      registerProvider(importProvider);

      // Register query providers.
      List<Object> queryProviders = buildQueryProviders(executorConfig);
      registerProviders(queryProviders);

      // Register resource providers.
      List<Object> resourceProviders = buildResourceProviders();
      registerProviders(resourceProviders);

      // Configure interceptors.
      defineCorsConfiguration();
      configureRequestLogging();
      configureAuthorisation();
      registerInterceptor(new ResponseHighlighterInterceptor());

      // Configure paging.
      FifoMemoryPagingProvider pagingProvider = new FifoMemoryPagingProvider(SEARCH_MAP_SIZE);
      pagingProvider.setDefaultPageSize(DEFAULT_PAGE_SIZE);
      pagingProvider.setMaximumPageSize(MAX_PAGE_SIZE);
      setPagingProvider(pagingProvider);

      // Report errors to Sentry, if configured.
      registerInterceptor(
          new ErrorReportingInterceptor(getFhirContext(), configuration.getVersion()));

      // Initialise the capability statement.
      AnalyticsServerCapabilities serverCapabilities = new AnalyticsServerCapabilities(
          configuration, resourceReader);
      setServerConformanceProvider(serverCapabilities);

      logger.info("Ready for query");
    } catch (Exception e) {
      throw new ServletException("Error initializing AnalyticsServer", e);
    }
  }

  private static void printLogo(Logger logger) {
    logger.info("    ____        __  __    ___            ");
    logger.info("   / __ \\____ _/ /_/ /_  / (_)___  ____ _");
    logger.info("  / /_/ / __ `/ __/ __ \\/ / / __ \\/ __ `/");
    logger.info(" / ____/ /_/ / /_/ / / / / / / / / /_/ / ");
    logger.info("/_/    \\__,_/\\__/_/ /_/_/_/_/ /_/\\__, /  ");
    logger.info("                                /____/");
  }

  private static SparkSession buildSpark(AnalyticsServerConfiguration configuration) {
    SparkSession spark = SparkSession.builder()
        .appName("pathling-server")
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
    return spark;
  }

  private static TerminologyClient buildTerminologyClient(
      AnalyticsServerConfiguration configuration, FhirContext fhirContext, Logger logger) {
    return TerminologyClient.build(
        fhirContext,
        configuration.getTerminologyServerUrl(),
        configuration.getTerminologySocketTimeout(),
        configuration.isVerboseRequestLogging(),
        logger);
  }

  private static FhirEncoders buildFhirEncoders() {
    return FhirEncoders.forR4().getOrCreate();
  }

  private static ExecutorConfiguration buildExecutorConfiguration(
      AnalyticsServerConfiguration configuration, SparkSession spark, FhirContext fhirContext,
      FhirEncoders fhirEncoders, ResourceReader resourceReader,
      TerminologyClient terminologyClient) {
    TerminologyClientFactory terminologyClientFactory = new TerminologyClientFactory(
        FhirVersionEnum.R4, configuration.getTerminologyServerUrl(),
        configuration.getTerminologySocketTimeout(), configuration.isVerboseRequestLogging());

    ExecutorConfiguration executorConfig = new ExecutorConfiguration(spark,
        fhirContext, terminologyClientFactory, terminologyClient, resourceReader);
    executorConfig.setFhirEncoders(fhirEncoders);

    copyStringProps(configuration, executorConfig,
        Arrays.asList("version", "warehouseUrl", "databaseName", "executorMemory"));
    executorConfig.setExplainQueries(configuration.isExplainQueries());
    executorConfig.setShufflePartitions(configuration.getShufflePartitions());

    return executorConfig;
  }

  private static ResourceReader buildResourceReader(AnalyticsServerConfiguration configuration,
      SparkSession spark) throws IOException, URISyntaxException {
    return new ResourceReader(spark, configuration.getWarehouseUrl(),
        configuration.getDatabaseName());
  }

  private List<Object> buildQueryProviders(ExecutorConfiguration executorConfiguration) {
    AggregateExecutor aggregateExecutor = new AggregateExecutor(executorConfiguration);
    List<Object> providers = new ArrayList<>();
    providers.add(new AggregateProvider(aggregateExecutor));
    providers.addAll(buildSearchProviders(executorConfiguration));
    return providers;
  }

  private List<IResourceProvider> buildSearchProviders(
      ExecutorConfiguration executorConfiguration) {
    List<IResourceProvider> providers = new ArrayList<>();
    for (ResourceType resourceType : ResourceType.values()) {
      Class<? extends IBaseResource> resourceTypeClass = getFhirContext()
          .getResourceDefinition(resourceType.name()).getImplementingClass();
      //noinspection unchecked
      providers.add(new SearchProvider(executorConfiguration, resourceTypeClass));
    }
    return providers;
  }

  private List<Object> buildResourceProviders() {
    List<Object> providers = new ArrayList<>();
    providers.add(new OperationDefinitionProvider(getFhirContext()));
    return providers;
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

  private void configureRequestLogging() {
    // Add the request ID to the logging context before each request.
    RequestIdInterceptor requestIdInterceptor = new RequestIdInterceptor();
    registerInterceptor(requestIdInterceptor);

    // Log the request duration following each successful request.
    LoggingInterceptor loggingInterceptor = new LoggingInterceptor();
    loggingInterceptor.setLogger(logger);
    loggingInterceptor.setMessageFormat("Request completed in ${processingTimeMillis} ms");
    loggingInterceptor.setLogExceptions(false);
    registerInterceptor(loggingInterceptor);
  }

  private void configureAuthorisation() throws MalformedURLException {
    if (configuration.isAuthEnabled()) {
      if (configuration.getAuthJwksUrl() == null || configuration.getAuthIssuer() == null
          || configuration.getAuthAudience() == null) {
        throw new RuntimeException(
            "Must configure JWKS URL, issuer and audience if authorisation is enabled");
      }
      registerInterceptor(new AuthorisationInterceptor(configuration.getAuthJwksUrl(),
          configuration.getAuthIssuer(), configuration.getAuthAudience()));
      registerInterceptor(new SmartConfigurationInterceptor(configuration));
    }
  }

  @Override
  public void addHeadersToResponse(HttpServletResponse theHttpResponse) {
    // This removes the information-leaking `X-Powered-By` header from responses. We will need to
    // keep an eye on this to make sure that we don't disable and future functionality placed
    // within this method in the super.
  }
}
