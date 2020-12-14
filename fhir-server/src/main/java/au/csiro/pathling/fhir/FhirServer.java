/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhir;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.Configuration.Authorisation;
import au.csiro.pathling.aggregate.AggregateProvider;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.search.CachingSearchProvider;
import au.csiro.pathling.search.SearchExecutorCache;
import au.csiro.pathling.search.SearchProvider;
import au.csiro.pathling.update.ImportProvider;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.api.EncodingEnum;
import ca.uhn.fhir.rest.server.ApacheProxyAddressStrategy;
import ca.uhn.fhir.rest.server.FifoMemoryPagingProvider;
import ca.uhn.fhir.rest.server.IResourceProvider;
import ca.uhn.fhir.rest.server.RestfulServer;
import ca.uhn.fhir.rest.server.interceptor.CorsInterceptor;
import ca.uhn.fhir.rest.server.interceptor.LoggingInterceptor;
import ca.uhn.fhir.rest.server.interceptor.ResponseHighlighterInterceptor;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.ResourceType;
import org.springframework.web.cors.CorsConfiguration;

/**
 * A HAPI RestfulServer that provides the FHIR interface to the functionality within Pathling.
 *
 * @author John Grimes
 */
@WebServlet(urlPatterns = "/fhir/*")
@Slf4j
@SuppressWarnings({"NonSerializableFieldInSerializableClass", "serial"})
public class FhirServer extends RestfulServer {

  private static final int DEFAULT_PAGE_SIZE = 100;
  private static final int MAX_PAGE_SIZE = Integer.MAX_VALUE;
  private static final int SEARCH_MAP_SIZE = 10;

  @Nonnull
  private final Configuration configuration;

  @Nonnull
  private final SparkSession sparkSession;

  @Nonnull
  private final FhirEncoders fhirEncoders;

  @Nonnull
  private final ResourceReader resourceReader;

  @Nonnull
  private final Optional<TerminologyClient> terminologyClient;

  @Nonnull
  private final Optional<TerminologyClientFactory> terminologyClientFactory;

  @Nonnull
  private final AggregateProvider aggregateProvider;

  @Nonnull
  private final ImportProvider importProvider;

  @Nonnull
  private final OperationDefinitionProvider operationDefinitionProvider;

  @Nonnull
  private final RequestIdInterceptor requestIdInterceptor;

  @Nonnull
  private final ErrorReportingInterceptor errorReportingInterceptor;

  @Nonnull
  private final ConformanceProvider conformanceProvider;

  @Nonnull
  private final SearchExecutorCache searchExecutorCache;

  /**
   * @param fhirContext A {@link FhirContext} for use in executing FHIR operations
   * @param configuration A {@link Configuration} instance which controls the behaviour of the
   * server
   * @param sparkSession A {@link SparkSession} for use in querying FHIR data using Spark
   * @param fhirEncoders A {@link FhirEncoders} for use in serializing and deserializing FHIR data
   * @param resourceReader A {@link ResourceReader} for retrieving FHIR data from storage
   * @param terminologyClient A {@link TerminologyClient} for resolving FHIR terminology queries
   * @param terminologyClientFactory A {@link TerminologyClientFactory} for resolving FHIR
   * terminology queries during parallel processing
   * @param aggregateProvider A {@link AggregateProvider} for receiving requests to the aggregate
   * operation
   * @param importProvider A {@link ImportProvider} for receiving requests to the import operation
   * @param operationDefinitionProvider A {@link OperationDefinitionProvider} for receiving requests
   * for OperationDefinitions
   * @param requestIdInterceptor A {@link RequestIdInterceptor} for adding request IDs to logging
   * @param errorReportingInterceptor A {@link ErrorReportingInterceptor} for reporting errors to
   * Sentry
   * @param conformanceProvider A {@link ConformanceProvider} for receiving requests for the server
   * CapabilityStatement
   * @param searchExecutorCache A {@link SearchExecutorCache} for caching search requests
   */
  public FhirServer(@Nonnull final FhirContext fhirContext,
      @Nonnull final Configuration configuration,
      @Nonnull final SparkSession sparkSession,
      @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final ResourceReader resourceReader,
      @Nonnull final Optional<TerminologyClient> terminologyClient,
      @Nonnull final Optional<TerminologyClientFactory> terminologyClientFactory,
      @Nonnull final AggregateProvider aggregateProvider,
      @Nonnull final ImportProvider importProvider,
      @Nonnull final OperationDefinitionProvider operationDefinitionProvider,
      @Nonnull final RequestIdInterceptor requestIdInterceptor,
      @Nonnull final ErrorReportingInterceptor errorReportingInterceptor,
      @Nonnull final ConformanceProvider conformanceProvider,
      @Nonnull final SearchExecutorCache searchExecutorCache) {
    super(fhirContext);
    this.configuration = configuration;
    this.sparkSession = sparkSession;
    this.fhirEncoders = fhirEncoders;
    this.resourceReader = resourceReader;
    this.terminologyClient = terminologyClient;
    this.terminologyClientFactory = terminologyClientFactory;
    this.aggregateProvider = aggregateProvider;
    this.importProvider = importProvider;
    this.operationDefinitionProvider = operationDefinitionProvider;
    this.requestIdInterceptor = requestIdInterceptor;
    this.errorReportingInterceptor = errorReportingInterceptor;
    this.conformanceProvider = conformanceProvider;
    this.searchExecutorCache = searchExecutorCache;
    log.info("Starting FHIR server with configuration: {}", configuration);
  }

  @Override
  protected void initialize() throws ServletException {
    super.initialize();

    try {
      // Set default response encoding to JSON.
      setDefaultResponseEncoding(EncodingEnum.JSON);

      // Use a proxy address strategy, which allows proxies to control the server base address with
      // the use of the X-Forwarded-Host and X-Forwarded-Proto headers.
      final ApacheProxyAddressStrategy addressStrategy = ApacheProxyAddressStrategy.forHttp();
      if (!configuration.getHttpBase().isEmpty()) {
        addressStrategy.setServletPath(configuration.getHttpBase() + "/fhir");
      }
      setServerAddressStrategy(addressStrategy);

      // Register the import provider.
      registerProvider(importProvider);

      // Register query providers.
      final Collection<Object> providers = new ArrayList<>();
      providers.add(aggregateProvider);
      providers.addAll(buildSearchProviders());
      registerProviders(providers);

      // Register resource providers.
      registerProvider(operationDefinitionProvider);

      // Configure interceptors.
      defineCorsConfiguration();
      configureRequestLogging();
      configureAuthorisation();
      registerInterceptor(new ResponseHighlighterInterceptor());

      // Configure paging.
      final FifoMemoryPagingProvider pagingProvider = new FifoMemoryPagingProvider(SEARCH_MAP_SIZE);
      pagingProvider.setDefaultPageSize(DEFAULT_PAGE_SIZE);
      pagingProvider.setMaximumPageSize(MAX_PAGE_SIZE);
      setPagingProvider(pagingProvider);

      // Register error handling interceptor
      registerInterceptor(new ErrorHandlingInterceptor());

      // Report errors to Sentry, if configured.
      registerInterceptor(errorReportingInterceptor);

      // Initialise the capability statement.
      setServerConformanceProvider(conformanceProvider);

      log.info("FHIR server initialized");
    } catch (final Exception e) {
      throw new ServletException("Error initializing AnalyticsServer", e);
    }
  }

  @Nonnull
  private List<IResourceProvider> buildSearchProviders() {
    final List<IResourceProvider> providers = new ArrayList<>();

    // Instantiate a search provider for every resource type in FHIR.
    for (final ResourceType resourceType : ResourceType.values()) {
      final Class<? extends IBaseResource> resourceTypeClass = getFhirContext()
          .getResourceDefinition(resourceType.name()).getImplementingClass();

      final IResourceProvider searchProvider =
          configuration.getCaching().isEnabled()
          ? new CachingSearchProvider(configuration, getFhirContext(), sparkSession, resourceReader,
              terminologyClient, terminologyClientFactory, fhirEncoders, resourceTypeClass,
              searchExecutorCache)
          : new SearchProvider(configuration, getFhirContext(), sparkSession, resourceReader,
              terminologyClient, terminologyClientFactory, fhirEncoders, resourceTypeClass);

      providers.add(searchProvider);
    }
    return providers;
  }

  /**
   * Declare a CORS interceptor, using the CorsConfiguration from Spring. This is required to enable
   * web-based applications hosted on different domains to communicate with this server.
   */
  private void defineCorsConfiguration() {
    final CorsConfiguration corsConfig = new CorsConfiguration();

    corsConfig.setAllowedOrigins(configuration.getCors().getAllowedOrigins());
    corsConfig.setAllowedMethods(configuration.getCors().getAllowedMethods());
    corsConfig.setAllowedHeaders(configuration.getCors().getAllowedHeaders());
    corsConfig.setMaxAge(configuration.getCors().getMaxAge());
    if (configuration.getCors().getExposeHeaders().isPresent()) {
      corsConfig.setExposedHeaders(configuration.getCors().getExposeHeaders().get());
    }

    final CorsInterceptor interceptor = new CorsInterceptor(corsConfig);
    registerInterceptor(interceptor);
  }

  private void configureRequestLogging() {
    // Add the request ID to the logging context before each request.
    registerInterceptor(requestIdInterceptor);

    // Log the request duration following each successful request.
    final LoggingInterceptor loggingInterceptor = new LoggingInterceptor();
    loggingInterceptor.setLogger(log);
    loggingInterceptor.setMessageFormat("Request completed in ${processingTimeMillis} ms");
    loggingInterceptor.setLogExceptions(false);
    registerInterceptor(loggingInterceptor);
  }

  private void configureAuthorisation() throws MalformedURLException {
    if (configuration.getAuth().isEnabled()) {
      final Authorisation authorisationConfig = this.configuration.getAuth();

      final AuthorisationInterceptor authorisationInterceptor =
          new AuthorisationInterceptor(authorisationConfig);
      registerInterceptor(authorisationInterceptor);

      final SmartConfigurationInterceptor smartConfigurationInterceptor =
          new SmartConfigurationInterceptor(authorisationConfig);
      registerInterceptor(smartConfigurationInterceptor);
    }
  }

  @Override
  public void addHeadersToResponse(final HttpServletResponse theHttpResponse) {
    // This removes the information-leaking `X-Powered-By` header from responses. We will need to
    // keep an eye on this to make sure that we don't disable any future functionality placed
    // within this method in the super.
  }

}
