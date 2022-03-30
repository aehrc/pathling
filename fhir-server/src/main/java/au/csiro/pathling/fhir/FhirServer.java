/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhir;

import static au.csiro.pathling.utilities.Preconditions.checkPresent;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.async.JobProvider;
import au.csiro.pathling.caching.EntityTagInterceptor;
import au.csiro.pathling.encoders.EncoderBuilder;
import au.csiro.pathling.extract.ResultProvider;
import au.csiro.pathling.security.OidcConfiguration;
import au.csiro.pathling.update.BatchProvider;
import au.csiro.pathling.update.ImportProvider;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.api.EncodingEnum;
import ca.uhn.fhir.rest.server.ApacheProxyAddressStrategy;
import ca.uhn.fhir.rest.server.FifoMemoryPagingProvider;
import ca.uhn.fhir.rest.server.IResourceProvider;
import ca.uhn.fhir.rest.server.RestfulServer;
import ca.uhn.fhir.rest.server.interceptor.LoggingInterceptor;
import ca.uhn.fhir.rest.server.interceptor.ResponseHighlighterInterceptor;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations;
import org.hl7.fhir.r4.model.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import scala.collection.JavaConverters;

/**
 * A HAPI RestfulServer that provides the FHIR interface to the functionality within Pathling.
 *
 * @author John Grimes
 */
@WebServlet(urlPatterns = "/fhir/*")
@Profile("server")
@Slf4j
public class FhirServer extends RestfulServer {

  private static final long serialVersionUID = -1519567839063860047L;

  private static final int DEFAULT_PAGE_SIZE = 100;
  private static final int MAX_PAGE_SIZE = Integer.MAX_VALUE;
  private static final int SEARCH_MAP_SIZE = 10;

  @Nonnull
  private final Configuration configuration;

  @Nonnull
  private final Optional<OidcConfiguration> oidcConfiguration;

  @Nonnull
  private final ImportProvider importProvider;

  @Nonnull
  private final Optional<JobProvider> jobProvider;

  @Nonnull
  private final ResultProvider resultProvider;

  @Nonnull
  private final OperationDefinitionProvider operationDefinitionProvider;

  @Nonnull
  private final RequestIdInterceptor requestIdInterceptor;

  @Nonnull
  private final ErrorReportingInterceptor errorReportingInterceptor;

  @Nonnull
  private final EntityTagInterceptor entityTagInterceptor;

  @Nonnull
  private final ConformanceProvider conformanceProvider;

  @Nonnull
  private final ResourceProviderFactory resourceProviderFactory;

  @Nonnull
  private final BatchProvider batchProvider;

  /**
   * @param fhirContext a {@link FhirContext} for use in executing FHIR operations
   * @param configuration a {@link Configuration} instance which controls the behaviour of the
   * server
   * @param oidcConfiguration a {@link OidcConfiguration} object containing configuration retrieved
   * from OIDC discovery
   * @param importProvider a {@link ImportProvider} for receiving requests to the import operation
   * @param jobProvider a {@link JobProvider} for checking on the status of jobs
   * @param resultProvider {@link ResultProvider} for retrieving the result of extract requests
   * @param operationDefinitionProvider a {@link OperationDefinitionProvider} for receiving requests
   * for OperationDefinitions
   * @param requestIdInterceptor a {@link RequestIdInterceptor} for adding request IDs to logging
   * @param errorReportingInterceptor a {@link ErrorReportingInterceptor} for reporting errors to
   * Sentry
   * @param entityTagInterceptor a {@link EntityTagInterceptor} validating and returning ETags
   * @param conformanceProvider a {@link ConformanceProvider} for receiving requests for the server
   * CapabilityStatement
   * @param resourceProviderFactory a {@link ResourceProviderFactory} for providing instances of
   */
  public FhirServer(@Nonnull final FhirContext fhirContext,
      @Nonnull final Configuration configuration,
      @Nonnull final Optional<OidcConfiguration> oidcConfiguration,
      @Nonnull final ImportProvider importProvider,
      @Nonnull final Optional<JobProvider> jobProvider,
      @Nonnull final ResultProvider resultProvider,
      @Nonnull final OperationDefinitionProvider operationDefinitionProvider,
      @Nonnull final RequestIdInterceptor requestIdInterceptor,
      @Nonnull final ErrorReportingInterceptor errorReportingInterceptor,
      @Nonnull final EntityTagInterceptor entityTagInterceptor,
      @Nonnull final ConformanceProvider conformanceProvider,
      @Nonnull final ResourceProviderFactory resourceProviderFactory,
      @Nonnull final BatchProvider batchProvider) {
    super(fhirContext);
    this.configuration = configuration;
    this.oidcConfiguration = oidcConfiguration;
    this.importProvider = importProvider;
    this.jobProvider = jobProvider;
    this.resultProvider = resultProvider;
    this.operationDefinitionProvider = operationDefinitionProvider;
    this.requestIdInterceptor = requestIdInterceptor;
    this.errorReportingInterceptor = errorReportingInterceptor;
    this.entityTagInterceptor = entityTagInterceptor;
    this.conformanceProvider = conformanceProvider;
    this.resourceProviderFactory = resourceProviderFactory;
    this.batchProvider = batchProvider;
    log.debug("Starting FHIR server with configuration: {}", configuration);
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
      addressStrategy.setServletPath("/fhir");
      setServerAddressStrategy(addressStrategy);

      // Register the import provider.
      registerProvider(importProvider);

      // Register query providers.
      final Collection<Object> providers = new ArrayList<>();
      providers.addAll(buildAggregateProviders());
      providers.addAll(buildExtractProviders());
      providers.addAll(buildSearchProviders());
      providers.addAll(buildUpdateProviders());
      registerProviders(providers);

      // Register batch provider.
      registerProvider(batchProvider);

      // Register resource providers.
      registerProvider(operationDefinitionProvider);

      // Register job provider, if async is enabled.
      jobProvider.ifPresent(this::registerProvider);

      // Register extract result provider.
      registerProvider(resultProvider);

      // Configure interceptors.
      configureRequestLogging();

      // Authorization-related configuration.
      configureAuthorization();

      registerInterceptor(new ResponseHighlighterInterceptor());

      // Configure paging.
      final FifoMemoryPagingProvider pagingProvider = new FifoMemoryPagingProvider(SEARCH_MAP_SIZE);
      pagingProvider.setDefaultPageSize(DEFAULT_PAGE_SIZE);
      pagingProvider.setMaximumPageSize(MAX_PAGE_SIZE);
      setPagingProvider(pagingProvider);

      // Register error handling interceptor.
      registerInterceptor(new ErrorHandlingInterceptor());

      // Register ETag handling interceptor.
      registerInterceptor(entityTagInterceptor);

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
  private List<IResourceProvider> buildAggregateProviders() {
    final List<IResourceProvider> providers = new ArrayList<>();

    // Instantiate an aggregate provider for every resource type in FHIR.
    for (final ResourceType resourceType : ResourceType.values()) {
      final IResourceProvider aggregateProvider = resourceProviderFactory
          .createAggregateResourceProvider(resourceType);
      providers.add(aggregateProvider);
    }
    return providers;
  }

  @Nonnull
  private List<IResourceProvider> buildExtractProviders() {
    final List<IResourceProvider> providers = new ArrayList<>();

    // Instantiate an extract provider for every resource type in FHIR.
    for (final ResourceType resourceType : ResourceType.values()) {
      final IResourceProvider extractProvider = resourceProviderFactory
          .createExtractResourceProvider(resourceType);
      providers.add(extractProvider);
    }
    return providers;
  }

  @Nonnull
  private List<IResourceProvider> buildSearchProviders() {
    final List<IResourceProvider> providers = new ArrayList<>();

    // Instantiate a search provider for every resource type in FHIR.
    for (final ResourceType resourceType : ResourceType.values()) {
      final IResourceProvider searchProvider =
          resourceProviderFactory.createSearchResourceProvider(resourceType);
      providers.add(searchProvider);
    }
    return providers;
  }

  @Nonnull
  private List<IResourceProvider> buildUpdateProviders() {
    final List<IResourceProvider> providers = new ArrayList<>();

    for (final ResourceType resourceType : ResourceType.values()) {
      final IResourceProvider updateProvider =
          resourceProviderFactory.createUpdateResourceProvider(resourceType);
      providers.add(updateProvider);
    }
    return providers;
  }

  private void configureRequestLogging() {
    // Add the request ID to the logging context before each request.
    registerInterceptor(requestIdInterceptor);

    // Create a dedicated logger, so that we can control it independently through logging
    // configuration.
    final Logger requestLogger = LoggerFactory.getLogger("requestLogger");

    // Log the request duration following each successful request.
    final LoggingInterceptor loggingInterceptor = new LoggingInterceptor();
    loggingInterceptor.setLogger(requestLogger);
    loggingInterceptor.setMessageFormat("Request completed in ${processingTimeMillis} ms");
    loggingInterceptor.setLogExceptions(false);
    registerInterceptor(loggingInterceptor);
  }

  private void configureAuthorization() {
    if (configuration.getAuth().isEnabled()) {
      final String issuer = checkPresent(configuration.getAuth().getIssuer());
      final SmartConfigurationInterceptor smartConfigurationInterceptor =
          new SmartConfigurationInterceptor(issuer, checkPresent(oidcConfiguration));
      registerInterceptor(smartConfigurationInterceptor);
    }
  }

  @Override
  public void addHeadersToResponse(final HttpServletResponse theHttpResponse) {
    // This removes the information-leaking `X-Powered-By` header from responses. We will need to
    // keep an eye on this to make sure that we don't disable any future functionality placed
    // within this method in the super.
  }

  /**
   * @param resourceClass a class that extends {@link IBaseResource}
   * @return a {@link Enumerations.ResourceType} enum
   */
  @Nonnull
  public static Enumerations.ResourceType resourceTypeFromClass(
      @Nonnull final Class<? extends IBaseResource> resourceClass) {
    try {
      final Constructor<? extends IBaseResource> constructor = resourceClass.getConstructor();
      final IBaseResource instance = constructor.newInstance();
      return Enumerations.ResourceType.fromCode(instance.fhirType());
    } catch (final NoSuchMethodException | IllegalAccessException | InstantiationException
        | InvocationTargetException e) {
      throw new RuntimeException("Problem determining FHIR type from resource class", e);
    }
  }

  /**
   * @return The set of resource types currently supported by this server.
   */
  @Nonnull
  public static Set<Enumerations.ResourceType> supportedResourceTypes() {
    final Set<Enumerations.ResourceType> availableResourceTypes = EnumSet.allOf(
        Enumerations.ResourceType.class);
    final Set<Enumerations.ResourceType> unsupportedResourceTypes =
        JavaConverters.setAsJavaSet(EncoderBuilder.UNSUPPORTED_RESOURCES()).stream()
            .map(Enumerations.ResourceType::fromCode)
            .collect(Collectors.toSet());
    availableResourceTypes.removeAll(unsupportedResourceTypes);
    availableResourceTypes.remove(Enumerations.ResourceType.RESOURCE);
    availableResourceTypes.remove(Enumerations.ResourceType.DOMAINRESOURCE);
    availableResourceTypes.remove(Enumerations.ResourceType.NULL);
    return availableResourceTypes;
  }

}

