package au.csiro.pathling;

import static au.csiro.pathling.utilities.Preconditions.checkPresent;

import au.csiro.pathling.async.JobProvider;
import au.csiro.pathling.cache.EntityTagInterceptor;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.encoders.EncoderBuilder;
import au.csiro.pathling.errors.ErrorHandlingInterceptor;
import au.csiro.pathling.errors.ErrorReportingInterceptor;
import au.csiro.pathling.fhir.ConformanceProvider;
import au.csiro.pathling.interceptors.BulkExportDeleteInterceptor;
import au.csiro.pathling.interceptors.SmartConfigurationInterceptor;
import au.csiro.pathling.operations.bulkexport.ExportResultProvider;
import au.csiro.pathling.operations.bulkexport.GroupExportProvider;
import au.csiro.pathling.operations.bulkexport.PatientExportProvider;
import au.csiro.pathling.operations.bulkexport.SystemExportProvider;
import au.csiro.pathling.operations.bulkimport.ImportPnpProvider;
import au.csiro.pathling.operations.bulkimport.ImportProvider;
import au.csiro.pathling.operations.bulksubmit.BulkSubmitProvider;
import au.csiro.pathling.operations.bulksubmit.BulkSubmitStatusProvider;
import au.csiro.pathling.operations.update.BatchProvider;
import au.csiro.pathling.operations.update.UpdateProviderFactory;
import au.csiro.pathling.operations.view.ViewDefinitionRunProvider;
import au.csiro.pathling.operations.viewexport.ViewDefinitionExportProvider;
import au.csiro.pathling.search.SearchProviderFactory;
import au.csiro.pathling.security.OidcConfiguration;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.api.EncodingEnum;
import ca.uhn.fhir.rest.server.ApacheProxyAddressStrategy;
import ca.uhn.fhir.rest.server.FifoMemoryPagingProvider;
import ca.uhn.fhir.rest.server.RestfulServer;
import ca.uhn.fhir.rest.server.interceptor.CorsInterceptor;
import ca.uhn.fhir.rest.server.interceptor.LoggingInterceptor;
import ca.uhn.fhir.rest.server.interceptor.ResponseHighlighterInterceptor;
import org.springframework.web.cors.CorsConfiguration;
import jakarta.annotation.Nonnull;
import jakarta.servlet.ServletException;
import jakarta.servlet.annotation.WebServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.Serial;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Enumerations;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.jdk.javaapi.CollectionConverters;

/**
 * @author Felix Naumann
 */
@WebServlet(urlPatterns = "/fhir/*")
@Slf4j
@SuppressWarnings("serial")
public class FhirServer extends RestfulServer {

  @Serial
  private static final long serialVersionUID = -1519567839063860047L;

  /**
   * The Accept header for FHIR JSON content.
   */
  public static final Header ACCEPT_HEADER = new Header("Accept", List.of("application/fhir+json"));

  /**
   * The Prefer header for respond-async behaviour.
   */
  public static final Header PREFER_RESPOND_TYPE_HEADER = new Header("Prefer",
      List.of("respond-async"));

  /**
   * The Prefer header for lenient handling behaviour.
   */
  public static final Header PREFER_LENIENT_HEADER = new Header("Prefer",
      List.of("handling=lenient"));

  /**
   * The _outputFormat parameter for specifying output format.
   */
  public static final Header OUTPUT_FORMAT = new Header("_outputFormat",
      List.of("application/fhir+ndjson", "application/ndjson", "ndjson"));

  private static final int DEFAULT_PAGE_SIZE = 100;
  private static final int MAX_PAGE_SIZE = Integer.MAX_VALUE;
  private static final int SEARCH_MAP_SIZE = 10;

  @Nonnull
  private final transient ServerConfiguration configuration;

  @Nonnull
  private final transient Optional<OidcConfiguration> oidcConfiguration;

  @Nonnull
  private final transient Optional<JobProvider> jobProvider;

  @Nonnull
  private final transient SystemExportProvider exportProvider;

  @Nonnull
  private final transient ExportResultProvider exportResultProvider;

  @Nonnull
  private final transient PatientExportProvider patientExportProvider;

  @Nonnull
  private final transient GroupExportProvider groupExportProvider;

  @Nonnull
  private final transient ImportProvider importProvider;

  @Nonnull
  private final transient ImportPnpProvider importPnpProvider;

  @Nonnull
  private final transient Optional<BulkSubmitProvider> bulkSubmitProvider;

  @Nonnull
  private final transient Optional<BulkSubmitStatusProvider> bulkSubmitStatusProvider;

  @Nonnull
  private final transient ErrorReportingInterceptor errorReportingInterceptor;

  @Nonnull
  private final transient EntityTagInterceptor entityTagInterceptor;

  @Nonnull
  private final transient BulkExportDeleteInterceptor bulkExportDeleteInterceptor;

  @Nonnull
  private final transient ConformanceProvider conformanceProvider;

  @Nonnull
  private final transient SearchProviderFactory searchProviderFactory;

  @Nonnull
  private final transient UpdateProviderFactory updateProviderFactory;

  @Nonnull
  private final transient BatchProvider batchProvider;

  @Nonnull
  private final transient ViewDefinitionRunProvider viewDefinitionRunProvider;

  @Nonnull
  private final transient ViewDefinitionExportProvider viewDefinitionExportProvider;

  /**
   * Constructs a new FhirServer.
   *
   * @param fhirContext the FHIR context
   * @param configuration the server configuration
   * @param oidcConfiguration the optional OIDC configuration
   * @param jobProvider the optional job provider
   * @param exportProvider the export provider
   * @param exportResultProvider the export result provider
   * @param patientExportProvider the patient export provider
   * @param groupExportProvider the group export provider
   * @param importProvider the import provider
   * @param importPnpProvider the import PnP provider
   * @param bulkSubmitProvider the optional bulk submit provider
   * @param bulkSubmitStatusProvider the optional bulk submit status provider
   * @param errorReportingInterceptor the error reporting interceptor
   * @param entityTagInterceptor the entity tag interceptor
   * @param bulkExportDeleteInterceptor the bulk export delete interceptor
   * @param conformanceProvider the conformance provider
   * @param searchProviderFactory the search provider factory
   * @param updateProviderFactory the update provider factory
   * @param batchProvider the batch provider
   * @param viewDefinitionRunProvider the view definition run provider
   * @param viewDefinitionExportProvider the view definition export provider
   */
  public FhirServer(@Nonnull final FhirContext fhirContext,
      @Nonnull final ServerConfiguration configuration,
      @Nonnull final Optional<OidcConfiguration> oidcConfiguration,
      @Nonnull final Optional<JobProvider> jobProvider,
      @Nonnull final SystemExportProvider exportProvider,
      @Nonnull final ExportResultProvider exportResultProvider,
      @Nonnull final PatientExportProvider patientExportProvider,
      @Nonnull final GroupExportProvider groupExportProvider,
      @Nonnull final ImportProvider importProvider,
      @Nonnull final ImportPnpProvider importPnpProvider,
      @Nonnull final Optional<BulkSubmitProvider> bulkSubmitProvider,
      @Nonnull final Optional<BulkSubmitStatusProvider> bulkSubmitStatusProvider,
      @Nonnull final ErrorReportingInterceptor errorReportingInterceptor,
      @Nonnull final EntityTagInterceptor entityTagInterceptor,
      @Nonnull final BulkExportDeleteInterceptor bulkExportDeleteInterceptor,
      @Nonnull final ConformanceProvider conformanceProvider,
      @Nonnull final SearchProviderFactory searchProviderFactory,
      @Nonnull final UpdateProviderFactory updateProviderFactory,
      @Nonnull final BatchProvider batchProvider,
      @Nonnull final ViewDefinitionRunProvider viewDefinitionRunProvider,
      @Nonnull final ViewDefinitionExportProvider viewDefinitionExportProvider) {
    // Pass the FhirContext to the RestfulServer superclass to ensure custom types like
    // ViewDefinitionResource are recognized when parsing request bodies.
    super(fhirContext);
    this.configuration = configuration;
    this.oidcConfiguration = oidcConfiguration;
    this.jobProvider = jobProvider;
    this.exportProvider = exportProvider;
    this.exportResultProvider = exportResultProvider;
    this.patientExportProvider = patientExportProvider;
    this.groupExportProvider = groupExportProvider;
    this.importProvider = importProvider;
    this.importPnpProvider = importPnpProvider;
    this.bulkSubmitProvider = bulkSubmitProvider;
    this.bulkSubmitStatusProvider = bulkSubmitStatusProvider;
    this.errorReportingInterceptor = errorReportingInterceptor;
    this.entityTagInterceptor = entityTagInterceptor;
    this.bulkExportDeleteInterceptor = bulkExportDeleteInterceptor;
    this.conformanceProvider = conformanceProvider;
    this.searchProviderFactory = searchProviderFactory;
    this.updateProviderFactory = updateProviderFactory;
    this.batchProvider = batchProvider;
    this.viewDefinitionRunProvider = viewDefinitionRunProvider;
    this.viewDefinitionExportProvider = viewDefinitionExportProvider;
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

      // Register job provider, if async is enabled.
      jobProvider.ifPresent(this::registerProvider);

      registerProvider(exportProvider);
      registerProvider(exportResultProvider);
      registerProvider(patientExportProvider);
      registerProvider(groupExportProvider);
      registerProvider(importProvider);
      registerProvider(importPnpProvider);

      // Register bulk submit providers.
      bulkSubmitProvider.ifPresent(this::registerProvider);
      bulkSubmitStatusProvider.ifPresent(this::registerProvider);

      // Register search providers for all supported resource types.
      for (final ResourceType resourceType : supportedResourceTypes()) {
        registerProvider(searchProviderFactory.createSearchProvider(resourceType));
      }

      // Register update providers for all supported resource types.
      for (final ResourceType resourceType : supportedResourceTypes()) {
        registerProvider(updateProviderFactory.createUpdateProvider(resourceType));
      }

      // Register batch provider.
      registerProvider(batchProvider);

      // Register view definition run provider.
      registerProvider(viewDefinitionRunProvider);

      // Register view definition export provider.
      registerProvider(viewDefinitionExportProvider);

      // CORS configuration.
      configureCors();

      // Authorization-related configuration.
      configureAuthorization();

      // Configure interceptors.
      configureRequestLogging();

      registerInterceptor(new ResponseHighlighterInterceptor());

      registerInterceptor(bulkExportDeleteInterceptor);

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

  @Override
  public void addHeadersToResponse(final HttpServletResponse theHttpResponse) {
    // This removes the information-leaking X-Powered-By header from responses.
  }

  @Override
  protected void service(final HttpServletRequest request, final HttpServletResponse response)
      throws ServletException, IOException {
    // Handle CORS preflight requests by returning 200 with appropriate headers.
    if ("OPTIONS".equalsIgnoreCase(request.getMethod())) {
      handleCorsPreflight(request, response);
      return;
    }
    super.service(request, response);
  }

  private void handleCorsPreflight(final HttpServletRequest request,
      final HttpServletResponse response) {
    final String origin = request.getHeader("Origin");
    if (origin != null) {
      // Check if origin is allowed.
      final List<String> allowedPatterns = configuration.getCors().getAllowedOriginPatterns();
      if (isOriginAllowed(origin, allowedPatterns)) {
        response.setHeader("Access-Control-Allow-Origin", origin);
        response.setHeader("Access-Control-Allow-Methods",
            String.join(", ", configuration.getCors().getAllowedMethods()));
        response.setHeader("Access-Control-Allow-Headers",
            String.join(", ", configuration.getCors().getAllowedHeaders()));
        response.setHeader("Access-Control-Expose-Headers",
            String.join(", ", configuration.getCors().getExposedHeaders()));
        response.setHeader("Access-Control-Max-Age",
            String.valueOf(configuration.getCors().getMaxAge()));
        if (configuration.getAuth().isEnabled()) {
          response.setHeader("Access-Control-Allow-Credentials", "true");
        }
        response.setHeader("Vary", "Origin, Access-Control-Request-Method, "
            + "Access-Control-Request-Headers");
      }
    }
    response.setStatus(HttpServletResponse.SC_OK);
  }

  private boolean isOriginAllowed(final String origin, final List<String> allowedPatterns) {
    if (allowedPatterns == null || allowedPatterns.isEmpty()) {
      return false;
    }
    for (final String pattern : allowedPatterns) {
      if ("*".equals(pattern) || origin.matches(convertPatternToRegex(pattern))) {
        return true;
      }
    }
    return false;
  }

  private String convertPatternToRegex(final String pattern) {
    // Convert simple wildcard patterns to regex.
    return pattern.replace(".", "\\.").replace("*", ".*");
  }

  private void configureAuthorization() {
    if (configuration.getAuth().isEnabled()) {
      final String issuer = checkPresent(configuration.getAuth().getIssuer());
      final SmartConfigurationInterceptor smartConfigurationInterceptor =
          new SmartConfigurationInterceptor(issuer, checkPresent(oidcConfiguration));
      registerInterceptor(smartConfigurationInterceptor);
    }
  }

  private void configureCors() {
    final CorsConfiguration corsConfig = new CorsConfiguration();
    corsConfig.setAllowedOriginPatterns(configuration.getCors().getAllowedOriginPatterns());
    corsConfig.setAllowedMethods(configuration.getCors().getAllowedMethods());
    corsConfig.setAllowedHeaders(configuration.getCors().getAllowedHeaders());
    corsConfig.setExposedHeaders(configuration.getCors().getExposedHeaders());
    corsConfig.setMaxAge(configuration.getCors().getMaxAge());
    corsConfig.setAllowCredentials(configuration.getAuth().isEnabled());

    final CorsInterceptor corsInterceptor = new CorsInterceptor(corsConfig);
    registerInterceptor(corsInterceptor);
    log.info("CORS interceptor registered with allowed origins: {}",
        configuration.getCors().getAllowedOriginPatterns());
  }


  private void configureRequestLogging() {

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

  /**
   * @return The set of resource types currently supported by this server.
   */
  @Nonnull
  public static Set<Enumerations.ResourceType> supportedResourceTypes() {
    final Set<Enumerations.ResourceType> availableResourceTypes = EnumSet.allOf(
        Enumerations.ResourceType.class);
    availableResourceTypes.removeAll(unsupportedResourceTypes());
    return availableResourceTypes;
  }

  /**
   * @return The set of resource types currently UN-supported by this server.
   */
  @Nonnull
  public static Set<ResourceType> unsupportedResourceTypes() {
    final Set<Enumerations.ResourceType> unsupportedResourceTypes =
        CollectionConverters.asJava(EncoderBuilder.UNSUPPORTED_RESOURCES()).stream()
            .map(Enumerations.ResourceType::fromCode)
            .collect(Collectors.toCollection(HashSet::new));
    unsupportedResourceTypes.add(ResourceType.NULL);
    unsupportedResourceTypes.add(ResourceType.DOMAINRESOURCE);
    unsupportedResourceTypes.add(ResourceType.RESOURCE);
    return unsupportedResourceTypes;
  }


}
