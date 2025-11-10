package au.csiro.pathling;

import static au.csiro.pathling.utilities.Preconditions.checkPresent;

import au.csiro.pathling.async.JobProvider;
import au.csiro.pathling.cache.EntityTagInterceptor;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.encoders.EncoderBuilder;
import au.csiro.pathling.errors.ErrorHandlingInterceptor;
import au.csiro.pathling.errors.ErrorReportingInterceptor;
import au.csiro.pathling.operations.bulkexport.ExportProvider;
import au.csiro.pathling.operations.bulkexport.ExportResultProvider;
import au.csiro.pathling.fhir.ConformanceProvider;
import au.csiro.pathling.interceptors.BulkExportDeleteInterceptor;
import au.csiro.pathling.interceptors.SmartConfigurationInterceptor;
import au.csiro.pathling.security.OidcConfiguration;
import ca.uhn.fhir.rest.api.EncodingEnum;
import ca.uhn.fhir.rest.server.ApacheProxyAddressStrategy;
import ca.uhn.fhir.rest.server.FifoMemoryPagingProvider;
import ca.uhn.fhir.rest.server.RestfulServer;
import ca.uhn.fhir.rest.server.interceptor.LoggingInterceptor;
import ca.uhn.fhir.rest.server.interceptor.ResponseHighlighterInterceptor;
import jakarta.annotation.Nonnull;
import jakarta.servlet.ServletException;
import jakarta.servlet.annotation.WebServlet;
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
public class FhirServer extends RestfulServer {

  @Serial
  private static final long serialVersionUID = -1519567839063860047L;

  public static final Header ACCEPT_HEADER = new Header("Accept", List.of("application/fhir+json"));
  public static final Header PREFER_RESPOND_TYPE_HEADER = new Header("Prefer",
      List.of("respond-async"));
  public static final Header PREFER_LENIENT_HEADER = new Header("Prefer",
      List.of("handling=lenient"));
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
  private final transient ExportProvider exportProvider;
  
  @Nonnull
  private final transient ExportResultProvider exportResultProvider;

  @Nonnull
  private final transient ErrorReportingInterceptor errorReportingInterceptor;

  @Nonnull
  private final transient EntityTagInterceptor entityTagInterceptor;

  @Nonnull
  private final transient BulkExportDeleteInterceptor bulkExportDeleteInterceptor;

  @Nonnull
  private final transient ConformanceProvider conformanceProvider;


  public FhirServer(@Nonnull final ServerConfiguration configuration,
      @Nonnull final Optional<OidcConfiguration> oidcConfiguration, @Nonnull Optional<JobProvider> jobProvider,
      @Nonnull ExportProvider exportProvider,
      @Nonnull ExportResultProvider exportResultProvider,
      @Nonnull ErrorReportingInterceptor errorReportingInterceptor,
      @Nonnull EntityTagInterceptor entityTagInterceptor, @Nonnull
      BulkExportDeleteInterceptor bulkExportDeleteInterceptor,
      @Nonnull ConformanceProvider conformanceProvider) {
    this.configuration = configuration;
    this.oidcConfiguration = oidcConfiguration;
    this.jobProvider = jobProvider;
    this.exportProvider = exportProvider;
    this.exportResultProvider = exportResultProvider;
    this.errorReportingInterceptor = errorReportingInterceptor;
    this.entityTagInterceptor = entityTagInterceptor;
    this.bulkExportDeleteInterceptor = bulkExportDeleteInterceptor;
    this.conformanceProvider = conformanceProvider;
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

  private void configureAuthorization() {
    if (configuration.getAuth().isEnabled()) {
      final String issuer = checkPresent(configuration.getAuth().getIssuer());
      final SmartConfigurationInterceptor smartConfigurationInterceptor =
          new SmartConfigurationInterceptor(issuer, checkPresent(oidcConfiguration));
      registerInterceptor(smartConfigurationInterceptor);
    }
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
    return unsupportedResourceTypes;
  }


}
