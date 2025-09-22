package au.csiro.pathling;

import au.csiro.pathling.async.JobProvider;
import au.csiro.pathling.cache.EntityTagInterceptor;
import au.csiro.pathling.encoders.EncoderBuilder;
import au.csiro.pathling.errors.ErrorHandlingInterceptor;
import au.csiro.pathling.errors.ErrorReportingInterceptor;
import au.csiro.pathling.export.ExportProvider;
import au.csiro.pathling.fhir.ConformanceProvider;
import au.csiro.pathling.interceptors.BulkExportDeleteInterceptor;
import ca.uhn.fhir.rest.api.EncodingEnum;
import ca.uhn.fhir.rest.server.ApacheProxyAddressStrategy;
import ca.uhn.fhir.rest.server.FifoMemoryPagingProvider;
import ca.uhn.fhir.rest.server.RestfulServer;
import ca.uhn.fhir.rest.server.interceptor.CorsInterceptor;
import ca.uhn.fhir.rest.server.interceptor.LoggingInterceptor;
import ca.uhn.fhir.rest.server.interceptor.ResponseHighlighterInterceptor;
import jakarta.annotation.Nonnull;
import jakarta.servlet.ServletException;
import jakarta.servlet.annotation.WebServlet;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Enumerations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.cors.CorsConfiguration;
import scala.collection.JavaConverters;

import java.net.InetAddress;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Felix Naumann
 */
@WebServlet(urlPatterns = "/fhir/*")
@Profile("server")
@Slf4j
public class FhirServer extends RestfulServer {
  
    public static final Header ACCEPT_HEADER = new Header("Accept", List.of("application/fhir+json"));
    public static final Header PREFER_RESPOND_TYPE_HEADER = new Header("Prefer", List.of("respond-async"));
    public static final Header PREFER_LENIENT_HEADER = new Header("Prefer", List.of("handling=lenient"));
    public static final Header OUTPUT_FORMAT = new Header("_outputFormat", List.of("application/fhir+ndjson", "application/ndjson", "ndjson"));

    private static final int DEFAULT_PAGE_SIZE = 100;
    private static final int MAX_PAGE_SIZE = Integer.MAX_VALUE;
    private static final int SEARCH_MAP_SIZE = 10;

    @Nonnull
    private final Optional<JobProvider> jobProvider;

    @Nonnull
    private final ExportProvider exportProvider;

    @Nonnull
    private final ErrorReportingInterceptor errorReportingInterceptor;

    @Nonnull
    private final EntityTagInterceptor entityTagInterceptor;
    
    @Nonnull
    private final BulkExportDeleteInterceptor bulkExportDeleteInterceptor;
    
  @Nonnull
    private final ConformanceProvider conformanceProvider;


  public FhirServer(@Nonnull Optional<JobProvider> jobProvider, @Nonnull ExportProvider exportProvider, @Nonnull ErrorReportingInterceptor errorReportingInterceptor, @Nonnull EntityTagInterceptor entityTagInterceptor, @Nonnull
    BulkExportDeleteInterceptor bulkExportDeleteInterceptor, @Nonnull ConformanceProvider conformanceProvider) {
        this.jobProvider = jobProvider;
        this.exportProvider = exportProvider;
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
       final Set<Enumerations.ResourceType> unsupportedResourceTypes =
               JavaConverters.setAsJavaSet(EncoderBuilder.UNSUPPORTED_RESOURCES()).stream()
                       .map(Enumerations.ResourceType::fromCode)
                       .collect(Collectors.toSet());
       availableResourceTypes.removeAll(unsupportedResourceTypes);
        return availableResourceTypes;
    }


}
