package au.csiro.pathling.terminology;

import static au.csiro.pathling.utilities.Preconditions.checkNotNull;

import au.csiro.pathling.config.TerminologyAuthConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhir.ClientAuthInterceptor;
import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.fhir.UserAgentInterceptor;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.api.IRestfulClientFactory;
import ca.uhn.fhir.rest.client.api.ServerValidationModeEnum;
import ca.uhn.fhir.rest.client.interceptor.LoggingInterceptor;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.cache.CacheConfig;
import org.apache.http.impl.client.cache.CachingHttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.slf4j.Logger;

public class CacheableTerminologyServiceFactory implements TerminologyServiceFactory {

  private static final long serialVersionUID = 2837933007972812597L;

  @Nullable
  private static TerminologyService terminologyService = null;

  @Nonnull
  private final FhirVersionEnum fhirVersion;

  @Nonnull
  private final String terminologyServerUrl;

  private final int socketTimeout;

  private final boolean verboseRequestLogging;

  @Nonnull
  private final TerminologyAuthConfiguration authConfig;

  public CacheableTerminologyServiceFactory(@Nonnull final FhirVersionEnum fhirVersion,
      @Nonnull final String terminologyServerUrl, final int socketTimeout,
      final boolean verboseRequestLogging, @Nonnull final TerminologyAuthConfiguration authConfig) {
    this.fhirVersion = fhirVersion;
    this.terminologyServerUrl = terminologyServerUrl;
    this.socketTimeout = socketTimeout;
    this.verboseRequestLogging = verboseRequestLogging;
    this.authConfig = authConfig;
  }

  @Nonnull
  @Override
  public TerminologyService buildService(@Nonnull final Logger logger) {
    if (terminologyService == null) {
      final FhirContext fhirContext = FhirEncoders.contextFor(fhirVersion);
      final IRestfulClientFactory restfulClientFactory = fhirContext.getRestfulClientFactory();
      final CloseableHttpClient httpClient = buildHttpClient();
      restfulClientFactory.setSocketTimeout(socketTimeout);
      restfulClientFactory.setServerValidationMode(ServerValidationModeEnum.NEVER);
      restfulClientFactory.setHttpClient(httpClient);

      final IGenericClient fhirClient = restfulClientFactory.newGenericClient(terminologyServerUrl);
      fhirClient.registerInterceptor(new UserAgentInterceptor());

      final LoggingInterceptor loggingInterceptor = new LoggingInterceptor();
      loggingInterceptor.setLogger(logger);
      loggingInterceptor.setLogRequestSummary(true);
      loggingInterceptor.setLogResponseSummary(true);
      loggingInterceptor.setLogRequestHeaders(true);
      loggingInterceptor.setLogResponseHeaders(true);
      if (verboseRequestLogging) {
        loggingInterceptor.setLogRequestBody(true);
        loggingInterceptor.setLogResponseBody(true);
      }
      fhirClient.registerInterceptor(loggingInterceptor);

      if (authConfig.isEnabled()) {
        checkNotNull(authConfig.getTokenEndpoint());
        checkNotNull(authConfig.getClientId());
        checkNotNull(authConfig.getClientSecret());
        final ClientAuthInterceptor clientAuthInterceptor = new ClientAuthInterceptor(
            authConfig.getTokenEndpoint(), authConfig.getClientId(), authConfig.getClientSecret(),
            authConfig.getScope(), authConfig.getTokenExpiryTolerance());
        fhirClient.registerInterceptor(clientAuthInterceptor);
      }

      terminologyService = new CacheableTerminologyService(fhirClient);
    }
    return terminologyService;

  }

  private static CloseableHttpClient buildHttpClient() {
    final CacheConfig cacheConfig = CacheConfig.custom()
        .setMaxCacheEntries(500_000)
        .build();
    final PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
    connectionManager.setMaxTotal(8);
    connectionManager.setDefaultMaxPerRoute(8);
    return CachingHttpClients.custom()
        .setCacheConfig(cacheConfig)
        .setConnectionManager(connectionManager)
        .setRetryHandler(new DefaultHttpRequestRetryHandler(1, true))
        .build();
  }

}
