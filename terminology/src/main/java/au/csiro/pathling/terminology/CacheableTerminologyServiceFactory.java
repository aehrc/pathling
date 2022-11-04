package au.csiro.pathling.terminology;

import static au.csiro.pathling.utilities.Preconditions.checkNotNull;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.config.TerminologyAuthConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhir.ClientAuthInterceptor;
import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.fhir.UserAgentInterceptor;
import au.csiro.pathling.sql.udf.ValidateCoding;
import au.csiro.pathling.utilities.Preconditions;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.api.IRestfulClientFactory;
import ca.uhn.fhir.rest.client.api.ServerValidationModeEnum;
import ca.uhn.fhir.rest.client.interceptor.LoggingInterceptor;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.utils.CloseableUtils;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.cache.CacheConfig;
import org.apache.http.impl.client.cache.CachingHttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;


@Slf4j
@EqualsAndHashCode
public class CacheableTerminologyServiceFactory implements TerminologyServiceFactory {

  private static final long serialVersionUID = 2837933007972812597L;

  @Nullable
  private static CacheableTerminologyService terminologyService = null;

  @Nullable
  private static CacheableTerminologyServiceFactory configuredFactory = null;

  @Nonnull
  private final FhirVersionEnum fhirVersion;

  @Nonnull
  private final String terminologyServerUrl;

  private final int socketTimeout;

  private final boolean verboseRequestLogging;

  @Nonnull
  private final TerminologyAuthConfiguration authConfig;

  private synchronized CacheableTerminologyService getOrCreateService(
      @Nonnull final CacheableTerminologyServiceFactory configFactory,
      @Nonnull final Logger logger) {
    if (terminologyService == null) {
      terminologyService = configFactory.createService(logger);
      configuredFactory = configFactory;
    } else {
      Preconditions.check(configFactory.equals(configuredFactory),
          "Attempt to create CachableTerminologyService with differed configuration");
    }
    return terminologyService;
  }

  public static synchronized void invalidate() {
    if (terminologyService != null) {
      log.info("Invalidating HTTPClient cache");
      CloseableUtils.closeQuietly(terminologyService);
      terminologyService = null;
      configuredFactory = null;
    }
  }

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
  public Result memberOf(@Nonnull final Dataset<Row> dataset, @Nonnull final Column value,
      final String valueSetUri) {
    final Column resultColumn = callUDF(ValidateCoding.FUNCTION_NAME,
        lit(valueSetUri),
        value);
    return new Result(dataset.repartition(8)
        , resultColumn);
  }
  
  @Nonnull
  @Override
  public TerminologyService buildService(@Nonnull final Logger logger) {
    return getOrCreateService(this, logger);
  }

  @Nonnull
  private CacheableTerminologyService createService(@Nonnull final Logger logger) {
    final FhirContext fhirContext = FhirEncoders.contextFor(fhirVersion);
    final IRestfulClientFactory restfulClientFactory = fhirContext.getRestfulClientFactory();
    final CloseableHttpClient httpClient = buildHttpClient();
    restfulClientFactory.setSocketTimeout(socketTimeout);
    restfulClientFactory.setServerValidationMode(ServerValidationModeEnum.NEVER);
    restfulClientFactory.setHttpClient(httpClient);

    final IGenericClient fhirClient = restfulClientFactory.newGenericClient(
        terminologyServerUrl);
    fhirClient.registerInterceptor(new UserAgentInterceptor());

    final LoggingInterceptor loggingInterceptor = new LoggingInterceptor();
    loggingInterceptor.setLogger(logger);
    loggingInterceptor.setLogRequestSummary(true);
    loggingInterceptor.setLogResponseSummary(true);
    loggingInterceptor.setLogRequestHeaders(false);
    loggingInterceptor.setLogResponseHeaders(false);
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
    return new CacheableTerminologyService(fhirClient, httpClient);
  }

  private static CloseableHttpClient buildHttpClient() {
    final CacheConfig cacheConfig = CacheConfig.custom()
        .setMaxCacheEntries(500_000)
        .build();
    final PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
    connectionManager.setMaxTotal(32);
    connectionManager.setDefaultMaxPerRoute(16);
    return CachingHttpClients.custom()
        .setCacheConfig(cacheConfig)
        .setConnectionManager(connectionManager)
        .setRetryHandler(new DefaultHttpRequestRetryHandler(1, true))
        .build();
  }

}
