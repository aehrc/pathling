package au.csiro.pathling.terminology;

import static org.apache.spark.sql.functions.callUDF;

import au.csiro.pathling.caching.CachingFactories;
import au.csiro.pathling.config.HttpCacheConfiguration;
import au.csiro.pathling.config.HttpClientConfiguration;
import au.csiro.pathling.config.TerminologyAuthConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClient2;
import au.csiro.pathling.terminology.ObjectHolder.SingletonHolder;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import javax.annotation.Nonnull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.cache.CacheConfig;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import java.util.UUID;


/**
 * Default implementation of {@link TerminologyServiceFactory} providing {@link TerminologyService}
 * implemented using {@link TerminologyClient} with given configuration.
 *
 * @author John Grimes
 * @author Piotr Szul
 */
@Slf4j
@EqualsAndHashCode
@Getter
public class DefaultTerminologyServiceFactory implements TerminologyServiceFactory {

  private static final long serialVersionUID = 2837933007972812597L;

  @Nonnull
  private static final ObjectHolder<DefaultTerminologyServiceFactory, TerminologyService2> terminologyServiceHolder2 = new SingletonHolder<>();

  @Nonnull
  private static final ObjectHolder<DefaultTerminologyServiceFactory, TerminologyService> terminologyServiceHolder = new SingletonHolder<>();


  @Nonnull
  private final FhirVersionEnum fhirVersion;

  @Nonnull
  private final String terminologyServerUrl;

  private final int socketTimeout;

  private final boolean verboseRequestLogging;

  @Nonnull
  private final HttpClientConfiguration clientConfig;

  @Nonnull
  private final TerminologyAuthConfiguration authConfig;

  public static synchronized void invalidate() {
    log.info("Invalidating HTTPClient cache");
    terminologyServiceHolder2.invalidate();
  }

  public DefaultTerminologyServiceFactory(@Nonnull final FhirVersionEnum fhirVersion,
      @Nonnull final String terminologyServerUrl, final int socketTimeout,
      final boolean verboseRequestLogging,
      @Nonnull final HttpClientConfiguration clientConfig,
      @Nonnull final TerminologyAuthConfiguration authConfig) {
    this.fhirVersion = fhirVersion;
    this.terminologyServerUrl = terminologyServerUrl;
    this.socketTimeout = socketTimeout;
    this.verboseRequestLogging = verboseRequestLogging;
    this.authConfig = authConfig;
    this.clientConfig = clientConfig;
  }

  @Nonnull
  @Override
  public TerminologyService buildService() {
    return buildService(UUID::randomUUID);
  }

  /**
   * Builds a new instance.
   *
   * @param uuidFactory the {@link UUIDFactory to use for UUID generation}
   * @return a shiny new TerminologyService instance =
   */
  @Nonnull
  @Deprecated
  public TerminologyService buildService(
      @Nonnull final UUIDFactory uuidFactory) {
    // TODO: we ignore here rhe uuidFactor in the lookp key, but hopfully that's not an issue.
    return terminologyServiceHolder.getOrCreate(this,
        f -> f.createService(uuidFactory));
  }

  @Nonnull
  @Override
  public TerminologyService2 buildService2() {
    return terminologyServiceHolder2.getOrCreate(this,
        DefaultTerminologyServiceFactory::createService2);
  }

  @Nonnull
  private TerminologyService createService(@Nonnull final UUIDFactory uuidFactory) {
    final FhirContext fhirContext = FhirEncoders.contextFor(fhirVersion);
    //TODO: maybe share the HttpClient 
    final CloseableHttpClient httpClient = buildHttpClient(socketTimeout, clientConfig);
    final TerminologyClient terminologyClient = TerminologyClient.build(
        fhirContext, terminologyServerUrl, verboseRequestLogging, authConfig,
        httpClient);
    return new DefaultTerminologyService(fhirContext, terminologyClient, uuidFactory);
  }

  @Nonnull
  private DefaultTerminologyService2 createService2() {
    final FhirContext fhirContext = FhirEncoders.contextFor(fhirVersion);
    final CloseableHttpClient httpClient = buildHttpClient(socketTimeout, clientConfig);
    final TerminologyClient2 terminologyClient = TerminologyClient2.build(
        fhirContext, terminologyServerUrl, verboseRequestLogging, authConfig,
        httpClient);
    return new DefaultTerminologyService2(terminologyClient, httpClient);
  }

  private static CloseableHttpClient buildHttpClient(final int socketTimeout,
      @Nonnull final HttpClientConfiguration config) {
    final PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
    connectionManager.setMaxTotal(config.getMaxConnectionsTotal());
    connectionManager.setDefaultMaxPerRoute(config.getMaxConnectionsPerRoute());
    final HttpCacheConfiguration cacheConfiguration = config.getCache();
    final HttpClientBuilder clientBuilder;
    if (cacheConfiguration.isEnabled()) {
      final CacheConfig cacheConfig = CacheConfig.custom()
          .setMaxCacheEntries(cacheConfiguration.getMaxCacheEntries())
          .setMaxObjectSize(cacheConfiguration.getMaxObjectSize())
          .build();
      clientBuilder = CachingFactories.of(cacheConfiguration.getStorageType())
          .create(cacheConfig, cacheConfiguration.getStorage());
    } else {
      clientBuilder = HttpClients.custom();
    }

    final RequestConfig defaultRequestConfig = RequestConfig.custom()
        .setSocketTimeout(socketTimeout)
        .build();

    return clientBuilder
        .setDefaultRequestConfig(defaultRequestConfig)
        .setConnectionManager(connectionManager)
        .setConnectionManagerShared(false)
        .setRetryHandler(new DefaultHttpRequestRetryHandler(1, true))
        .build();
  }

}
