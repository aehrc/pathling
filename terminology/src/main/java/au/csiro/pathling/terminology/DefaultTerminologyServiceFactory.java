package au.csiro.pathling.terminology;

import au.csiro.pathling.caching.CachingFactories;
import au.csiro.pathling.config.HttpCacheConf;
import au.csiro.pathling.config.HttpClientConf;
import au.csiro.pathling.config.TerminologyAuthConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhir.TerminologyClient2;
import au.csiro.pathling.terminology.ObjectHolder.SingletonHolder;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.cache.CacheConfig;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import static java.util.Objects.nonNull;


/**
 * Default implementation of {@link TerminologyServiceFactory} providing {@link TerminologyService}
 * implemented using {@link TerminologyClient} with given configuration.
 *
 * @author John Grimes
 * @author Piotr Szul
 */
@Slf4j
@EqualsAndHashCode
@ToString
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

  private final boolean verboseRequestLogging;

  @Nonnull
  private final HttpClientConf clientConfig;

  @Nonnull
  private final HttpCacheConf cacheConfig;
  
  @Nonnull
  private final TerminologyAuthConfiguration authConfig;

  public static synchronized void reset() {
    log.info("Resetting terminology services");
    terminologyServiceHolder.invalidate();
    terminologyServiceHolder2.invalidate();
  }

  @Deprecated
  public DefaultTerminologyServiceFactory(@Nonnull final FhirVersionEnum fhirVersion,
      @Nonnull final String terminologyServerUrl,
      @Nullable final Integer socketTimeout,
      final boolean verboseRequestLogging,
      @Nonnull final HttpClientConf clientConfig,
      @Nonnull final HttpCacheConf cacheConfig,
      @Nonnull final TerminologyAuthConfiguration authConfig) {

    // For backwards compatibility with the old version config version
    this(fhirVersion, terminologyServerUrl, verboseRequestLogging,
        nonNull(socketTimeout)
        ? clientConfig.toBuilder()
            .socketTimeout(socketTimeout)
            .build()
        : clientConfig,
        cacheConfig,
        authConfig);
  }

  public DefaultTerminologyServiceFactory(@Nonnull final FhirVersionEnum fhirVersion,
      @Nonnull final String terminologyServerUrl,
      final boolean verboseRequestLogging,
      @Nonnull final HttpClientConf clientConfig,
      @Nonnull final HttpCacheConf cacheConfig,
      @Nonnull final TerminologyAuthConfiguration authConfig) {
    this.fhirVersion = fhirVersion;
    this.terminologyServerUrl = terminologyServerUrl;
    this.verboseRequestLogging = verboseRequestLogging;
    this.authConfig = authConfig;
    this.clientConfig = clientConfig;
    this.cacheConfig = cacheConfig;
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
    final CloseableHttpClient httpClient = buildHttpClient(clientConfig,
        cacheConfig);
    final TerminologyClient terminologyClient = TerminologyClient.build(
        fhirContext, terminologyServerUrl, verboseRequestLogging, authConfig,
        httpClient);
    return new DefaultTerminologyService(fhirContext, terminologyClient, uuidFactory);
  }

  @Nonnull
  private DefaultTerminologyService2 createService2() {
    final FhirContext fhirContext = FhirEncoders.contextFor(fhirVersion);
    final CloseableHttpClient httpClient = buildHttpClient(clientConfig,
        cacheConfig);
    final TerminologyClient2 terminologyClient = TerminologyClient2.build(
        fhirContext, terminologyServerUrl, verboseRequestLogging, authConfig,
        httpClient);
    return new DefaultTerminologyService2(terminologyClient, httpClient);
  }

  private static CloseableHttpClient buildHttpClient(
      @Nonnull final HttpClientConf clientConf,
      @Nonnull final HttpCacheConf cacheConf) {
    final PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
    connectionManager.setMaxTotal(clientConf.getMaxConnectionsTotal());
    connectionManager.setDefaultMaxPerRoute(clientConf.getMaxConnectionsPerRoute());
    final HttpClientBuilder clientBuilder;
    if (cacheConf.isEnabled()) {
      final CacheConfig cacheConfig = CacheConfig.custom()
          .setMaxCacheEntries(cacheConf.getMaxCacheEntries())
          .setMaxObjectSize(cacheConf.getMaxObjectSize())
          .build();
      clientBuilder = CachingFactories.of(cacheConf.getStorageType())
          .create(cacheConfig, cacheConf.getStorage());
    } else {
      clientBuilder = HttpClients.custom();
    }

    final RequestConfig defaultRequestConfig = RequestConfig.custom()
        .setSocketTimeout(clientConf.getSocketTimeout())
        .build();

    return clientBuilder
        .setDefaultRequestConfig(defaultRequestConfig)
        .setConnectionManager(connectionManager)
        .setConnectionManagerShared(false)
        .setRetryHandler(new DefaultHttpRequestRetryHandler(1, true))
        .build();
  }

}
