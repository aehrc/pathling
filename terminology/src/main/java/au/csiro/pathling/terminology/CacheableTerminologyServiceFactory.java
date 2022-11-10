package au.csiro.pathling.terminology;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.caching.CachingFactories;
import au.csiro.pathling.config.HttpCacheConfiguration;
import au.csiro.pathling.config.HttpClientConfiguration;
import au.csiro.pathling.config.TerminologyAuthConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhir.TerminologyClient2;
import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.sql.udf.ValidateCoding;
import au.csiro.pathling.terminology.ObjectHolder.SingletonHolder;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import javax.annotation.Nonnull;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.cache.CacheConfig;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;


@Slf4j
@EqualsAndHashCode
public class CacheableTerminologyServiceFactory implements TerminologyServiceFactory {

  private static final long serialVersionUID = 2837933007972812597L;

  @Nonnull
  private static final ObjectHolder<CacheableTerminologyServiceFactory, TerminologyService> terminologyServiceHolder = new SingletonHolder<>();

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
    terminologyServiceHolder.invalidate();
  }

  public CacheableTerminologyServiceFactory(@Nonnull final FhirVersionEnum fhirVersion,
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
    return terminologyServiceHolder.getOrCreate(this,
        CacheableTerminologyServiceFactory::createService);
  }

  @Nonnull
  private SimpleTerminologyService createService() {
    final FhirContext fhirContext = FhirEncoders.contextFor(fhirVersion);
    final CloseableHttpClient httpClient = buildHttpClient(clientConfig);
    final TerminologyClient2 terminologyClient = TerminologyClient2.build(
        fhirContext, terminologyServerUrl, socketTimeout, verboseRequestLogging, authConfig,
        httpClient);
    return new SimpleTerminologyService(terminologyClient, httpClient);
  }

  private static CloseableHttpClient buildHttpClient(
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
    return clientBuilder
        .setConnectionManager(connectionManager)
        .setConnectionManagerShared(false)
        .setRetryHandler(new DefaultHttpRequestRetryHandler(1, true))
        .build();
  }

}
