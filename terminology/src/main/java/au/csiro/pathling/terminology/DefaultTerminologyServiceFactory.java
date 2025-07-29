/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.terminology;

import au.csiro.pathling.config.HttpClientCachingConfiguration;
import au.csiro.pathling.config.HttpClientCachingStorageType;
import au.csiro.pathling.config.HttpClientConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.terminology.caching.InMemoryCachingTerminologyService;
import au.csiro.pathling.terminology.caching.PersistentCachingTerminologyService;
import au.csiro.pathling.utilities.ObjectHolder;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import jakarta.annotation.Nonnull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

/**
 * Default implementation of {@link TerminologyServiceFactory}, providing the appropriate
 * implementation of {@link TerminologyService} based upon the given configuration.
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
  private static final ObjectHolder<DefaultTerminologyServiceFactory, TerminologyService> terminologyServiceHolder = ObjectHolder.singleton(
      DefaultTerminologyServiceFactory::createService);

  /**
   * The FHIR version to use for terminology services.
   */
  @Nonnull
  private final FhirVersionEnum fhirVersion;

  /**
   * The terminology configuration settings.
   */
  @Nonnull
  private final TerminologyConfiguration configuration;


  /**
   * Resets the terminology services, clearing any cached instances.
   * This is useful for testing or when configuration changes require a fresh instance.
   */
  public static synchronized void reset() {
    log.info("Resetting terminology services");
    terminologyServiceHolder.reset();
  }

  /**
   * Constructs a new {@link DefaultTerminologyServiceFactory} with the specified FHIR version and
   * terminology configuration.
   *
   * @param fhirVersion the FHIR version to use
   * @param configuration the terminology configuration settings
   */
  public DefaultTerminologyServiceFactory(@Nonnull final FhirVersionEnum fhirVersion,
      @Nonnull final TerminologyConfiguration configuration) {
    this.fhirVersion = fhirVersion;
    this.configuration = configuration;
  }

  @Nonnull
  @Override
  public TerminologyService build() {
    return terminologyServiceHolder.getOrCreate(this);
  }

  @Nonnull
  private TerminologyService createService() {

    final FhirContext fhirContext = FhirEncoders.contextFor(fhirVersion);
    final CloseableHttpClient httpClient = buildHttpClient(configuration.getClient());
    final TerminologyClient terminologyClient = TerminologyClient.build(fhirContext, configuration,
        httpClient);
    final HttpClientCachingConfiguration cacheConfig = configuration.getCache();

    if (cacheConfig.isEnabled() && cacheConfig.getStorageType()
        .equals(HttpClientCachingStorageType.DISK)) {
      // If caching is enabled and storage type is disk, use a persistent caching terminology 
      // service implementation.
      log.debug("Creating PersistentCachingTerminologyService with cache config: {}", cacheConfig);
      return new PersistentCachingTerminologyService(terminologyClient, cacheConfig, httpClient,
          terminologyClient);

    } else if (cacheConfig.isEnabled() && cacheConfig.getStorageType().equals(
        HttpClientCachingStorageType.MEMORY)) {
      // If caching is enabled and storage type is memory, use an in-memory caching terminology
      // service implementation.
      log.debug("Creating InMemoryCachingTerminologyService with cache config: {}", cacheConfig);
      return new InMemoryCachingTerminologyService(terminologyClient, cacheConfig, httpClient,
          terminologyClient);

    } else {
      // If caching is disabled, use a terminology service implementation that does not cache.
      log.debug("Creating DefaultTerminologyService with no caching");
      return new DefaultTerminologyService(terminologyClient, httpClient, terminologyClient);
    }
  }

  private static CloseableHttpClient buildHttpClient(
      @Nonnull final HttpClientConfiguration clientConfig) {

    final PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
    connectionManager.setMaxTotal(clientConfig.getMaxConnectionsTotal());
    connectionManager.setDefaultMaxPerRoute(clientConfig.getMaxConnectionsPerRoute());

    final RequestConfig defaultRequestConfig = RequestConfig.custom()
        .setSocketTimeout(clientConfig.getSocketTimeout())
        .build();

    final HttpClientBuilder clientBuilder = HttpClients.custom()
        .setDefaultRequestConfig(defaultRequestConfig)
        .setConnectionManager(connectionManager)
        .setConnectionManagerShared(false);

    if (clientConfig.isRetryEnabled()) {
      clientBuilder.setRetryHandler(new RequestRetryHandler(clientConfig.getRetryCount()));
    }

    return clientBuilder.build();
  }

}
