/*
 * Copyright 2022 Commonwealth Scientific and Industrial Research
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

import static java.util.Objects.nonNull;

import au.csiro.pathling.config.HttpClientCachingConfiguration;
import au.csiro.pathling.config.HttpClientCachingConfiguration.StorageType;
import au.csiro.pathling.config.HttpClientConfiguration;
import au.csiro.pathling.config.TerminologyAuthConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.terminology.caching.InMemoryCachingTerminologyService;
import au.csiro.pathling.terminology.caching.PersistentCachingTerminologyService;
import au.csiro.pathling.utilities.ObjectHolder;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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


  @Nonnull
  private final FhirVersionEnum fhirVersion;

  @Nonnull
  private final String terminologyServerUrl;

  private final boolean verboseRequestLogging;

  @Nonnull
  private final HttpClientConfiguration clientConfig;

  @Nonnull
  private final HttpClientCachingConfiguration cacheConfig;

  @Nonnull
  private final TerminologyAuthConfiguration authConfig;

  public static synchronized void reset() {
    log.info("Resetting terminology services");
    terminologyServiceHolder.reset();
  }

  @Deprecated
  public DefaultTerminologyServiceFactory(@Nonnull final FhirVersionEnum fhirVersion,
      @Nonnull final String terminologyServerUrl,
      @Nullable final Integer socketTimeout,
      final boolean verboseRequestLogging,
      @Nonnull final HttpClientConfiguration clientConfig,
      @Nonnull final HttpClientCachingConfiguration cacheConfig,
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
      @Nonnull final HttpClientConfiguration clientConfig,
      @Nonnull final HttpClientCachingConfiguration cacheConfig,
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
  public TerminologyService build() {
    return terminologyServiceHolder.getOrCreate(this);
  }

  @Nonnull
  private TerminologyService createService() {
    final FhirContext fhirContext = FhirEncoders.contextFor(fhirVersion);
    final CloseableHttpClient httpClient = buildHttpClient(clientConfig);
    final TerminologyClient terminologyClient = TerminologyClient.build(fhirContext,
        terminologyServerUrl, verboseRequestLogging, authConfig, httpClient);

    if (cacheConfig.isEnabled() && cacheConfig.getStorageType().equals(StorageType.DISK)) {
      // If caching is enabled and storage type is disk, use a persistent caching terminology 
      // service implementation.
      return new PersistentCachingTerminologyService(terminologyClient, httpClient, cacheConfig);
    } else if (cacheConfig.isEnabled() && cacheConfig.getStorageType().equals(StorageType.MEMORY)) {
      // If caching is enabled and storage type is memory, use an in-memory caching terminology
      // service implementation.
      return new InMemoryCachingTerminologyService(terminologyClient, httpClient, cacheConfig);
    } else {
      // If caching is disabled, use a terminology service implementation that does not cache.
      return new DefaultTerminologyService(terminologyClient, httpClient);
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
