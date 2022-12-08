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

package au.csiro.pathling.library;

import static au.csiro.pathling.utilities.Preconditions.checkArgument;
import static java.util.Objects.nonNull;

import au.csiro.pathling.config.HttpClientCachingConfiguration;
import au.csiro.pathling.config.HttpClientCachingConfiguration.StorageType;
import au.csiro.pathling.config.HttpClientConfiguration;
import au.csiro.pathling.config.TerminologyAuthConfiguration;
import au.csiro.pathling.utilities.Default;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class PathlingContextConfiguration {

  public static final Default<String> DEFAULT_TERMINOLOGY_SERVER_URL = Default.of(
      "https://tx.ontoserver.csiro.au/fhir");

  public static final Default<Long> DEFAULT_TOKEN_EXPIRY_TOLERANCE = Default.of(
      TerminologyAuthConfiguration.DEF_TOKEN_EXPIRY_TOLERANCE);
  public static final Default<Boolean> DEFAULT_TERMINOLOGY_VERBOSE_LOGGING = Default.of(false);

  public static final Default<Integer> DEFAULT_MAX_TOTAL_CONNECTIONS = Default.of(
      HttpClientConfiguration.DEFAULT_MAX_CONNECTIONS_TOTAL);
  public static final Default<Integer> DEFAULT_MAX_CONNECTIONS_PER_ROUTE = Default.of(
      HttpClientConfiguration.DEFAULT_MAX_CONNECTIONS_PER_ROUTE);
  public static final Default<Integer> DEFAULT_SOCKET_TIMEOUT = Default.of(
      HttpClientConfiguration.DEFAULT_SOCKET_TIMEOUT);

  public static final Default<StorageType> DEFAULT_CACHE_STORAGE_TYPE = Default.of(
      HttpClientCachingConfiguration.DEFAULT_STORAGE_TYPE);
  public static final Default<Integer> DEFAULT_CACHE_MAX_ENTRIES = Default.of(
      HttpClientCachingConfiguration.DEFAULT_MAX_CACHE_ENTRIES);
  public static final Default<Long> DEFAULT_CACHE_MAX_OBJECT_SIZE = Default.of(
      HttpClientCachingConfiguration.DEFAULT_MAX_OBJECT_SIZE);

  @Nullable
  String fhirVersion;

  @Nullable
  Integer maxNestingLevel;

  @Nullable
  Boolean extensionsEnabled;

  @Nullable
  List<String> openTypesEnabled;

  @Nullable
  String terminologyServerUrl;

  @Nullable
  Integer terminologySocketTimeout;

  @Nullable
  Boolean terminologyVerboseRequestLogging;

  @Nullable
  Integer maxConnectionsTotal;

  @Nullable
  Integer maxConnectionsPerRoute;

  @Nullable
  Integer cacheMaxEntries;

  @Nullable
  Long cacheMaxObjectSize;

  @Nullable
  @Builder.Default
  String cacheStorageType = HttpClientCachingConfiguration.DEFAULT_STORAGE_TYPE.toString();

  @Nullable
  String cacheStoragePath;

  @Nullable
  String tokenEndpoint;

  @Nullable
  String clientId;

  @Nullable
  String clientSecret;

  @Nullable
  String scope;

  @Nullable
  Long tokenExpiryTolerance;

  @Builder.Default
  boolean mockTerminology = false;

  @Nonnull
  TerminologyAuthConfiguration toAuthConfig() {
    final TerminologyAuthConfiguration authConfig = TerminologyAuthConfiguration.defaults();
    if (nonNull(getTokenEndpoint()) && nonNull(getClientId()) && nonNull(getClientSecret())) {
      authConfig.setEnabled(true);
      authConfig.setTokenEndpoint(getTokenEndpoint());
      authConfig.setClientId(getClientId());
      authConfig.setClientSecret(getClientSecret());
      authConfig.setScope(getScope());
    }
    authConfig.setTokenExpiryTolerance(
        DEFAULT_TOKEN_EXPIRY_TOLERANCE.resolve(getTokenExpiryTolerance()));
    return authConfig;
  }


  @Nonnull
  HttpClientConfiguration toClientConfig() {
    final HttpClientConfiguration config = HttpClientConfiguration.defaults();
    config.setMaxConnectionsTotal(DEFAULT_MAX_TOTAL_CONNECTIONS.resolve(getMaxConnectionsTotal()));
    config.setMaxConnectionsPerRoute(
        DEFAULT_MAX_CONNECTIONS_PER_ROUTE.resolve(getMaxConnectionsPerRoute()));
    config.setSocketTimeout(DEFAULT_SOCKET_TIMEOUT.resolve(getTerminologySocketTimeout()));
    return config;
  }

  @Nonnull
  HttpClientCachingConfiguration toCacheConfig() {
    final HttpClientCachingConfiguration config = HttpClientCachingConfiguration.defaults();
    final boolean enabled = nonNull(getCacheStorageType());
    config.setEnabled(enabled);
    if (enabled) {
      config.setMaxEntries(DEFAULT_CACHE_MAX_ENTRIES.resolve(getCacheMaxEntries()));
      config.setMaxObjectSize(DEFAULT_CACHE_MAX_OBJECT_SIZE.resolve(getCacheMaxObjectSize()));
      final StorageType storageType = StorageType.fromCode(getCacheStorageType());
      config.setStorageType(DEFAULT_CACHE_STORAGE_TYPE.resolve(storageType));
      if (config.getStorageType().equals(StorageType.DISK)) {
        checkArgument(nonNull(getCacheStoragePath()),
            "Cache storage path must be specified when cache storage type is disk");
        config.setStoragePath(getCacheStoragePath());
      }
    }
    return config;
  }
}
