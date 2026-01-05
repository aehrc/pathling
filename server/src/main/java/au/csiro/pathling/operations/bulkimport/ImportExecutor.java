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

package au.csiro.pathling.operations.bulkimport;

import au.csiro.pathling.cache.CacheableDatabase;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.sink.DataSinkBuilder;
import au.csiro.pathling.library.io.sink.WriteDetails;
import au.csiro.pathling.library.io.source.DeltaSource;
import au.csiro.pathling.library.io.source.NdjsonSource;
import au.csiro.pathling.library.io.source.ParquetSource;
import au.csiro.pathling.security.PathlingAuthority;
import au.csiro.pathling.security.ResourceAccess.AccessType;
import au.csiro.pathling.security.SecurityAspect;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Encapsulates the execution of an import operation.
 *
 * @author Felix Naumann
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/server/operations/import">Import</a>
 */
@Component
@Slf4j
public class ImportExecutor {

  @Nonnull private final Optional<AccessRules> accessRules;
  private final PathlingContext pathlingContext;

  private final String databasePath;
  private final ServerConfiguration serverConfiguration;

  @Nonnull private final CacheableDatabase cacheableDatabase;

  /**
   * @param accessRules a {@link AccessRules} for validating access to URLs
   * @param pathlingContext the Pathling context for Spark and FHIR operations
   * @param databasePath directory to where the data will be imported
   * @param serverConfiguration the server configuration including authentication settings
   * @param cacheableDatabase the cacheable database for cache invalidation
   */
  public ImportExecutor(
      @Nonnull final Optional<AccessRules> accessRules,
      final PathlingContext pathlingContext,
      @Value("${pathling.storage.warehouseUrl}/${pathling.storage.databaseName}")
          final String databasePath,
      final ServerConfiguration serverConfiguration,
      @Nonnull final CacheableDatabase cacheableDatabase) {
    this.accessRules = accessRules;
    this.pathlingContext = pathlingContext;
    this.databasePath = databasePath;
    this.serverConfiguration = serverConfiguration;
    this.cacheableDatabase = cacheableDatabase;
  }

  /**
   * Executes the import operation using the configured {@link AccessRules} for URL validation.
   *
   * @param importRequest the import request containing the source files and configuration
   * @param jobId the job identifier for tracking this import operation
   * @return the import response containing details of the imported data
   */
  @Nonnull
  public ImportResponse execute(
      @Nonnull final ImportRequest importRequest, @SuppressWarnings("unused") final String jobId) {
    return execute(importRequest, jobId, null);
  }

  /**
   * Executes the import operation with custom allowable sources for URL validation.
   *
   * <p>When custom allowable sources are provided, they are used instead of the configured {@link
   * AccessRules}. This allows callers (such as the bulk submit operation) to use their own
   * allowable sources configuration.
   *
   * @param importRequest the import request containing the source files and configuration
   * @param jobId the job identifier for tracking this import operation
   * @param customAllowableSources optional list of URL prefixes to use for validation; if null, the
   *     configured {@link AccessRules} will be used
   * @return the import response containing details of the imported data
   */
  @Nonnull
  public ImportResponse execute(
      @Nonnull final ImportRequest importRequest,
      @SuppressWarnings("unused") final String jobId,
      @Nullable final List<String> customAllowableSources) {
    log.info("Received $import request");
    final WriteDetails writeDetails = readAndWriteFilesFrom(importRequest, customAllowableSources);
    log.info("Import completed successfully");
    return new ImportResponse(importRequest.originalRequest(), importRequest, writeDetails);
  }

  private WriteDetails readAndWriteFilesFrom(
      final ImportRequest request, @Nullable final List<String> customAllowableSources) {
    final Map<String, Collection<String>> resourcesWithAuthority =
        checkAuthority(request, customAllowableSources);

    // Create the appropriate data source based on the import format.
    final DataSource dataSource =
        switch (request.importFormat()) {
          case NDJSON -> new NdjsonSource(pathlingContext, resourcesWithAuthority, "ndjson");
          case DELTA -> new DeltaSource(pathlingContext, resourcesWithAuthority);
          case PARQUET ->
              new ParquetSource(
                  pathlingContext,
                  resourcesWithAuthority,
                  (Predicate<ResourceType>) ignored -> true);
        };

    // Always write to Delta format regardless of source format.
    final WriteDetails writeDetails =
        new DataSinkBuilder(pathlingContext, dataSource)
            .saveMode(request.saveMode().getCode())
            .delta(databasePath);

    // Invalidate the cache to ensure subsequent requests see the updated data.
    cacheableDatabase.invalidate();

    return writeDetails;
  }

  private @NotNull Map<String, Collection<String>> checkAuthority(
      final ImportRequest request, @Nullable final List<String> customAllowableSources) {
    if (serverConfiguration.getAuth().isEnabled()) {
      // Check global write authority.
      SecurityAspect.checkHasAuthority(PathlingAuthority.fromAuthority("pathling:write"));

      // Check per-resource-type write authority.
      for (final Entry<String, Collection<String>> entry : request.input().entrySet()) {
        SecurityAspect.checkHasAuthority(
            PathlingAuthority.resourceAccess(
                AccessType.WRITE, ResourceType.fromCode(entry.getKey())));
      }
    }

    // TODO: Re-enable allowed sources check after connectathon.
    // Validate file access rules.
    // if (customAllowableSources != null) {
    //   // Use custom allowable sources provided by the caller.
    //   request.input().values().stream()
    //       .flatMap(Collection::stream)
    //       .forEach(file -> checkCustomAllowableSources(file, customAllowableSources));
    // } else {
    //   // Use the configured access rules.
    //   request.input().values().stream()
    //       .flatMap(Collection::stream)
    //       .forEach(file -> accessRules.ifPresent(ar -> ar.checkCanImportFrom(file)));
    // }

    return request.input();
  }

  // TODO: Re-enable along with allowed sources check after connectathon.
  @SuppressWarnings("unused")
  private void checkCustomAllowableSources(
      @Nonnull final String url, @Nonnull final List<String> allowableSources) {
    if (allowableSources.isEmpty()) {
      return;
    }
    final boolean allowed = allowableSources.stream().anyMatch(url::startsWith);
    if (!allowed) {
      throw new AccessDeniedError("URL: '" + url + "' is not an allowed source.");
    }
  }
}
