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

package au.csiro.pathling.operations.import_;

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.sink.DataSinkBuilder;
import au.csiro.pathling.library.io.sink.WriteDetails;
import au.csiro.pathling.library.io.source.DataSourceBuilder;
import au.csiro.pathling.security.PathlingAuthority;
import au.csiro.pathling.security.ResourceAccess.AccessType;
import au.csiro.pathling.security.SecurityAspect;
import jakarta.annotation.Nonnull;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Encapsulates the execution of an import operation.
 *
 * @author  Felix Naumann
 * @see <a href="https://pathling.csiro.au/docs/server/operations/import">Import</a>
 */
@Component
@Profile({"core", "import"})
@Slf4j
public class ImportExecutor {

  @Nonnull
  private final Optional<AccessRules> accessRules;
  private final PathlingContext pathlingContext;
  
  private final String databasePath;
  private final ServerConfiguration serverConfiguration;

  /**
   * @param accessRules a {@link AccessRules} for validating access to URLs
   * @param pathlingContext the Pathling context for Spark and FHIR operations
   * @param databasePath directory to where the data will be imported
   * @param serverConfiguration the server configuration including authentication settings
   */
  public ImportExecutor(@Nonnull final Optional<AccessRules> accessRules,
      final PathlingContext pathlingContext,
      @Value("${pathling.storage.warehouseUrl}/${pathling.storage.databaseName}")
      final String databasePath, final ServerConfiguration serverConfiguration) {
    this.accessRules = accessRules;
    this.pathlingContext = pathlingContext;
    this.databasePath = databasePath;
    this.serverConfiguration = serverConfiguration;
  }
  
  /**
   * Executes the import operation.
   *
   * @param importRequest the import request containing the source files and configuration
   * @param jobId the job identifier for tracking this import operation
   * @return the import response containing details of the imported data
   */
  @Nonnull
  public ImportResponse execute(@Nonnull final ImportRequest importRequest,
      @SuppressWarnings("unused") final String jobId) {
    log.info("Received $import request");
    final WriteDetails writeDetails = readAndWriteFilesFrom(importRequest);
    return new ImportResponse(importRequest.originalRequest(), importRequest, writeDetails);
  }



  private WriteDetails readAndWriteFilesFrom(final ImportRequest request) {
    final DataSourceBuilder sourceBuilder = new DataSourceBuilder(pathlingContext);

    final Map<String, Collection<String>> resourcesWithAuthority = checkAuthority(request);

    final Function<DataSource, DataSinkBuilder> sinkBuilderFunc = dataSource -> new DataSinkBuilder(pathlingContext, dataSource).saveMode(request.saveMode().getCode());
    return switch (request.importFormat()) {
      case NDJSON -> sinkBuilderFunc.apply(sourceBuilder.ndjson(resourcesWithAuthority, "ndjson")).ndjson(databasePath);
      case DELTA -> sinkBuilderFunc.apply(sourceBuilder.delta(resourcesWithAuthority)).delta(databasePath);
      case PARQUET -> sinkBuilderFunc.apply(sourceBuilder.parquet(resourcesWithAuthority)).parquet(databasePath);
    };
  }

  private @NotNull Map<String, Collection<String>> checkAuthority(final ImportRequest request) {
    if (serverConfiguration.getAuth().isEnabled()) {
      // Check global write authority.
      SecurityAspect.checkHasAuthority(PathlingAuthority.fromAuthority("pathling:write"));

      // Check per-resource-type write authority.
      for (final Entry<String, Collection<String>> entry : request.input().entrySet()) {
        SecurityAspect.checkHasAuthority(
            PathlingAuthority.resourceAccess(AccessType.WRITE,
                ResourceType.fromCode(entry.getKey())));
      }
    }

    // Validate file access rules.
    request.input().values().stream()
        .flatMap(Collection::stream)
        .forEach(file -> accessRules.ifPresent(ar -> ar.checkCanImportFrom(file)));

    return request.input();
  }
}
