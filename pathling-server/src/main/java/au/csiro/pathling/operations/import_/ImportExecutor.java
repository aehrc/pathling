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

import au.csiro.pathling.cache.CacheableDatabase;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.UnsupportedResourceError;
import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.errors.SecurityError;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.sink.DataSinkBuilder;
import au.csiro.pathling.library.io.sink.FileInfo;
import au.csiro.pathling.library.io.sink.WriteDetails;
import au.csiro.pathling.library.io.source.DataSourceBuilder;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.security.PathlingAuthority;
import au.csiro.pathling.security.ResourceAccess.AccessType;
import au.csiro.pathling.security.SecurityAspect;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import jakarta.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.*;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.OperationOutcome.OperationOutcomeIssueComponent;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

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
   * @param databasePath directory to where the data will be imported
   */
  public ImportExecutor(@Nonnull final Optional<AccessRules> accessRules,
      PathlingContext pathlingContext,
      @Value("${pathling.storage.warehouseUrl}/${pathling.storage.databaseName}")
      String databasePath, ServerConfiguration serverConfiguration) {
    this.accessRules = accessRules;
    this.pathlingContext = pathlingContext;
    this.databasePath = databasePath;
    this.serverConfiguration = serverConfiguration;
  }
  
  @Nonnull
  public ImportResponse execute(@Nonnull final ImportRequest importRequest, String jobId) {
    log.info("Received $import request");
    WriteDetails writeDetails = readAndWriteFilesFrom(importRequest, jobId);
    return new ImportResponse(importRequest.originalRequest(), importRequest, writeDetails);
  }



  private WriteDetails readAndWriteFilesFrom(ImportRequest request, String jobId) {
    DataSourceBuilder sourceBuilder = new DataSourceBuilder(pathlingContext);

    Map<String, Collection<String>> resourcesWithAuthority = checkAuthority(request);

    Collection<String> resourceFiles = resourcesWithAuthority.values().stream().flatMap(Collection::stream).toList();
    Function<DataSource, DataSinkBuilder> sinkBuilderFunc = dataSource -> new DataSinkBuilder(pathlingContext, dataSource).saveMode(request.saveMode().getCode());
    Function<String, Set<String>> filenameMapper = resourceType -> new HashSet<>(request.input().getOrDefault(resourceType, Set.of()));
    return switch (request.importFormat()) {
      case NDJSON -> sinkBuilderFunc.apply(sourceBuilder.ndjson(resourceFiles, "ndjson", filenameMapper)).ndjson(databasePath);
      case DELTA -> sinkBuilderFunc.apply(sourceBuilder.delta(resourceFiles)).delta(databasePath);
      case PARQUET -> sinkBuilderFunc.apply(sourceBuilder.parquet(resourceFiles)).parquet(databasePath);
    };
  }

  private @NotNull Map<String, Collection<String>> checkAuthority(ImportRequest request) {
    if(serverConfiguration.getAuth().isEnabled() && !SecurityAspect.hasAuthority(PathlingAuthority.fromAuthority("pathling:write"))) {
      throw new AccessDeniedError("Missing authority 'pathling:write'");
    }
    
    record WriteAuthority(Collection<String> fileToImport, boolean authority) {}
    
    Map<String, WriteAuthority> writeAuthorityMap = request.input().entrySet().stream()
        .collect(Collectors.toMap(
            Entry::getKey,
            entry -> new WriteAuthority(entry.getValue(), !serverConfiguration.getAuth().isEnabled() || SecurityAspect.hasAuthority(
                PathlingAuthority.resourceAccess(AccessType.WRITE, ResourceType.fromCode(entry.getKey()))))
        ));

    List<String> missingAuthMsg = writeAuthorityMap.entrySet().stream()
        .filter(entry -> !entry.getValue().authority())
        .map(entry -> "Missing authority: pathling:write:%s for %s".formatted(entry.getKey(), entry.getValue().fileToImport()))
        .toList();
    if(!missingAuthMsg.isEmpty()) {
      throw new AccessDeniedError("Missing auths: %s".formatted(String.join(",", missingAuthMsg)));
    }
    
    writeAuthorityMap.values().stream()
        .map(WriteAuthority::fileToImport)
        .forEach(fileToImport -> accessRules.ifPresent(ar -> fileToImport.forEach(ar::checkCanImportFrom)));

    return writeAuthorityMap.entrySet().stream()
        .filter(entry -> entry.getValue().authority())
        .collect(Collectors.toMap(
            Map.Entry::getKey,
            entry -> entry.getValue().fileToImport()
        ));
  }
}
