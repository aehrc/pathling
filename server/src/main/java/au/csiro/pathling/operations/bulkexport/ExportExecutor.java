/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.operations.bulkexport;

import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.array_union;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.map;
import static org.apache.spark.sql.functions.struct;

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.sink.DataSinkBuilder;
import au.csiro.pathling.library.io.sink.WriteDetails;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.operations.bulkexport.ExportRequest.ExportLevel;
import au.csiro.pathling.operations.compartment.PatientCompartmentService;
import au.csiro.pathling.security.PathlingAuthority;
import au.csiro.pathling.security.ResourceAccess.AccessType;
import au.csiro.pathling.security.SecurityAspect;
import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Performs the $export logic.
 *
 * @author Felix Naumann
 * @author John Grimes
 */
@Slf4j
@Component
public class ExportExecutor {

  @Nonnull private final PathlingContext pathlingContext;

  @Nonnull private final QueryableDataSource deltaLake;

  @Nonnull private final FhirContext fhirContext;

  @Nonnull private final SparkSession sparkSession;

  @Nonnull private final String databasePath;

  @Nonnull private final ServerConfiguration serverConfiguration;

  @Nonnull private final PatientCompartmentService patientCompartmentService;

  /**
   * Constructs a new ExportExecutor.
   *
   * @param pathlingContext The Pathling context.
   * @param deltaLake The queryable data source.
   * @param fhirContext The FHIR context.
   * @param sparkSession The Spark session.
   * @param databasePath The database path.
   * @param serverConfiguration The server configuration.
   * @param patientCompartmentService The patient compartment service.
   */
  @Autowired
  public ExportExecutor(
      @Nonnull final PathlingContext pathlingContext,
      @Nonnull final QueryableDataSource deltaLake,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final SparkSession sparkSession,
      @Nonnull @Value("${pathling.storage.warehouseUrl}/${pathling.storage.databaseName}")
          final String databasePath,
      @Nonnull final ServerConfiguration serverConfiguration,
      @Nonnull final PatientCompartmentService patientCompartmentService) {
    this.pathlingContext = pathlingContext;
    this.deltaLake = deltaLake;
    this.fhirContext = fhirContext;
    this.sparkSession = sparkSession;
    this.databasePath = databasePath;
    this.serverConfiguration = serverConfiguration;
    this.patientCompartmentService = patientCompartmentService;
  }

  /**
   * Perform the $export request. The input data is already parsed and validated.
   *
   * @param exportRequest The export request data.
   * @param jobId The job id to which this async request belongs to.
   * @return The export response data.
   */
  @Nonnull
  public ExportResponse execute(
      @Nonnull final ExportRequest exportRequest, @Nonnull final String jobId) {
    // Filter out resources (silently) due to insufficient permissions.
    QueryableDataSource mapped = checkResourceAccess(AccessType.READ, deltaLake);

    mapped = applyResourceTypeFiltering(exportRequest, mapped);
    mapped = applySinceDateFilter(exportRequest, mapped);
    mapped = applyUntilDateFilter(exportRequest, mapped);
    mapped = applyTypeFilters(exportRequest, mapped);

    // Apply patient compartment filtering for patient-level and group-level exports.
    if (exportRequest.exportLevel() != ExportLevel.SYSTEM) {
      mapped = applyPatientCompartmentFilter(exportRequest, mapped);
    }

    if (!exportRequest.elements().isEmpty()) {
      mapped = applyElementsParams(exportRequest, mapped);
      // If the returned ndjson is limited by the _elements param, then it should have the SUBSETTED
      // tag.
      final Column subsettedTagArray = createSubsettedTagInSparkStructure();
      mapped = addSubsettedTag(mapped, subsettedTagArray);
    }
    return writeResultToJobDirectory(exportRequest, jobId, mapped);
  }

  /**
   * Applies patient compartment filtering for patient-level and group-level exports.
   *
   * @param exportRequest the export request
   * @param dataSource the data source to filter
   * @return the filtered data source
   */
  @Nonnull
  private QueryableDataSource applyPatientCompartmentFilter(
      @Nonnull final ExportRequest exportRequest, @Nonnull final QueryableDataSource dataSource) {
    final Set<String> patientIds = exportRequest.patientIds();

    // First, filter out resource types that are not in the Patient compartment.
    final QueryableDataSource filtered =
        dataSource.filterByResourceType(patientCompartmentService::isInPatientCompartment);

    // Then, apply row-level filtering based on patient compartment membership.
    // We use filterByPatientCompartment which performs a semi-join, avoiding the need to collect
    // all matching IDs into driver memory. This is more scalable for large datasets.
    return filtered.map(
        (resourceType, rowDataset) -> {
          log.debug("Applying patient compartment filter for resource type {}", resourceType);
          return patientCompartmentService.filterByPatientCompartment(
              resourceType, patientIds, rowDataset, deltaLake);
        });
  }

  @Nonnull
  private QueryableDataSource checkResourceAccess(
      @Nonnull final AccessType accessType, @Nonnull final QueryableDataSource dataSource) {
    if (!serverConfiguration.getAuth().isEnabled()) {
      return dataSource;
    }
    return dataSource.filterByResourceType(
        resourceType -> {
          if (!SecurityAspect.hasAuthority(
              PathlingAuthority.resourceAccess(accessType, resourceType))) {
            log.debug(
                "Insufficient {} resource access permissions for {}. Hiding resource from user.",
                accessType.getCode(),
                resourceType);
            return false;
          } else {
            return true;
          }
        });
  }

  @Nonnull
  private ExportResponse writeResultToJobDirectory(
      @Nonnull final ExportRequest exportRequest,
      @Nonnull final String jobId,
      @Nonnull final QueryableDataSource mapped) {
    final URI warehouseUri = URI.create(databasePath);
    final Path warehousePath = new Path(warehouseUri);
    final Path jobDirPath = new Path(new Path(warehousePath, "jobs"), jobId);
    final Configuration configuration = sparkSession.sparkContext().hadoopConfiguration();
    try {
      final FileSystem fs = FileSystem.get(configuration);
      if (!fs.exists(jobDirPath)) {
        final boolean created = fs.mkdirs(jobDirPath);
        if (!created) {
          throw new InternalErrorException(
              "Failed to created subdirectory at %s for job %s.".formatted(databasePath, jobId));
        }
        log.debug("Created dir {}", jobDirPath);
      }

      final DataSinkBuilder sinkBuilder =
          new DataSinkBuilder(pathlingContext, mapped).saveMode("overwrite");
      final WriteDetails writeDetails =
          switch (exportRequest.outputFormat()) {
            case NDJSON -> sinkBuilder.ndjson(jobDirPath.toString());
            case PARQUET -> sinkBuilder.parquet(jobDirPath.toString());
            case null -> sinkBuilder.ndjson(jobDirPath.toString());
          };
      return new ExportResponse(
          exportRequest.originalRequest(),
          exportRequest.serverBaseUrl(),
          writeDetails,
          serverConfiguration.getAuth().isEnabled());
    } catch (final IOException e) {
      throw new InternalErrorException(
          "Failed to created subdirectory at %s for job %s.".formatted(databasePath, jobId));
    }
  }

  @Nonnull
  private static QueryableDataSource addSubsettedTag(
      @Nonnull final QueryableDataSource mapped, @Nonnull final Column subsettedTagArray) {
    return mapped.map(
        rowDataset ->
            rowDataset.withColumn(
                "meta",
                struct(
                    coalesce(col("meta.id"), lit(null).cast(DataTypes.StringType)).as("id"),
                    coalesce(col("meta.versionId"), lit(null).cast(DataTypes.StringType))
                        .as("versionId"),
                    coalesce(col("meta.versionId_versioned"), lit(null).cast(DataTypes.StringType))
                        .as("versionId_versioned"),
                    coalesce(col("meta.lastUpdated"), lit(null).cast(DataTypes.TimestampType))
                        .as("lastUpdated"),
                    coalesce(col("meta.source"), lit(null).cast(DataTypes.StringType)).as("source"),
                    coalesce(
                            col("meta.profile"),
                            lit(null).cast(DataTypes.createArrayType(DataTypes.StringType)))
                        .as("profile"),
                    coalesce(
                            col("meta.security"),
                            lit(null)
                                .cast(
                                    DataTypes.createArrayType(
                                        DataTypes.createStructType(
                                            new StructField[] {
                                              DataTypes.createStructField(
                                                  "id", DataTypes.StringType, true),
                                              DataTypes.createStructField(
                                                  "system", DataTypes.StringType, true),
                                              DataTypes.createStructField(
                                                  "version", DataTypes.StringType, true),
                                              DataTypes.createStructField(
                                                  "code", DataTypes.StringType, true),
                                              DataTypes.createStructField(
                                                  "display", DataTypes.StringType, true),
                                              DataTypes.createStructField(
                                                  "userSelected", DataTypes.BooleanType, true),
                                              DataTypes.createStructField(
                                                  "_fid", DataTypes.IntegerType, true)
                                            }))))
                        .as("security"),
                    // Always combine existing tags with the new SUBSETTED tag
                    array_union(coalesce(col("meta.tag"), array()), subsettedTagArray).as("tag"),
                    coalesce(col("meta._fid"), lit(null).cast(DataTypes.IntegerType)).as("_fid"))));
  }

  @Nonnull
  private static Column createSubsettedTagInSparkStructure() {
    return array(
        struct(
            lit(null).cast(DataTypes.StringType).as("id"),
            lit("http://terminology.hl7.org/CodeSystem/v3-ObservationValue").as("system"),
            lit(null).cast(DataTypes.StringType).as("version"),
            lit("SUBSETTED").as("code"),
            lit("Resource encoded in summary mode").as("display"),
            lit(null).cast(DataTypes.BooleanType).as("userSelected"),
            lit(null).cast(DataTypes.IntegerType).as("_fid")));
  }

  @Nonnull
  private QueryableDataSource applyElementsParams(
      @Nonnull final ExportRequest exportRequest, @Nonnull final QueryableDataSource mapped) {
    final Map<String, Set<String>> localElements =
        exportRequest.elements().stream()
            .filter(fhirElement -> fhirElement.resourceTypeCode() != null)
            .collect(
                Collectors.groupingBy(
                    ExportRequest.FhirElement::resourceTypeCode,
                    Collectors.mapping(
                        ExportRequest.FhirElement::elementName, Collectors.toSet())));
    final Set<String> globalElements =
        exportRequest.elements().stream()
            .filter(fhirElement -> fhirElement.resourceTypeCode() == null)
            .map(ExportRequest.FhirElement::elementName)
            .collect(Collectors.toCollection(HashSet::new));
    globalElements.add("id"); // id is globally mandatory.
    globalElements.add("id_versioned"); // id_versioned is coupled to id in spark datasets.
    globalElements.add("meta"); // meta is globally mandatory.
    final Map<String, UnaryOperator<Dataset<Row>>> localGlobalCombined =
        localElements.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> {
                      final Set<String> allElementsForThisResourceType =
                          new HashSet<>(entry.getValue());
                      allElementsForThisResourceType.addAll(
                          getMandatoryElements(
                              entry.getKey())); // Add all local mandatory elements to be returned.
                      allElementsForThisResourceType.addAll(globalElements);
                      return rowDataset ->
                          rowDataset.select(
                              columnsWithNullification(rowDataset, allElementsForThisResourceType));
                    }));
    QueryableDataSource result =
        mapped.map(
            (resourceType, rowDataset) ->
                localGlobalCombined
                    .getOrDefault(resourceType, UnaryOperator.identity())
                    .apply(rowDataset));

    // Apply global elements to all other resource types that don't have specific elements
    if (!globalElements.isEmpty()) {
      result =
          result.map(
              (resourceType, dataset) -> {
                // Only apply if this resource type wasn't already handled by bulkMap
                if (!localGlobalCombined.containsKey(resourceType)) {
                  return dataset.select(columnsWithNullification(dataset, globalElements));
                }
                return dataset;
              });
    }
    return result;
  }

  @Nonnull
  private static QueryableDataSource applyUntilDateFilter(
      @Nonnull final ExportRequest exportRequest, @Nonnull final QueryableDataSource mapped) {
    if (exportRequest.until() != null) {
      return mapped.map(
          rowDataset ->
              rowDataset.filter(
                  "meta.lastUpdated IS NULL OR meta.lastUpdated <= '"
                      + exportRequest.until().getValueAsString()
                      + "'"));
    }
    return mapped;
  }

  /**
   * Applies _typeFilter search-based filtering to the exported resources. For each resource type
   * that has filters, generates a Spark Column from each search query string and combines them with
   * OR logic. Resource types without filters are passed through unmodified.
   *
   * @param exportRequest the export request containing the type filters
   * @param dataSource the data source to filter
   * @return the filtered data source
   */
  @Nonnull
  private QueryableDataSource applyTypeFilters(
      @Nonnull final ExportRequest exportRequest, @Nonnull final QueryableDataSource dataSource) {
    if (exportRequest.typeFilters().isEmpty()) {
      return dataSource;
    }

    return dataSource.map(
        (resourceType, rowDataset) -> {
          final java.util.List<String> filters = exportRequest.typeFilters().get(resourceType);
          if (filters == null || filters.isEmpty()) {
            return rowDataset;
          }

          // Convert each search query string to a Spark Column and combine with OR.
          Column combined = null;
          for (final String searchQuery : filters) {
            final Column filterColumn = pathlingContext.searchToColumn(resourceType, searchQuery);
            combined = combined == null ? filterColumn : combined.or(filterColumn);
          }
          log.debug(
              "Applying _typeFilter for resource type {} with {} filter(s)",
              resourceType,
              filters.size());
          return rowDataset.filter(combined);
        });
  }

  @Nonnull
  private static QueryableDataSource applySinceDateFilter(
      @Nonnull final ExportRequest exportRequest, @Nonnull final QueryableDataSource mapped) {
    if (exportRequest.since() != null) {
      return mapped.map(
          rowDataset ->
              rowDataset.filter(
                  "meta.lastUpdated IS NULL OR meta.lastUpdated >= '"
                      + exportRequest.since().getValueAsString()
                      + "'"));
    }
    return mapped;
  }

  @Nonnull
  private QueryableDataSource applyResourceTypeFiltering(
      @Nonnull final ExportRequest exportRequest, @Nonnull final QueryableDataSource mapped) {
    // Assume that every resource from the _type param is accessible.
    final Map<String, Boolean> perResourceAuth =
        exportRequest.includeResourceTypeFilters().stream()
            .collect(Collectors.toMap(code -> code, code -> true));
    if (serverConfiguration.getAuth().isEnabled()) {
      // Provide actual authority access for each resource type.
      exportRequest.includeResourceTypeFilters().stream()
          .map(
              resourceTypeCode ->
                  Map.entry(
                      resourceTypeCode,
                      PathlingAuthority.resourceAccess(AccessType.READ, resourceTypeCode)))
          .forEach(
              entry -> {
                // handling=strict and auth exists -> throw error on wrong auth.
                if (!exportRequest.lenient()) {
                  SecurityAspect.checkHasAuthority(entry.getValue());
                } else {
                  perResourceAuth.put(
                      entry.getKey(), SecurityAspect.hasAuthority(entry.getValue()));
                }
              });
    }
    // Apply the perResourceAuth map.
    return mapped.filterByResourceType(
        resourceType -> {
          if (exportRequest.includeResourceTypeFilters().isEmpty()) {
            // It is ok to just pass the resources on without further auth, because at this stage,
            // the delta lake only contains resources that the user is allowed to see (it was
            // filtered earlier).
            return true;
          }
          return perResourceAuth.getOrDefault(resourceType, false)
              && exportRequest.includeResourceTypeFilters().contains(resourceType);
        });
  }

  @Nonnull
  private Column[] columnsWithNullification(
      @Nonnull final Dataset<Row> dataset, @Nonnull final Set<String> columnsToKeep) {
    return Arrays.stream(dataset.columns())
        .map(
            colName -> {
              if (columnsToKeep.contains(colName)) {
                return col(colName);
              } else {
                final DataType expectedType = dataset.schema().apply(colName).dataType();
                // For MapType, create an empty map instead of null to avoid NullPointerException
                // when Spark tries to convert null Scala Maps to Java Maps.
                if (expectedType instanceof MapType) {
                  return map().cast(expectedType).as(colName);
                } else {
                  return lit(null).cast(expectedType).as(colName);
                }
              }
            })
        .toArray(Column[]::new);
  }

  /**
   * Returns the set of mandatory elements for a given resource type.
   *
   * @param resourceTypeCode The resource type code.
   * @return The set of mandatory element names.
   */
  @Nonnull
  public Set<String> getMandatoryElements(@Nonnull final String resourceTypeCode) {
    final Set<String> alwaysMandatory = Set.of("id");

    final RuntimeResourceDefinition resourceDef =
        fhirContext.getResourceDefinition(resourceTypeCode);
    final Set<String> mandatoryElements = new HashSet<>();

    for (final BaseRuntimeChildDefinition child : resourceDef.getChildren()) {
      if (child.getMin() > 0) {
        mandatoryElements.add(child.getElementName());
      }
    }

    mandatoryElements.addAll(alwaysMandatory);
    return mandatoryElements;
  }
}
