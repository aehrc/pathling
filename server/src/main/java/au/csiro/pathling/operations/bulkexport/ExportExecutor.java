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

import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.io.JobDirectoryFileSystem;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.sink.DataSinkBuilder;
import au.csiro.pathling.library.io.sink.WriteDetails;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.operations.bulkexport.ExportRequest.ExportLevel;
import au.csiro.pathling.operations.compartment.PatientCompartmentService;
import au.csiro.pathling.operations.export.LastUpdatedFilter;
import au.csiro.pathling.security.PathlingAuthority;
import au.csiro.pathling.security.ResourceAccess.AccessType;
import au.csiro.pathling.security.SecurityAspect;
import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Column;
import org.hl7.fhir.r4.model.InstantType;
import org.springframework.beans.factory.annotation.Autowired;
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

  @Nonnull private final JobDirectoryFileSystem jobDirectoryFileSystem;

  @Nonnull private final ServerConfiguration serverConfiguration;

  @Nonnull private final PatientCompartmentService patientCompartmentService;

  /**
   * Constructs a new ExportExecutor.
   *
   * @param pathlingContext The Pathling context.
   * @param deltaLake The queryable data source.
   * @param fhirContext The FHIR context.
   * @param jobDirectoryFileSystem The shared helper that creates per-job directories on the
   *     warehouse filesystem.
   * @param serverConfiguration The server configuration.
   * @param patientCompartmentService The patient compartment service.
   */
  @Autowired
  public ExportExecutor(
      @Nonnull final PathlingContext pathlingContext,
      @Nonnull final QueryableDataSource deltaLake,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final JobDirectoryFileSystem jobDirectoryFileSystem,
      @Nonnull final ServerConfiguration serverConfiguration,
      @Nonnull final PatientCompartmentService patientCompartmentService) {
    this.pathlingContext = pathlingContext;
    this.deltaLake = deltaLake;
    this.fhirContext = fhirContext;
    this.jobDirectoryFileSystem = jobDirectoryFileSystem;
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
      // Resources limited by the _elements param carry the SUBSETTED tag.
      mapped = applyElementsParams(exportRequest, mapped);
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
    final Path jobDirPath;
    try {
      // The helper resolves the filesystem from the warehouse URI, so directory creation works on
      // any warehouse scheme, not just the process default filesystem.
      jobDirectoryFileSystem.ensureJobDirectory(jobId);
      jobDirPath = jobDirectoryFileSystem.jobDirectory(jobId);
    } catch (final IOException e) {
      throw new InternalErrorException(
          "Failed to create job directory for job %s.".formatted(jobId), e);
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
  }

  /**
   * Restricts each resource type to the elements requested through {@code _elements}, and tags the
   * resources as subsetted. A type with elements requested specifically for it keeps those, its
   * mandatory elements and the elements requested for all types; any other type keeps the elements
   * requested for all types.
   */
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
    globalElements.add("meta"); // meta is globally mandatory.
    return mapped.map(
        (resourceType, rowDataset) -> {
          final Set<String> elements = new HashSet<>(globalElements);
          Optional.ofNullable(localElements.get(resourceType))
              .ifPresent(
                  local -> {
                    elements.addAll(local);
                    elements.addAll(getMandatoryElements(resourceType));
                  });
          return ElementsSubsetting.subset(pathlingContext, resourceType, rowDataset, elements);
        });
  }

  @Nonnull
  private static QueryableDataSource applyUntilDateFilter(
      @Nonnull final ExportRequest exportRequest, @Nonnull final QueryableDataSource mapped) {
    final InstantType until = exportRequest.until();
    return until == null
        ? mapped
        : mapped.map(rowDataset -> LastUpdatedFilter.until(rowDataset, until));
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
    final InstantType since = exportRequest.since();
    return since == null
        ? mapped
        : mapped.map(rowDataset -> LastUpdatedFilter.since(rowDataset, since));
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
