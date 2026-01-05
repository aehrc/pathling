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

package au.csiro.pathling.operations.compartment;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import ca.uhn.fhir.context.RuntimeSearchParam;
import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Service for determining patient compartment membership and building filters for patient-level
 * bulk export operations.
 *
 * <p>This service uses HAPI FHIR's compartment definition metadata to discover which element paths
 * link a resource type to the Patient compartment.
 *
 * @author John Grimes
 */
@Slf4j
@Component
public class PatientCompartmentService {

  /** The Patient resource type code. */
  private static final String PATIENT_RESOURCE_TYPE = "Patient";

  @Nonnull private final FhirContext fhirContext;

  /** Cache of resource type to compartment element paths. */
  @Nonnull private final Map<String, List<String>> compartmentPathCache = new ConcurrentHashMap<>();

  /**
   * Constructs a new PatientCompartmentService.
   *
   * @param fhirContext the FHIR context
   */
  @Autowired
  public PatientCompartmentService(@Nonnull final FhirContext fhirContext) {
    this.fhirContext = fhirContext;
  }

  /**
   * Get the element paths that link a resource type to the Patient compartment. Based on the FHIR
   * Patient compartment definition.
   *
   * @param resourceType the FHIR resource type code (e.g., "Observation")
   * @return list of element paths that reference Patient (e.g., ["subject", "performer"])
   * @see <a href="https://hl7.org/fhir/R4/compartmentdefinition-patient.html">Patient
   *     Compartment</a>
   */
  @Nonnull
  public List<String> getPatientCompartmentPaths(@Nonnull final String resourceType) {
    return compartmentPathCache.computeIfAbsent(resourceType, this::discoverCompartmentPaths);
  }

  /**
   * Check if a resource type is part of the Patient compartment.
   *
   * @param resourceType the FHIR resource type code
   * @return true if the resource type is in the Patient compartment
   */
  public boolean isInPatientCompartment(@Nonnull final String resourceType) {
    // Patient is always in its own compartment.
    return PATIENT_RESOURCE_TYPE.equals(resourceType)
        || !getPatientCompartmentPaths(resourceType).isEmpty();
  }

  /**
   * Build a Spark filter Column for compartment membership. This filter matches resources that
   * reference any of the specified patient IDs through any of the compartment paths.
   *
   * @param resourceType the resource type
   * @param patientIds the patient IDs to filter by (empty set means all patients in compartment)
   * @return Spark Column for filtering, or null if no filter is needed
   */
  @Nonnull
  public Column buildPatientFilter(
      @Nonnull final String resourceType, @Nonnull final Set<String> patientIds) {
    // Handle Patient resource specially.
    if (PATIENT_RESOURCE_TYPE.equals(resourceType)) {
      if (patientIds.isEmpty()) {
        // All patients - no filter needed.
        return lit(true);
      } else {
        // Filter to specific patient IDs.
        return col("id").isin(patientIds.toArray());
      }
    }

    final List<String> paths = getPatientCompartmentPaths(resourceType);
    if (paths.isEmpty()) {
      // Not in compartment - filter out all rows.
      return lit(false);
    }

    // Build OR filter across all compartment paths.
    final String patientRefPrefix = PATIENT_RESOURCE_TYPE + "/";
    Column filter = lit(false);
    for (final String path : paths) {
      final String refColumn = path + ".reference";
      if (patientIds.isEmpty()) {
        // All patients: match any Patient reference.
        filter = filter.or(col(refColumn).startsWith(patientRefPrefix));
      } else {
        // Specific patients: match exact references.
        final String[] patientRefs =
            patientIds.stream().map(id -> patientRefPrefix + id).toArray(String[]::new);
        filter = filter.or(col(refColumn).isin((Object[]) patientRefs));
      }
    }
    return filter;
  }

  /**
   * Discover compartment paths for a resource type using HAPI FHIR's compartment definition
   * metadata.
   */
  @Nonnull
  private List<String> discoverCompartmentPaths(@Nonnull final String resourceType) {
    try {
      final RuntimeResourceDefinition resourceDef = fhirContext.getResourceDefinition(resourceType);
      final List<RuntimeSearchParam> searchParams =
          resourceDef.getSearchParamsForCompartmentName(PATIENT_RESOURCE_TYPE);

      final List<String> paths =
          searchParams.stream()
              .map(RuntimeSearchParam::getPath)
              .filter(path -> path != null && !path.isEmpty())
              // Paths may be pipe-separated (e.g., "Observation.subject | Observation.performer").
              .flatMap(path -> Arrays.stream(path.split("\\|")))
              .map(String::trim)
              // Remove the resource type prefix (e.g., "Observation.subject" -> "subject").
              .map(
                  path ->
                      path.startsWith(resourceType + ".")
                          ? path.substring(resourceType.length() + 1)
                          : path)
              .filter(path -> !path.isEmpty())
              .distinct()
              .toList();

      log.debug("Discovered Patient compartment paths for {}: {}", resourceType, paths);
      return paths;
    } catch (final Exception e) {
      log.warn(
          "Failed to discover Patient compartment paths for {}: {}", resourceType, e.getMessage());
      return List.of();
    }
  }
}
