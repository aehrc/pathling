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

package au.csiro.pathling.operations.compartment;

import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluator;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluatorBuilder;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.io.source.DataSource;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import ca.uhn.fhir.context.RuntimeSearchParam;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Service for determining patient compartment membership and building filters for patient-level
 * bulk export operations.
 *
 * <p>This service uses HAPI FHIR's compartment definition metadata to discover which element paths
 * link a resource type to the Patient compartment. For non-trivial paths (those containing FHIRPath
 * expressions like {@code where()} or {@code resolve()}), it uses FHIRPath evaluation to correctly
 * extract the reference values.
 *
 * @author John Grimes
 */
@Slf4j
@Component
public class PatientCompartmentService {

  /** The Patient resource type code. */
  private static final String PATIENT_RESOURCE_TYPE = "Patient";

  /** Prefix for patient references. */
  private static final String PATIENT_REF_PREFIX = PATIENT_RESOURCE_TYPE + "/";

  @Nonnull private final FhirContext fhirContext;

  /** Cache of resource type to compartment element paths. */
  @Nonnull private final Map<String, List<String>> compartmentPathCache = new ConcurrentHashMap<>();

  /** Parser for FHIRPath expressions. */
  @Nonnull private final Parser parser;

  /**
   * Constructs a new PatientCompartmentService.
   *
   * @param fhirContext the FHIR context
   */
  @Autowired
  public PatientCompartmentService(@Nonnull final FhirContext fhirContext) {
    this.fhirContext = fhirContext;
    this.parser = new Parser();
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
   * Build a Spark filter Column for compartment membership. This method is for simple cases where
   * the filter doesn't require FHIRPath evaluation (e.g., Patient resources).
   *
   * <p>For non-Patient resources that may have complex FHIRPath expressions in their compartment
   * paths, use {@link #filterByPatientCompartment} instead.
   *
   * @param resourceType the resource type
   * @param patientIds the patient IDs to filter by (empty set means all patients in compartment)
   * @return Spark Column for filtering
   */
  @Nonnull
  public Column buildPatientFilter(
      @Nonnull final String resourceType, @Nonnull final Set<String> patientIds) {
    // Handle Patient resource specially - no FHIRPath evaluation needed.
    if (PATIENT_RESOURCE_TYPE.equals(resourceType)) {
      if (patientIds.isEmpty()) {
        // All patients - no filter needed.
        return lit(true);
      } else {
        // Filter to specific patient IDs.
        return col("id").isin(patientIds.toArray());
      }
    }

    // For non-Patient resources without DataSource, we can't evaluate FHIRPath properly.
    // Return a filter that matches nothing (this shouldn't normally be called for such cases).
    final List<String> paths = getPatientCompartmentPaths(resourceType);
    if (paths.isEmpty()) {
      return lit(false);
    }

    // Fall back to simple column-based filtering for backward compatibility.
    // This will only work for simple paths without FHIRPath functions.
    log.warn(
        "buildPatientFilter called without DataSource for resource type {} - "
            + "FHIRPath expressions in compartment paths will not be evaluated correctly",
        resourceType);
    return buildSimpleFilter(paths, patientIds);
  }

  /**
   * Filter a dataset to only include resources in the Patient compartment. This method uses a
   * semi-join approach that avoids collecting IDs into driver memory, making it suitable for large
   * datasets.
   *
   * <p>For complex compartment paths that contain FHIRPath expressions like {@code where(resolve()
   * is Patient)}, this method uses FHIRPath evaluation to correctly identify matching resources and
   * then performs a semi-join to filter the input dataset.
   *
   * @param resourceType the resource type
   * @param patientIds the patient IDs to filter by (empty set means all patients in compartment)
   * @param rowDataset the dataset to filter (must have flat column structure with 'id' column)
   * @param dataSource the data source for FHIRPath evaluation
   * @return the filtered dataset containing only resources in the Patient compartment
   */
  @Nonnull
  public org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> filterByPatientCompartment(
      @Nonnull final String resourceType,
      @Nonnull final Set<String> patientIds,
      @Nonnull final Dataset<Row> rowDataset,
      @Nonnull final DataSource dataSource) {
    // Handle Patient resource specially - no FHIRPath evaluation needed.
    if (PATIENT_RESOURCE_TYPE.equals(resourceType)) {
      if (patientIds.isEmpty()) {
        // All patients - return the dataset unchanged.
        return rowDataset;
      } else {
        // Filter to specific patient IDs using a simple column filter.
        return rowDataset.filter(col("id").isin(patientIds.toArray()));
      }
    }

    final List<String> paths = getPatientCompartmentPaths(resourceType);
    if (paths.isEmpty()) {
      // Not in compartment - return empty dataset.
      return rowDataset.filter(lit(false));
    }

    // Read the resource data from the data source.
    final org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> resourceDataset =
        dataSource.read(resourceType);

    // Create a FHIRPath evaluator for this resource type.
    final DatasetEvaluator evaluator =
        DatasetEvaluatorBuilder.create(resourceType, fhirContext)
            .withDataset(resourceDataset)
            .build();

    // Get the input context for FHIRPath evaluation.
    final ResourceCollection inputContext = evaluator.getDefaultInputContext();

    // Build OR filter across all compartment paths using FHIRPath evaluation.
    Column filter = lit(false);
    for (final String path : paths) {
      final Column pathFilter = buildPathFilter(path, patientIds, evaluator, inputContext);
      if (pathFilter != null) {
        filter = filter.or(pathFilter);
      }
    }

    // Coalesce to handle null values (treat null as false).
    final Column safeFilter = coalesce(filter, lit(false));

    // Apply the filter to the evaluator's dataset and extract matching IDs.
    final org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> evaluatorDataset =
        evaluator.getDataset();
    final org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> matchingIds =
        evaluatorDataset.filter(safeFilter).select(evaluatorDataset.col("id"));

    // Use a left semi-join to filter the row dataset efficiently without collecting IDs.
    // This preserves all columns from rowDataset while only keeping rows with matching IDs.
    return rowDataset.join(
        matchingIds, rowDataset.col("id").equalTo(matchingIds.col("id")), "left_semi");
  }

  /**
   * Build a filter column for a single compartment path using FHIRPath evaluation.
   *
   * <p>This method evaluates a boolean FHIRPath expression that checks if any reference in the path
   * matches the patient filter criteria. For the "all patients" case, it uses {@code
   * where(resolve() is Patient)} to filter to Patient references. The {@code resolve() is Patient}
   * check extracts the resource type from the reference string without requiring actual resource
   * resolution.
   *
   * @param path the compartment path (may contain FHIRPath expressions)
   * @param patientIds the patient IDs to filter by
   * @param evaluator the FHIRPath evaluator
   * @param inputContext the input context for evaluation
   * @return the filter column, or null if the path couldn't be evaluated
   */
  @Nullable
  private Column buildPathFilter(
      @Nonnull final String path,
      @Nonnull final Set<String> patientIds,
      @Nonnull final DatasetEvaluator evaluator,
      @Nonnull final ResourceCollection inputContext) {
    try {
      // Build a boolean FHIRPath expression that checks if any reference matches.
      final String expression;
      if (patientIds.isEmpty()) {
        // All patients: filter to Patient references and check if any exist.
        // Use where(resolve() is Patient) to filter to references that point to Patient resources.
        // The resolve() function extracts the resource type from the reference string.
        expression = path + ".where(resolve() is Patient).reference.exists()";
      } else {
        // Specific patients: filter to Patient references and check for matching IDs.
        // Build an expression that checks equality for each patient ID.
        expression =
            patientIds.stream()
                .map(
                    id ->
                        path
                            + ".where(resolve() is Patient).reference = '"
                            + PATIENT_REF_PREFIX
                            + id
                            + "'")
                .reduce((a, b) -> "(" + a + " or " + b + ")")
                .orElse("false");
      }

      final FhirPath fhirPath = parser.parse(expression);

      // Evaluate the FHIRPath expression - this returns a boolean collection.
      final Collection result = evaluator.evaluateToCollection(fhirPath, inputContext);

      // Get the boolean column.
      final Column boolColumn = result.getColumn().getValue();

      // Use coalesce to handle null values gracefully.
      return coalesce(boolColumn, lit(false));
    } catch (final Exception e) {
      log.warn("Failed to evaluate FHIRPath for compartment path '{}': {}", path, e.getMessage());
      return null;
    }
  }

  /**
   * Build a simple column-based filter without FHIRPath evaluation. This is used for backward
   * compatibility when no DataSource is provided.
   *
   * @param paths the compartment paths to filter on
   * @param patientIds the patient IDs to filter by
   * @return a Spark Column filter expression
   */
  @Nonnull
  private Column buildSimpleFilter(
      @Nonnull final List<String> paths, @Nonnull final Set<String> patientIds) {
    Column filter = lit(false);
    for (final String path : paths) {
      final String refColumn = path + ".reference";
      if (patientIds.isEmpty()) {
        filter = filter.or(col(refColumn).startsWith(PATIENT_REF_PREFIX));
      } else {
        final String[] patientRefs =
            patientIds.stream().map(id -> PATIENT_REF_PREFIX + id).toArray(String[]::new);
        filter = filter.or(col(refColumn).isin((Object[]) patientRefs));
      }
    }
    return filter;
  }

  /**
   * Discover compartment paths for a resource type using HAPI FHIR's compartment definition
   * metadata.
   *
   * @param resourceType the FHIR resource type code
   * @return list of compartment paths for the resource type
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
