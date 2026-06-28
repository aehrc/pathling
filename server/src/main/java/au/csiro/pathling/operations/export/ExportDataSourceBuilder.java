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

package au.csiro.pathling.operations.export;

import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.operations.compartment.PatientCompartmentService;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Set;
import org.hl7.fhir.r4.model.InstantType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Builds the filtered data source for an asynchronous export: applies the {@code _since}
 * (updated-since) filter and the patient-compartment filter derived from the {@code patient} and
 * {@code group} parameters. Shared by both {@code $viewdefinition-export} and {@code
 * $sqlquery-export} so the two operations scope exported rows identically.
 *
 * @author John Grimes
 */
@Component
public class ExportDataSourceBuilder {

  @Nonnull private final PatientCompartmentService patientCompartmentService;

  /**
   * Constructs a new ExportDataSourceBuilder.
   *
   * @param patientCompartmentService the patient compartment service used for row-level filtering
   */
  @Autowired
  public ExportDataSourceBuilder(
      @Nonnull final PatientCompartmentService patientCompartmentService) {
    this.patientCompartmentService = patientCompartmentService;
  }

  /**
   * Applies the export filters to the base data source.
   *
   * @param base the unfiltered data source
   * @param since the {@code _since} filter, or null for no time filter
   * @param patientIds the patient ids (resolved from {@code patient} and {@code group}); empty for
   *     no compartment filter
   * @return the filtered data source
   */
  @Nonnull
  public QueryableDataSource build(
      @Nonnull final QueryableDataSource base,
      @Nullable final InstantType since,
      @Nonnull final Set<String> patientIds) {
    QueryableDataSource dataSource = base;

    // Apply the _since filter.
    if (since != null) {
      dataSource =
          dataSource.map(
              rowDataset ->
                  rowDataset.filter(
                      "meta.lastUpdated IS NULL OR meta.lastUpdated >= '"
                          + since.getValueAsString()
                          + "'"));
    }

    // Apply the patient compartment filter if patient ids were specified.
    if (!patientIds.isEmpty()) {
      dataSource = applyPatientCompartmentFilter(dataSource, patientIds);
    }

    return dataSource;
  }

  /**
   * Applies the patient compartment filter to the data source. Uses the FHIRPath-aware compartment
   * filter so that non-Patient resource types whose compartment membership is defined by a FHIRPath
   * expression (for example {@code Observation.subject.where(resolve() is Patient)}) are filtered
   * correctly, not just resources with a flat reference column.
   */
  @Nonnull
  private QueryableDataSource applyPatientCompartmentFilter(
      @Nonnull final QueryableDataSource base, @Nonnull final Set<String> patientIds) {

    // Filter out resource types that are not in the Patient compartment.
    final QueryableDataSource filtered =
        base.filterByResourceType(patientCompartmentService::isInPatientCompartment);

    // Apply row-level filtering based on patient compartment membership, evaluating any FHIRPath
    // compartment paths against the unfiltered source.
    return filtered.map(
        (resourceType, rowDataset) ->
            patientCompartmentService.filterByPatientCompartment(
                resourceType, patientIds, rowDataset, base));
  }
}
