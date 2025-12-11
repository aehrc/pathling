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

package au.csiro.pathling.operations.viewexport;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Set;
import org.hl7.fhir.r4.model.InstantType;

/**
 * Parsed and validated request data for the ViewDefinition export operation.
 *
 * @param originalRequest the original request URL
 * @param serverBaseUrl the FHIR server base URL (without trailing slash)
 * @param views the list of views to export
 * @param clientTrackingId optional client-provided tracking identifier
 * @param format the output format (NDJSON, CSV, or Parquet)
 * @param includeHeader whether to include headers in CSV output
 * @param patientIds patient IDs to filter by (from patient and group parameters)
 * @param since filter resources modified after this timestamp
 * @author John Grimes
 */
public record ViewDefinitionExportRequest(
    @Nonnull String originalRequest,
    @Nonnull String serverBaseUrl,
    @Nonnull List<ViewInput> views,
    @Nullable String clientTrackingId,
    @Nonnull ViewExportFormat format,
    boolean includeHeader,
    @Nonnull Set<String> patientIds,
    @Nullable InstantType since
) {

}
