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

package au.csiro.pathling.operations.sqlquery;

import au.csiro.pathling.operations.view.ViewExportFormat;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Set;
import org.hl7.fhir.r4.model.InstantType;

/**
 * The parsed and validated kick-off request for a {@code $sqlquery-export} operation, produced by
 * {@link SqlQueryExportRequestParser#parse} and carried to background execution via the job. Each
 * {@link QueryInput} is already parsed, parameter bound, and view resolved; view sources are folded
 * into the per-query resolved views and produce no outputs of their own.
 *
 * @param originalRequest the original request URL
 * @param serverBaseUrl the FHIR server base URL (used for constructing result/download URLs)
 * @param queries the ordered list of queries; one output per query
 * @param clientTrackingId optional client-provided tracking identifier, echoed when present
 * @param format the output format (NDJSON, CSV, or Parquet)
 * @param includeHeader whether to include a header row in CSV output
 * @param patientIds patient ids to filter by (from {@code patient} and {@code group} parameters)
 * @param since filter resources modified after this timestamp
 * @author John Grimes
 */
public record SqlQueryExportRequest(
    @Nonnull String originalRequest,
    @Nonnull String serverBaseUrl,
    @Nonnull List<QueryInput> queries,
    @Nullable String clientTrackingId,
    @Nonnull ViewExportFormat format,
    boolean includeHeader,
    @Nonnull Set<String> patientIds,
    @Nullable InstantType since) {}
