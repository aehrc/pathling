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

package au.csiro.pathling.operations.sql;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Set;
import org.hl7.fhir.r4.model.InstantType;

/**
 * The parsed and validated kick-off request for an {@code $sql-export} operation, produced by
 * {@link SqlExportRequestParser} and carried to background execution via the job.
 *
 * <p>Every subject is already resolved, name-assigned and prepared, so background execution can
 * fail only on the data itself. Context entries are folded into the prepared subjects and produce
 * no outputs of their own.
 *
 * @param originalRequest the original request URL
 * @param serverBaseUrl the FHIR server base URL, used to construct result and download URLs
 * @param subjects the subjects, in request order; one output per subject
 * @param clientTrackingId the client-supplied tracking identifier, echoed when present
 * @param format the output format
 * @param includeHeader whether CSV output carries a header row
 * @param patientIds patient ids to filter by, resolved from {@code patient} and {@code group}
 * @param since restricts to resources updated at or after this instant
 * @author John Grimes
 */
public record SqlExportRequest(
    @Nonnull String originalRequest,
    @Nonnull String serverBaseUrl,
    @Nonnull List<SubjectInput> subjects,
    @Nullable String clientTrackingId,
    @Nonnull SqlExportFormat format,
    boolean includeHeader,
    @Nonnull Set<String> patientIds,
    @Nullable InstantType since) {}
