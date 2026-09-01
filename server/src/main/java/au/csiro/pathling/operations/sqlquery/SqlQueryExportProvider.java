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

import au.csiro.pathling.async.AsyncPattern;
import au.csiro.pathling.async.AsyncSupported;
import au.csiro.pathling.async.PreAsyncValidation;
import au.csiro.pathling.security.OperationAccess;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Reference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Provider for the system-level {@code $sqlquery-export} operation from the SQL on FHIR
 * specification: the asynchronous counterpart to {@code $sqlquery-run}. Runs one or more SQL
 * queries against materialised ViewDefinition tables in the background and exports each result to
 * downloadable files. The level-agnostic machinery is shared via {@link SqlQueryExportSupport}.
 *
 * @author John Grimes
 * @see <a href="http://sql-on-fhir.org/OperationDefinition/$sqlquery-export">$sqlquery-export</a>
 * @see SqlQueryInstanceExportProvider for the type-level and instance-level operations
 */
@Slf4j
@Component
public class SqlQueryExportProvider implements PreAsyncValidation<SqlQueryExportRequest> {

  @Nonnull private final SqlQueryExportRequestParser requestParser;

  @Nonnull private final SqlQueryExportSupport support;

  /**
   * Constructs a new SqlQueryExportProvider.
   *
   * @param requestParser parses and validates the kick-off request
   * @param support the shared export machinery (job resolution, execution, manifest, cache key)
   */
  @Autowired
  public SqlQueryExportProvider(
      @Nonnull final SqlQueryExportRequestParser requestParser,
      @Nonnull final SqlQueryExportSupport support) {
    this.requestParser = requestParser;
    this.support = support;
  }

  /**
   * Handles the {@code $sqlquery-export} operation at the system level.
   *
   * @param clientTrackingId optional client-provided tracking identifier
   * @param format the output format (ndjson, csv, parquet)
   * @param includeHeader whether to include headers in CSV output
   * @param patientIds patient ids to filter by
   * @param groupIds group ids to filter by
   * @param since filter resources modified after this timestamp
   * @param source the unsupported external data source parameter, rejected when supplied
   * @param requestDetails the request details
   * @return the completion manifest, or null if cancelled
   */
  @SuppressWarnings({"unused", "java:S107"})
  @Operation(name = "$sqlquery-export", idempotent = true)
  @OperationAccess("sqlquery-export")
  @AsyncSupported(pattern = AsyncPattern.STANDARD_ASYNC_PATTERN)
  @Nullable
  public Parameters export(
      @Nullable @OperationParam(name = "clientTrackingId") final String clientTrackingId,
      @Nullable @OperationParam(name = "_format") final String format,
      @Nullable @OperationParam(name = "header") final BooleanType includeHeader,
      @Nullable @OperationParam(name = "patient") final List<Reference> patientIds,
      @Nullable @OperationParam(name = "group") final List<Reference> groupIds,
      @Nullable @OperationParam(name = "_since") final InstantType since,
      @Nullable @OperationParam(name = "source") final String source,
      @Nonnull final ServletRequestDetails requestDetails) {
    return support.runExport(requestDetails, this);
  }

  @Override
  @Nonnull
  public PreAsyncValidationResult<SqlQueryExportRequest> preAsyncValidate(
      @Nonnull final ServletRequestDetails servletRequestDetails, @Nonnull final Object[] params)
      throws InvalidRequestException {
    final SqlQueryExportRequest request =
        requestParser.parse(
            servletRequestDetails,
            /* boundLibrary= */ null,
            support.stringParam(servletRequestDetails, "_format"),
            support.headerParam(servletRequestDetails),
            support.stringParam(servletRequestDetails, "clientTrackingId"),
            support.collectPatientIds(servletRequestDetails),
            support.sinceParam(servletRequestDetails),
            support.stringParam(servletRequestDetails, "source"));
    return new PreAsyncValidationResult<>(request, Collections.emptyList());
  }

  @Override
  @Nonnull
  public String computeCacheKeyComponent(@Nonnull final SqlQueryExportRequest request) {
    return support.computeCacheKeyComponent(request);
  }
}
