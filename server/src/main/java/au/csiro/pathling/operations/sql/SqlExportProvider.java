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
 * Provider for the system-level {@code $sql-export} operation from the SQL on FHIR specification:
 * the asynchronous export of many subjects, of any mixture of kinds, against one snapshot of the
 * data.
 *
 * <p>The declared parameters exist so that HAPI advertises the operation correctly and accepts the
 * body; the request itself is parsed from the raw {@code Parameters} by {@link
 * SqlExportRequestParser}, because the {@code subject} repetitions are complex parts that HAPI's
 * parameter binding cannot express.
 *
 * @author John Grimes
 * @see <a href="http://hl7.org/fhir/uv/sql-on-fhir/OperationDefinition/SQLExport">SQLExport</a>
 */
@Slf4j
@Component
public class SqlExportProvider implements PreAsyncValidation<SqlExportRequest> {

  @Nonnull private final SqlExportRequestParser requestParser;

  @Nonnull private final SqlExportSupport support;

  /**
   * Constructs a new SqlExportProvider.
   *
   * @param requestParser parses and validates the kick-off request
   * @param support the asynchronous job machinery
   */
  @Autowired
  public SqlExportProvider(
      @Nonnull final SqlExportRequestParser requestParser,
      @Nonnull final SqlExportSupport support) {
    this.requestParser = requestParser;
    this.support = support;
  }

  /**
   * Handles the {@code $sql-export} operation.
   *
   * @param clientTrackingId a client-supplied tracking identifier, echoed in the manifest
   * @param format the output format
   * @param includeHeader whether CSV output carries a header row
   * @param patient patient references restricting the resources fed to every subject
   * @param group group references restricting the resources fed to every subject
   * @param since restricts to resources updated at or after this instant
   * @param source the unsupported external data source parameter, rejected when supplied
   * @param requestDetails the servlet request details
   * @return the completion manifest, or null when the job was cancelled
   */
  @SuppressWarnings({"unused", "java:S107"})
  @Operation(name = "$sql-export", idempotent = true)
  @OperationAccess("sql-export")
  @AsyncSupported(pattern = AsyncPattern.STANDARD_ASYNC_PATTERN)
  @Nullable
  public Parameters export(
      @Nullable @OperationParam(name = "clientTrackingId") final String clientTrackingId,
      @Nullable @OperationParam(name = "_format") final String format,
      @Nullable @OperationParam(name = "header") final BooleanType includeHeader,
      @Nullable @OperationParam(name = "patient", max = OperationParam.MAX_UNLIMITED)
          final List<Reference> patient,
      @Nullable @OperationParam(name = "group", max = OperationParam.MAX_UNLIMITED)
          final List<Reference> group,
      @Nullable @OperationParam(name = "_since") final InstantType since,
      @Nullable @OperationParam(name = "source") final String source,
      @Nonnull final ServletRequestDetails requestDetails) {
    return support.runExport(requestDetails, this);
  }

  @Override
  @Nonnull
  public PreAsyncValidationResult<SqlExportRequest> preAsyncValidate(
      @Nonnull final ServletRequestDetails servletRequestDetails, @Nonnull final Object[] params)
      throws InvalidRequestException {
    return new PreAsyncValidationResult<>(
        requestParser.parse(servletRequestDetails), Collections.emptyList());
  }

  @Override
  @Nonnull
  public String computeCacheKeyComponent(@Nonnull final SqlExportRequest request) {
    return support.computeCacheKeyComponent(request);
  }
}
