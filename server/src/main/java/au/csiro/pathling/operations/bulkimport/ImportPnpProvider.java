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

package au.csiro.pathling.operations.bulkimport;

import static au.csiro.pathling.security.SecurityAspect.getCurrentUserId;

import au.csiro.pathling.async.AsyncJobContext;
import au.csiro.pathling.async.AsyncSupported;
import au.csiro.pathling.async.Job;
import au.csiro.pathling.async.JobRegistry;
import au.csiro.pathling.async.PreAsyncValidation;
import au.csiro.pathling.async.RequestTag;
import au.csiro.pathling.async.RequestTagFactory;
import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.security.OperationAccess;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Parameters;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

/**
 * Enables the ping and pull bulk import of data into the server, where data is fetched from a
 * remote bulk data export endpoint.
 *
 * @author John Grimes
 * @see <a href="https://github.com/smart-on-fhir/bulk-import/blob/master/import-pnp.md">Bulk Data
 *     Import - Ping and Pull Approach</a>
 */
@Component
@Slf4j
public class ImportPnpProvider implements PreAsyncValidation<ImportPnpRequest> {

  @Nonnull private final ImportPnpExecutor executor;

  @Nonnull private final ImportPnpOperationValidator validator;

  @Nonnull private final RequestTagFactory requestTagFactory;

  @Nonnull private final JobRegistry jobRegistry;

  @Nonnull private final ImportResultRegistry importResultRegistry;

  /**
   * Constructor for ImportPnpProvider.
   *
   * @param executor An {@link ImportPnpExecutor} to use in executing import requests
   * @param validator validator for ping and pull import requests
   * @param requestTagFactory factory for creating request tags
   * @param jobRegistry registry for async jobs
   * @param importResultRegistry registry for import results
   */
  public ImportPnpProvider(
      @Nonnull final ImportPnpExecutor executor,
      @Nonnull final ImportPnpOperationValidator validator,
      @Nonnull final RequestTagFactory requestTagFactory,
      @Nonnull final JobRegistry jobRegistry,
      @Nonnull final ImportResultRegistry importResultRegistry) {
    this.executor = executor;
    this.validator = validator;
    this.requestTagFactory = requestTagFactory;
    this.jobRegistry = jobRegistry;
    this.importResultRegistry = importResultRegistry;
  }

  /**
   * Ping and pull bulk import operation that fetches data from a remote bulk data export endpoint
   * and imports it into the server.
   *
   * @param parameters A FHIR {@link Parameters} object describing the import request
   * @param requestDetails the {@link ServletRequestDetails} containing HAPI inferred info
   * @return A FHIR {@link Parameters} resource describing the result
   */
  @Operation(name = "$import-pnp")
  @SuppressWarnings("UnusedReturnValue")
  @OperationAccess("import-pnp")
  @AsyncSupported
  @Nullable
  public Parameters importPnpOperation(
      @ResourceParam final Parameters parameters,
      @Nonnull final ServletRequestDetails requestDetails) {

    final Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

    // Try to get the job from the async context first. This is set by AsyncAspect when the
    // operation runs asynchronously, avoiding the need to access the servlet request which may
    // have been recycled by Tomcat.
    @SuppressWarnings("unchecked")
    final Job<ImportPnpRequest> ownJob =
        AsyncJobContext.getCurrentJob()
            .map(job -> (Job<ImportPnpRequest>) job)
            .orElseGet(
                () -> {
                  // Fallback for cases where async context is not available.
                  final PreAsyncValidationResult<ImportPnpRequest> validationResult =
                      preAsyncValidate(requestDetails, new Object[] {parameters});
                  final String operationCacheKey =
                      computeCacheKeyComponent(
                          Objects.requireNonNull(
                              validationResult.result(),
                              "Validation result should not be null for a valid request"));
                  final RequestTag ownTag =
                      requestTagFactory.createTag(
                          requestDetails, authentication, operationCacheKey);
                  return jobRegistry.get(ownTag);
                });

    if (ownJob == null) {
      throw new InvalidRequestException("Missing 'Prefer: respond-async' header value.");
    }

    // Check that the user requesting the result is the same user that started the job.
    final Optional<String> currentUserId = getCurrentUserId(authentication);
    if (currentUserId.isPresent() && !ownJob.getOwnerId().equals(currentUserId)) {
      throw new AccessDeniedError(
          "The requested result is not owned by the current user '%s'."
              .formatted(currentUserId.orElse("null")));
    }

    final ImportPnpRequest importPnpRequest = ownJob.getPreAsyncValidationResult();
    if (ownJob.isCancelled()) {
      return null;
    }

    importResultRegistry.put(ownJob.getId(), new ImportResult(ownJob.getOwnerId()));

    final ImportResponse importResponse = executor.execute(importPnpRequest, ownJob.getId());

    return importResponse.toOutput();
  }

  @Override
  @Nonnull
  public PreAsyncValidationResult<ImportPnpRequest> preAsyncValidate(
      @Nonnull final ServletRequestDetails servletRequestDetails, @Nonnull final Object[] params)
      throws InvalidRequestException {
    return validator.validateParametersRequest(servletRequestDetails, (Parameters) params[0]);
  }

  @Override
  @Nonnull
  public String computeCacheKeyComponent(@Nonnull final ImportPnpRequest request) {
    // Build a deterministic cache key from request parameters.
    // Exclude originalRequest as it's already captured in the request URL.
    final StringBuilder key = new StringBuilder();
    key.append("exportUrl=").append(request.exportUrl());
    key.append("|exportType=").append(request.exportType());
    key.append("|saveMode=").append(request.saveMode());
    key.append("|format=").append(request.importFormat());
    // Include Bulk Data Export passthrough parameters in the cache key.
    if (!request.types().isEmpty()) {
      key.append("|types=").append(String.join(",", request.types()));
    }
    request.since().ifPresent(since -> key.append("|since=").append(since));
    request.until().ifPresent(until -> key.append("|until=").append(until));
    request.outputFormat().ifPresent(format -> key.append("|outputFormat=").append(format));
    if (!request.elements().isEmpty()) {
      key.append("|elements=").append(String.join(",", request.elements()));
    }
    if (!request.typeFilters().isEmpty()) {
      key.append("|typeFilters=").append(String.join(",", request.typeFilters()));
    }
    if (!request.includeAssociatedData().isEmpty()) {
      key.append("|includeAssociatedData=")
          .append(String.join(",", request.includeAssociatedData()));
    }
    return key.toString();
  }
}
