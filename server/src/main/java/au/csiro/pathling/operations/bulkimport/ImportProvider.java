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

package au.csiro.pathling.operations.bulkimport;

import static au.csiro.pathling.security.SecurityAspect.getCurrentUserId;

import au.csiro.pathling.async.AsyncSupported;
import au.csiro.pathling.async.Job;
import au.csiro.pathling.async.JobRegistry;
import au.csiro.pathling.async.PreAsyncValidation;
import au.csiro.pathling.async.RequestTag;
import au.csiro.pathling.async.RequestTagFactory;
import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.security.OperationAccess;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Parameters;
import org.springframework.context.annotation.Profile;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

/**
 * Enables the bulk import of data into the server, supporting both FHIR Parameters and JSON (SMART
 * Bulk Data Import) request formats.
 *
 * @author John Grimes
 */
@Component
@Profile("server")
@Slf4j
public class ImportProvider implements PreAsyncValidation<ImportRequest> {

  @Nonnull
  private final ImportExecutor executor;
  
  @Nonnull
  private final ImportOperationValidator importOperationValidator;
  
  @Nonnull
  private final RequestTagFactory requestTagFactory;
  
  @Nonnull
  private final JobRegistry jobRegistry;
  
  @Nonnull
  private final ImportResultRegistry importResultRegistry;
  
  @Nonnull
  private final ObjectMapper objectMapper;

  /**
   * @param executor An {@link ImportExecutor} to use in executing import requests
   * @param importOperationValidator validator for import requests
   * @param requestTagFactory factory for creating request tags
   * @param jobRegistry registry for async jobs
   * @param importResultRegistry registry for import results
   */
  public ImportProvider(@Nonnull final ImportExecutor executor,
      @Nonnull final ImportOperationValidator importOperationValidator,
      @Nonnull final RequestTagFactory requestTagFactory,
      @Nonnull final JobRegistry jobRegistry,
      @Nonnull final ImportResultRegistry importResultRegistry) {
    this.executor = executor;
    this.importOperationValidator = importOperationValidator;
    this.requestTagFactory = requestTagFactory;
    this.jobRegistry = jobRegistry;
    this.importResultRegistry = importResultRegistry;
    this.objectMapper = new ObjectMapper();
  }

  /**
   * Bulk import operation supporting both FHIR Parameters (application/fhir+json) and JSON
   * (application/json) request formats aligned with the SMART Bulk Data Import specification.
   *
   * @param parameters A FHIR {@link Parameters} object describing the import request (when using
   * application/fhir+json)
   * @param requestDetails the {@link ServletRequestDetails} containing HAPI inferred info
   * @return A FHIR {@link Parameters} resource describing the result
   */
  @Operation(name = "$import")
  @SuppressWarnings("UnusedReturnValue")
  @OperationAccess("import")
  @AsyncSupported
  @Nullable
  public Parameters importOperation(@ResourceParam final Parameters parameters,
      @Nonnull final ServletRequestDetails requestDetails) {

    final Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
    final RequestTag ownTag = requestTagFactory.createTag(requestDetails, authentication);
    final Job<ImportRequest> ownJob = jobRegistry.get(ownTag);
    if (ownJob == null) {
      throw new InvalidRequestException("Missing 'Prefer: respond-async' header value.");
    }
    // Check that the user requesting the result is the same user that started the job.
    final Optional<String> currentUserId = getCurrentUserId(authentication);
    if (currentUserId.isPresent() && !ownJob.getOwnerId().equals(currentUserId)) {
      throw new AccessDeniedError(
          "The requested result is not owned by the current user '%s'.".formatted(
              currentUserId.orElse("null")));
    }

    final ImportRequest importRequest = ownJob.getPreAsyncValidationResult();
    if (ownJob.isCancelled()) {
      return null;
    }

    importResultRegistry.put(ownJob.getId(), new ImportResult(ownJob.getOwnerId()));

    final ImportResponse importResponse = executor.execute(importRequest, ownJob.getId());

    return importResponse.toOutput();
  }

  @Override
  @Nonnull
  public PreAsyncValidationResult<ImportRequest> preAsyncValidate(
      @Nonnull final ServletRequestDetails servletRequestDetails, @Nonnull final Object[] params)
      throws InvalidRequestException {
    // Detect content type to determine which validator to use.
    final String contentType = getContentType(servletRequestDetails);

    if (contentType != null && contentType.toLowerCase().contains("application/json")) {
      // Handle JSON (SMART Bulk Data Import) format.
      return validateJsonRequest(servletRequestDetails);
    } else {
      // Handle FHIR Parameters (application/fhir+json) format.
      return importOperationValidator.validateParametersRequest(
          servletRequestDetails,
          (Parameters) params[0]
      );
    }
  }

  /**
   * Validates a JSON import request (SMART Bulk Data Import format).
   *
   * @param servletRequestDetails the servlet request details
   * @return the validation result
   */
  @Nonnull
  private PreAsyncValidationResult<ImportRequest> validateJsonRequest(
      @Nonnull final ServletRequestDetails servletRequestDetails) {
    try {
      // Read the raw request body.
      final HttpServletRequest httpRequest = servletRequestDetails.getServletRequest();
      final ImportManifest manifest = objectMapper.readValue(
          httpRequest.getInputStream(),
          ImportManifest.class
      );
      return importOperationValidator.validateJsonRequest(servletRequestDetails, manifest);
    } catch (final IOException e) {
      throw new InvalidUserInputError("Failed to parse JSON request body: " + e.getMessage(), e);
    }
  }

  /**
   * Extracts the Content-Type header from the request.
   *
   * @param servletRequestDetails the servlet request details
   * @return the content type, or null if not present
   */
  @Nullable
  private String getContentType(@Nonnull final ServletRequestDetails servletRequestDetails) {
    final HttpServletRequest httpRequest = servletRequestDetails.getServletRequest();
    if (httpRequest != null) {
      return httpRequest.getContentType();
    }
    return null;
  }
}
