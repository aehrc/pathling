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

package au.csiro.pathling.operations.import_;

import au.csiro.pathling.async.AsyncSupported;
import au.csiro.pathling.async.Job;
import au.csiro.pathling.async.JobRegistry;
import au.csiro.pathling.async.PreAsyncValidation;
import au.csiro.pathling.async.RequestTag;
import au.csiro.pathling.async.RequestTagFactory;
import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.operations.export.ExportRequest;
import au.csiro.pathling.security.OperationAccess;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.Parameters;
import org.springframework.context.annotation.Profile;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import java.util.Optional;

import static au.csiro.pathling.security.SecurityAspect.getCurrentUserId;

/**
 * Enables the bulk import of data into the server.
 *
 * @author John Grimes
 */
@Component
@Profile("server")
@Slf4j
public class ImportProvider implements PreAsyncValidation<ImportRequest> {

  @Nonnull
  private final ImportExecutor executor;
  private final ImportOperationValidator importOperationValidator;
  private final RequestTagFactory requestTagFactory;
  private final JobRegistry jobRegistry;
  private final ImportResultRegistry importResultRegistry;

  /**
   * @param executor An {@link ImportExecutor} to use in executing import requests
   */
  public ImportProvider(@Nonnull final ImportExecutor executor,
      ImportOperationValidator importOperationValidator, RequestTagFactory requestTagFactory,
      JobRegistry jobRegistry, ImportResultRegistry importResultRegistry) {
    this.executor = executor;
    this.importOperationValidator = importOperationValidator;
    this.requestTagFactory = requestTagFactory;
    this.jobRegistry = jobRegistry;
    this.importResultRegistry = importResultRegistry;
  }

  /**
   * Accepts a request of type `application/fhir+ndjson` and overwrites the warehouse tables with
   * the contents. Does not currently support any sort of incremental update or appending to the
   * warehouse tables.
   * <p>
   * Each input will be treated as a file containing only one type of resource type. Bundles are not
   * currently given any special treatment. Each resource type is assumed to appear in the list only
   * once - multiple occurrences will result in the last input overwriting the previous ones.
   *
   * @param parameters A FHIR {@link Parameters} object describing the import request
   * @param requestDetails the {@link ServletRequestDetails} containing HAPI inferred info
   * @return A FHIR {@link OperationOutcome} resource describing the result
   */
  @Operation(name = "$import")
  @SuppressWarnings("UnusedReturnValue")
  @OperationAccess("import")
  @AsyncSupported
  public Parameters importOperation(@ResourceParam final Parameters parameters,
      @SuppressWarnings("unused") @Nullable final ServletRequestDetails requestDetails) {

    final Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
    RequestTag ownTag = requestTagFactory.createTag(requestDetails, authentication);
    Job<ImportRequest> ownJob = jobRegistry.get(ownTag);
    if (ownJob == null) {
      throw new InvalidRequestException("Missing 'Prefer: respond-async' header value.");
    }
    // Check that the user requesting the result is the same user that started the job.
    final Optional<String> currentUserId = getCurrentUserId(authentication);
    if (currentUserId.isPresent() && !ownJob.getOwnerId().equals(currentUserId)) {
      throw new AccessDeniedError("The requested result is not owned by the current user '%s'.".formatted(currentUserId.orElse("null")));
    }

    ImportRequest importRequest = ownJob.getPreAsyncValidationResult();
    if (ownJob.isCancelled()) {
      return null;
    }
    
    importResultRegistry.put(ownJob.getId(), new ImportResult(ownJob.getOwnerId()));
    
    ImportResponse importResponse = executor.execute(importRequest, ownJob.getId());
    
    return importResponse.toOutput();
  }

  @Override
  public PreAsyncValidationResult<ImportRequest> preAsyncValidate(
      final ServletRequestDetails servletRequestDetails, final Object[] params)
      throws InvalidRequestException {
    return importOperationValidator.validateRequest(
        servletRequestDetails,
        (Parameters) params[0]
    );
  }
}
