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

package au.csiro.pathling.operations.bulkexport;

import au.csiro.pathling.async.AsyncJobContext;
import au.csiro.pathling.async.Job;
import au.csiro.pathling.async.JobRegistry;
import au.csiro.pathling.async.RequestTag;
import au.csiro.pathling.async.RequestTagFactory;
import au.csiro.pathling.config.ServerConfiguration;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import org.hl7.fhir.r4.model.Parameters;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

/**
 * Shared helper for executing bulk export operations across different export providers.
 *
 * @author John Grimes
 */
@Component
public class ExportOperationHelper {

  @Nonnull private final ExportExecutor exportExecutor;

  @Nonnull private final JobRegistry jobRegistry;

  @Nonnull private final RequestTagFactory requestTagFactory;

  @Nonnull private final ExportResultRegistry exportResultRegistry;

  @Nonnull private final ServerConfiguration serverConfiguration;

  /**
   * Constructs a new ExportOperationHelper.
   *
   * @param exportExecutor the export executor
   * @param jobRegistry the job registry
   * @param requestTagFactory the request tag factory
   * @param exportResultRegistry the export result registry
   * @param serverConfiguration the server configuration
   */
  public ExportOperationHelper(
      @Nonnull final ExportExecutor exportExecutor,
      @Nonnull final JobRegistry jobRegistry,
      @Nonnull final RequestTagFactory requestTagFactory,
      @Nonnull final ExportResultRegistry exportResultRegistry,
      @Nonnull final ServerConfiguration serverConfiguration) {
    this.exportExecutor = exportExecutor;
    this.jobRegistry = jobRegistry;
    this.requestTagFactory = requestTagFactory;
    this.exportResultRegistry = exportResultRegistry;
    this.serverConfiguration = serverConfiguration;
  }

  /**
   * Executes a bulk export operation using the job information from the request details.
   *
   * @param requestDetails the servlet request details
   * @return the parameters result containing the export manifest, or null if the job was cancelled
   */
  @Nullable
  public Parameters executeExport(@Nonnull final ServletRequestDetails requestDetails) {
    final Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

    // Try to get the job from the async context first. This is set by AsyncAspect when the
    // operation runs asynchronously, avoiding the need to access the servlet request which may
    // have been recycled by Tomcat.
    @SuppressWarnings("unchecked")
    final Job<ExportRequest> ownJob =
        AsyncJobContext.getCurrentJob()
            .map(job -> (Job<ExportRequest>) job)
            .orElseGet(
                () -> {
                  // Fallback for cases where async context is not available.
                  final RequestTag ownTag =
                      requestTagFactory.createTag(requestDetails, authentication);
                  return jobRegistry.get(ownTag);
                });

    if (ownJob == null) {
      throw new InvalidRequestException("Missing 'Prefer: respond-async' header value.");
    }

    final ExportRequest exportRequest = ownJob.getPreAsyncValidationResult();
    if (ownJob.isCancelled()) {
      return null;
    }

    exportResultRegistry.put(ownJob.getId(), new ExportResult(ownJob.getOwnerId()));

    final ExportResponse exportResponse = exportExecutor.execute(exportRequest, ownJob.getId());

    // Set the Expires header to indicate when the export result will expire, based on the
    // configured value.
    ownJob.setResponseModification(
        httpServletResponse -> {
          final String expiresValue =
              ZonedDateTime.now(ZoneOffset.UTC)
                  .plusSeconds(serverConfiguration.getExport().getResultExpiry())
                  .format(DateTimeFormatter.RFC_1123_DATE_TIME);
          httpServletResponse.addHeader("Expires", expiresValue);
        });

    return exportResponse.toOutput();
  }
}
