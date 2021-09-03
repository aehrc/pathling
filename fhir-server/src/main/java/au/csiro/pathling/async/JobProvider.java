/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.async;

import static au.csiro.pathling.security.SecurityAspect.checkHasAuthority;
import static au.csiro.pathling.utilities.Preconditions.checkNotNull;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.errors.ResourceNotFoundError;
import au.csiro.pathling.security.PathlingAuthority;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.OperationOutcome.OperationOutcomeIssueComponent;
import org.hl7.fhir.r4.model.Parameters;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * @author John Grimes
 */
@Component
@Profile("server")
@Slf4j
public class JobProvider {

  private static final String PROGRESS_HEADER = "X-Progress";

  @Nonnull
  private final Configuration configuration;

  @Nonnull
  private final JobRegistry jobRegistry;

  /**
   * @param configuration a {@link Configuration} for determining if authorization is enabled
   * @param jobRegistry the {@link JobRegistry} used to keep track of running jobs
   */
  public JobProvider(@Nonnull final Configuration configuration,
      @Nonnull final JobRegistry jobRegistry) {
    this.configuration = configuration;
    this.jobRegistry = jobRegistry;
  }

  /**
   * Queries a running job for its progress, completion status and final result.
   *
   * @param id the ID of the running job
   * @param response the {@link HttpServletResponse} for updating the response
   * @return the final result of the job, as a {@link Parameters} resource
   */
  @SuppressWarnings("unused")
  @Operation(name = "$job", idempotent = true)
  public IBaseResource job(@Nullable @OperationParam(name = "id") final String id,
      @Nullable final HttpServletResponse response) {
    @Nullable final Job job = jobRegistry.get(id);
    // Check that the job exists.
    if (job == null) {
      throw new ResourceNotFoundError("Job ID not found");
    }

    // Check for the required authority associated with the operation that initiated the job.
    if (configuration.getAuth().isEnabled()) {
      checkHasAuthority(PathlingAuthority.operationAccess(job.getOperation()));
    }

    if (job.getResult().isDone()) {
      // If the job is done, we return the Parameters resource.
      try {
        return job.getResult().get();
      } catch (final InterruptedException | ExecutionException e) {
        throw new RuntimeException("Problem retrieving job result", e);
      }
    } else {
      // If the job is not done, we return a 202 along with an OperationOutcome and progress header.
      checkNotNull(response);
      if (job.getTotalStages() > 0) {
        final int progress = job.getProgressPercentage();
        if (progress != 100) {
          // We don't bother showing 100%, this usually means that there are outstanding stages
          // which have not yet been submitted.
          response.setHeader(PROGRESS_HEADER, progress + "%");
        }
      }
      throw new ProcessingNotCompletedException("Processing", buildProcessingOutcome());
    }
  }

  @Nonnull
  private static OperationOutcome buildProcessingOutcome() {
    final OperationOutcome opOutcome = new OperationOutcome();
    final OperationOutcomeIssueComponent issue = new OperationOutcomeIssueComponent();
    issue.setCode(IssueType.INFORMATIONAL);
    issue.setSeverity(IssueSeverity.INFORMATION);
    issue.setDiagnostics("Job currently processing");
    opOutcome.addIssue(issue);
    return opOutcome;
  }

}
