/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.async;

import java.util.concurrent.Future;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.ToString;
import org.hl7.fhir.instance.model.api.IBaseResource;

/**
 * Represents a background job that is in progress.
 *
 * @author John Grimes
 */
@Getter
@ToString
public class Job {

  @Nonnull
  private final String operation;

  @Nonnull
  private final Future<IBaseResource> result;

  private int totalStages;

  private int completedStages;

  /**
   * @param operation the operation that initiated the job, used for enforcing authorization
   * @param result the {@link Future} result
   */
  public Job(@Nonnull final String operation, @Nonnull final Future<IBaseResource> result) {
    this.operation = operation;
    this.result = result;
  }

  /**
   * Increment the number of total stages within the job, used to calculate progress.
   */
  public void incrementTotalStages() {
    totalStages++;
  }

  /**
   * Increment the number of completed stages within the job, used to calculate progress.
   */
  public void incrementCompletedStages() {
    completedStages++;
  }

  public int getProgressPercentage() {
    return (completedStages * 100) / totalStages;
  }

}
