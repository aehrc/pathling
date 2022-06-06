/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.async;

import static au.csiro.pathling.utilities.Preconditions.checkNotNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Used to listen to progress of Spark operations.
 *
 * @author John Grimes
 */
@Component
@Profile("server")
@ConditionalOnProperty(prefix = "pathling", name = "async.enabled", havingValue = "true")
@Slf4j
public class SparkListener extends org.apache.spark.scheduler.SparkListener {

  @Nonnull
  private final JobRegistry jobRegistry;

  @Nonnull
  private final StageMap stageMap;

  /**
   * @param jobRegistry the {@link JobRegistry} used to keep track of running jobs
   * @param stageMap the {@link StageMap} used to map stages to job IDs
   */
  public SparkListener(@Nonnull final JobRegistry jobRegistry,
      @Nonnull final StageMap stageMap) {
    this.jobRegistry = jobRegistry;
    this.stageMap = stageMap;
  }

  @Override
  public void onStageCompleted(final SparkListenerStageCompleted stageCompleted) {
    checkNotNull(stageCompleted);
    @Nullable final String jobGroupId = stageMap.get(stageCompleted.stageInfo().stageId());
    if (jobGroupId != null) {
      @Nullable final Job job = jobRegistry.get(jobGroupId);
      if (job != null) {
        job.incrementCompletedStages();
      }
    }
  }

  @Override
  public void onStageSubmitted(final SparkListenerStageSubmitted stageSubmitted) {
    checkNotNull(stageSubmitted);
    @Nullable final String jobGroupId = stageSubmitted.properties()
        .getProperty("spark.jobGroup.id");
    if (jobGroupId == null) {
      return;
    }
    @Nullable final Job job = jobRegistry.get(jobGroupId);
    if (job != null) {
      stageMap.put(stageSubmitted.stageInfo().stageId(), jobGroupId);
      job.incrementTotalStages();
    }
  }

}
