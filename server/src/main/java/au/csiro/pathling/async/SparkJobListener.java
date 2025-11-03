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

package au.csiro.pathling.async;

import static java.util.Objects.requireNonNull;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Lazy;
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
public class SparkJobListener extends org.apache.spark.scheduler.SparkListener {

  @Nonnull
  private final JobRegistry jobRegistry;

  @Nonnull
  private final StageMap stageMap;
  private final SparkSession sparkSession;

  /**
   * @param jobRegistry the {@link JobRegistry} used to keep track of running jobs
   * @param stageMap the {@link StageMap} used to map stages to job IDs
   */
  public SparkJobListener(@Nonnull final JobRegistry jobRegistry,
                          @Nonnull final StageMap stageMap, @Lazy SparkSession sparkSession) {
    this.jobRegistry = jobRegistry;
    this.stageMap = stageMap;
    this.sparkSession = sparkSession;
  }

  @Override
  public void onStageCompleted(final SparkListenerStageCompleted stageCompleted) {
    requireNonNull(stageCompleted);
    @Nullable final String jobGroupId = stageMap.get(stageCompleted.stageInfo().stageId());
    if (jobGroupId != null) {
      @Nullable final Job<?> job = jobRegistry.get(jobGroupId);
      if (job != null) {
        if(job.isCancelled()) {
          log.debug("Stage submitted called but job {} is cancelled. Cancelling the associated job group.", job.getId());
          sparkSession.sparkContext().cancelJobGroup(job.getId());
          return;
        }
        job.incrementCompletedStages();
        log.debug("Completed stage of job {} (Total completed stages now: {})", job.getId(), job.getCompletedStages());
      }
      else if(jobRegistry.removedFromRegistryButStillWithSparkJobContains(jobGroupId)) {
        log.debug("Detected a cancelled job that has been removed from the registry but has a running spark job attached to it. Cancelling the spark job.");
        sparkSession.sparkContext().cancelJobGroup(jobGroupId);
      }
    }
  }

  @Override
  public void onStageSubmitted(final SparkListenerStageSubmitted stageSubmitted) {
    requireNonNull(stageSubmitted);
    @Nullable final String jobGroupId = stageSubmitted.properties()
        .getProperty("spark.jobGroup.id");
    if (jobGroupId == null) {
      return;
    }
    @Nullable final Job<?> job = jobRegistry.get(jobGroupId);
    if (job != null) {
      if(job.isCancelled()) {
        log.info("Stage submitted called but job {} is cancelled. Cancelling the associated job group.", job.getId());
        sparkSession.sparkContext().cancelJobGroup(job.getId());
        return;
      }
      stageMap.put(stageSubmitted.stageInfo().stageId(), jobGroupId);
      job.incrementTotalStages();
      log.debug("Incremented total stages of job {} (Total stages now: {})", job.getId(), job.getTotalStages());
    }
    else if(jobRegistry.removedFromRegistryButStillWithSparkJobContains(jobGroupId)) {
      log.debug("Detected a cancelled job that has been removed from the registry but has a running spark job attached to it. Cancelling the spark job.");
      sparkSession.sparkContext().cancelJobGroup(jobGroupId);
    }
  }
}
