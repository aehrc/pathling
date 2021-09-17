/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.async;

import static au.csiro.pathling.utilities.Preconditions.checkNotNull;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.*;
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
public class SparkListener implements SparkListenerInterface {

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

  @Override
  public void onJobStart(@Nullable final SparkListenerJobStart jobStart) {
  }

  @Override
  public void onTaskEnd(@Nullable final SparkListenerTaskEnd taskEnd) {
  }

  @Override
  public void onTaskStart(final SparkListenerTaskStart taskStart) {
  }

  @Override
  public void onTaskGettingResult(final SparkListenerTaskGettingResult taskGettingResult) {
  }

  @Override
  public void onJobEnd(final SparkListenerJobEnd jobEnd) {
  }

  @Override
  public void onEnvironmentUpdate(final SparkListenerEnvironmentUpdate environmentUpdate) {
  }

  @Override
  public void onBlockManagerAdded(final SparkListenerBlockManagerAdded blockManagerAdded) {
  }

  @Override
  public void onBlockManagerRemoved(final SparkListenerBlockManagerRemoved blockManagerRemoved) {
  }

  @Override
  public void onUnpersistRDD(final SparkListenerUnpersistRDD unpersistRDD) {
  }

  @Override
  public void onApplicationStart(final SparkListenerApplicationStart applicationStart) {
  }

  @Override
  public void onApplicationEnd(final SparkListenerApplicationEnd applicationEnd) {
  }

  @Override
  public void onExecutorMetricsUpdate(
      final SparkListenerExecutorMetricsUpdate executorMetricsUpdate) {
  }

  @Override
  public void onStageExecutorMetrics(final SparkListenerStageExecutorMetrics executorMetrics) {
  }

  @Override
  public void onExecutorAdded(final SparkListenerExecutorAdded executorAdded) {
  }

  @Override
  public void onExecutorRemoved(final SparkListenerExecutorRemoved executorRemoved) {
  }

  @SuppressWarnings("deprecation")
  @Override
  public void onExecutorBlacklisted(final SparkListenerExecutorBlacklisted executorBlacklisted) {
  }

  @Override
  public void onExecutorExcluded(final SparkListenerExecutorExcluded executorExcluded) {
  }

  @SuppressWarnings("deprecation")
  @Override
  public void onExecutorBlacklistedForStage(
      final SparkListenerExecutorBlacklistedForStage executorBlacklistedForStage) {
  }

  @Override
  public void onExecutorExcludedForStage(
      final SparkListenerExecutorExcludedForStage executorExcludedForStage) {
  }

  @SuppressWarnings("deprecation")
  @Override
  public void onNodeBlacklistedForStage(
      final SparkListenerNodeBlacklistedForStage nodeBlacklistedForStage) {
  }

  @Override
  public void onNodeExcludedForStage(final SparkListenerNodeExcludedForStage nodeExcludedForStage) {

  }

  @SuppressWarnings("deprecation")
  @Override
  public void onExecutorUnblacklisted(
      final SparkListenerExecutorUnblacklisted executorUnblacklisted) {
  }

  @Override
  public void onExecutorUnexcluded(final SparkListenerExecutorUnexcluded executorUnexcluded) {
  }

  @SuppressWarnings("deprecation")
  @Override
  public void onNodeBlacklisted(final SparkListenerNodeBlacklisted nodeBlacklisted) {
  }

  @Override
  public void onNodeExcluded(final SparkListenerNodeExcluded nodeExcluded) {
  }

  @SuppressWarnings("deprecation")
  @Override
  public void onNodeUnblacklisted(final SparkListenerNodeUnblacklisted nodeUnblacklisted) {
  }

  @Override
  public void onNodeUnexcluded(final SparkListenerNodeUnexcluded nodeUnexcluded) {
  }

  @Override
  public void onUnschedulableTaskSetAdded(
      final SparkListenerUnschedulableTaskSetAdded unschedulableTaskSetAdded) {
  }

  @Override
  public void onUnschedulableTaskSetRemoved(
      final SparkListenerUnschedulableTaskSetRemoved unschedulableTaskSetRemoved) {
  }

  @Override
  public void onBlockUpdated(final SparkListenerBlockUpdated blockUpdated) {
  }

  @Override
  public void onSpeculativeTaskSubmitted(
      final SparkListenerSpeculativeTaskSubmitted speculativeTask) {
  }

  @Override
  public void onOtherEvent(final SparkListenerEvent event) {
  }

  @Override
  public void onResourceProfileAdded(final SparkListenerResourceProfileAdded event) {
  }

}
