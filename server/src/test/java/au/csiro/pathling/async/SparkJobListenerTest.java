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

package au.csiro.pathling.async;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Properties;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for {@link SparkJobListener} covering stage completion tracking, stage submission, job
 * cancellation, and orphaned job group handling.
 *
 * @author John Grimes
 */
@Tag("UnitTest")
@ExtendWith(MockitoExtension.class)
class SparkJobListenerTest {

  @Mock private JobRegistry jobRegistry;
  @Mock private SparkSession sparkSession;
  @Mock private SparkContext sparkContext;

  private StageMap stageMap;
  private SparkJobListener listener;

  @BeforeEach
  void setUp() {
    stageMap = new StageMap();
    listener = new SparkJobListener(jobRegistry, stageMap, sparkSession);
  }

  // -- onStageCompleted --

  @Test
  void onStageCompletedIncrementsCompletedStages() {
    // When a stage completes for a known job, the completed stages counter should increment.
    stageMap.put(1, "job-1");
    final Job<?> job = mock(Job.class);
    doReturn(job).when(jobRegistry).get("job-1");
    when(job.isCancelled()).thenReturn(false);

    final SparkListenerStageCompleted event = createStageCompletedEvent(1);

    listener.onStageCompleted(event);

    verify(job).incrementCompletedStages();
  }

  @Test
  void onStageCompletedCancelsCancelledJob() {
    // When a stage completes for a cancelled job, the Spark job group should be cancelled.
    stageMap.put(1, "job-1");
    final Job<?> job = mock(Job.class);
    doReturn(job).when(jobRegistry).get("job-1");
    when(job.isCancelled()).thenReturn(true);
    when(job.getId()).thenReturn("job-1");
    when(sparkSession.sparkContext()).thenReturn(sparkContext);

    final SparkListenerStageCompleted event = createStageCompletedEvent(1);

    listener.onStageCompleted(event);

    verify(sparkContext).cancelJobGroup("job-1");
    verify(job, never()).incrementCompletedStages();
  }

  @Test
  void onStageCompletedCancelsOrphanedJobGroup() {
    // When a stage completes for a job group not in the registry but in the orphan list, it should
    // be cancelled.
    stageMap.put(1, "orphan-1");
    when(jobRegistry.get("orphan-1")).thenReturn(null);
    when(jobRegistry.removedFromRegistryButStillWithSparkJobContains("orphan-1")).thenReturn(true);
    when(sparkSession.sparkContext()).thenReturn(sparkContext);

    final SparkListenerStageCompleted event = createStageCompletedEvent(1);

    listener.onStageCompleted(event);

    verify(sparkContext).cancelJobGroup("orphan-1");
  }

  @Test
  void onStageCompletedIgnoresUnknownStage() {
    // When a stage completes that is not in the stage map, nothing should happen.
    final SparkListenerStageCompleted event = createStageCompletedEvent(999);

    listener.onStageCompleted(event);

    // No exceptions and no interactions with the registry.
  }

  // -- onStageSubmitted --

  @Test
  void onStageSubmittedIncrementsTotalStages() {
    // When a stage is submitted for a known job, the total stages counter should increment.
    final Job<?> job = mock(Job.class);
    doReturn(job).when(jobRegistry).get("job-1");
    when(job.isCancelled()).thenReturn(false);

    final SparkListenerStageSubmitted event = createStageSubmittedEvent(1, "job-1");

    listener.onStageSubmitted(event);

    verify(job).incrementTotalStages();
  }

  @Test
  void onStageSubmittedIgnoresNullJobGroupId() {
    // When a stage is submitted without a job group ID, it should be ignored.
    final SparkListenerStageSubmitted event = createStageSubmittedEvent(1, null);

    listener.onStageSubmitted(event);

    // No exceptions and no interactions with the registry.
  }

  @Test
  void onStageSubmittedCancelsCancelledJob() {
    // When a stage is submitted for a cancelled job, the Spark job group should be cancelled.
    final Job<?> job = mock(Job.class);
    doReturn(job).when(jobRegistry).get("job-1");
    when(job.isCancelled()).thenReturn(true);
    when(job.getId()).thenReturn("job-1");
    when(sparkSession.sparkContext()).thenReturn(sparkContext);

    final SparkListenerStageSubmitted event = createStageSubmittedEvent(1, "job-1");

    listener.onStageSubmitted(event);

    verify(sparkContext).cancelJobGroup("job-1");
    verify(job, never()).incrementTotalStages();
  }

  @Test
  void onStageSubmittedCancelsOrphanedJobGroup() {
    // When a stage is submitted for a job group removed from registry but still running, it should
    // be cancelled.
    when(jobRegistry.get("orphan-1")).thenReturn(null);
    when(jobRegistry.removedFromRegistryButStillWithSparkJobContains("orphan-1")).thenReturn(true);
    when(sparkSession.sparkContext()).thenReturn(sparkContext);

    final SparkListenerStageSubmitted event = createStageSubmittedEvent(1, "orphan-1");

    listener.onStageSubmitted(event);

    verify(sparkContext).cancelJobGroup("orphan-1");
  }

  private SparkListenerStageCompleted createStageCompletedEvent(final int stageId) {
    final StageInfo stageInfo = mock(StageInfo.class);
    when(stageInfo.stageId()).thenReturn(stageId);
    return new SparkListenerStageCompleted(stageInfo);
  }

  private SparkListenerStageSubmitted createStageSubmittedEvent(
      final int stageId, final String jobGroupId) {
    final StageInfo stageInfo = mock(StageInfo.class);
    lenient().when(stageInfo.stageId()).thenReturn(stageId);
    final Properties properties = new Properties();
    if (jobGroupId != null) {
      properties.setProperty("spark.jobGroup.id", jobGroupId);
    }
    return new SparkListenerStageSubmitted(stageInfo, properties);
  }
}
