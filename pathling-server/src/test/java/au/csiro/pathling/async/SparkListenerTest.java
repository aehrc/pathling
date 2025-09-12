package au.csiro.pathling.async;

import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import org.apache.http.concurrent.BasicFuture;
import org.apache.spark.scheduler.JobFailed;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Felix Naumann
 */
@Import(FhirServerTestConfiguration.class)
@SpringBootUnitTest
public class SparkListenerTest {

  @Autowired
  private SparkListener sparkListener;
  
  @Autowired
  private StageMap stageMap;
  
  @MockBean
  private JobRegistry jobRegistry;
  @Autowired
  private SparkSession sparkSession;
  
  
  @Test
  void test_on_stage_completed_increments_completed_stages_counter_in_job() {
    Job<?> job = scenario(42, "job-group-1");
    
    assertThat(job.getCompletedStages()).isZero();
    sparkListener.onStageCompleted(completed_event(42));
    assertThat(job.getCompletedStages()).isOne();
  }
  
  private Job<?> scenario(int stageId, String jobGroupId) {
    stageMap.put(stageId, jobGroupId);
    Job<Object> job = new Job<>(UUID.randomUUID().toString(), "test", mock(Future.class), Optional.empty());
    when(jobRegistry.get(jobGroupId)).thenReturn(job);
    return job;
  }
  
  private static SparkListenerStageCompleted completed_event(int stageId) {
    SparkListenerStageCompleted completeEvent = mock(SparkListenerStageCompleted.class);
    StageInfo stageInfo = mock(StageInfo.class);
    when(completeEvent.stageInfo()).thenReturn(stageInfo);
    when(stageInfo.stageId()).thenReturn(stageId);
    return completeEvent;
  }

  private static class JobGroupTracker extends org.apache.spark.scheduler.SparkListener {
    private final Set<String> activeJobGroups = ConcurrentHashMap.newKeySet();
    private final Map<Integer, String> jobIdToGroup = new ConcurrentHashMap<>();

    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {
      String jobGroup = getJobGroupFromProperties(jobStart.properties());
      if (jobGroup != null) {
        activeJobGroups.add(jobGroup);
        jobIdToGroup.put(jobStart.jobId(), jobGroup);
      }
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
      String jobGroup = jobIdToGroup.remove(jobEnd.jobId());
      if (jobGroup != null && !hasActiveJobsInGroup(jobGroup)) {
        activeJobGroups.remove(jobGroup);
      }
    }

    private String getJobGroupFromProperties(Properties properties) {
      if (properties == null) {
        return null;
      }
      return properties.getProperty("spark.jobGroup.id");
    }

    private boolean hasActiveJobsInGroup(String jobGroup) {
      return jobIdToGroup.values().stream()
          .anyMatch(group -> Objects.equals(group, jobGroup));
    }

    public boolean isJobGroupActive(String jobGroup) {
      return activeJobGroups.contains(jobGroup);
    }
  }
}
