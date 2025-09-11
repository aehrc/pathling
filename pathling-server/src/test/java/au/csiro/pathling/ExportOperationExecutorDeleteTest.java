package au.csiro.pathling;

import au.csiro.pathling.async.Job;
import au.csiro.pathling.export.ExportExecutor;
import au.csiro.pathling.export.ExportOperationValidator;
import au.csiro.pathling.export.ExportRequest;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import au.csiro.pathling.util.TestDataSetup;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Felix Naumann
 */
@Slf4j
@Import({
    ExportOperationValidator.class,
    ExportExecutor.class,
    PathlingContext.class,
    TestDataSetup.class,
    FhirServerTestConfiguration.class
})
@SpringBootUnitTest
public class ExportOperationExecutorDeleteTest {
  
  
  private static Job<ExportRequest> mockJob(boolean cancelled) {
    Job<ExportRequest> mockJob = mock(Job.class);
    when(mockJob.isCancelled()).thenReturn(cancelled);
    return mockJob;
  }
}
