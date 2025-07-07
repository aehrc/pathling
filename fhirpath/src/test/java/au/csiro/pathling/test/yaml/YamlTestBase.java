package au.csiro.pathling.test.yaml;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.yaml.executor.DefaultYamlTestExecutor;
import au.csiro.pathling.test.yaml.executor.YamlTestExecutor;
import au.csiro.pathling.test.yaml.resolver.ResolverBuilder;
import au.csiro.pathling.test.yaml.resolver.RuntimeContext;
import jakarta.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.opentest4j.TestAbortedException;
import org.springframework.beans.factory.annotation.Autowired;


/**
 * Base class for YAML-based FHIRPath specification tests. This class provides the infrastructure
 * for running FHIRPath tests defined in YAML files, with support for test exclusions, resource
 * resolution, and result validation.
 * <p>
 * The class uses a combination of Spring Boot test infrastructure and custom test utilities to
 * execute FHIRPath expressions against test data and validate the results against expected
 * outcomes.
 */
@SpringBootUnitTest
@Slf4j
public abstract class YamlTestBase {

  public static final String PROPERTY_DISABLED_EXCLUSIONS = "au.csiro.pathling.test.yaml.disabledExclusions";
  public static final String PROPERTY_EXCLUSIONS_ONLY = "au.csiro.pathling.test.yaml.exclusionsOnly";

  @Autowired
  protected SparkSession spark;

  @Autowired
  protected FhirEncoders fhirEncoders;


  /**
   * Creates a new resolver builder instance for test execution.
   *
   * @return A new resolver builder configured with the current Spark session and FHIR encoders
   */
  @Nonnull
  protected ResolverBuilder createResolverBuilder() {
    return RuntimeContext.of(spark, fhirEncoders);
  }

  /**
   * Executes a runtime test case, handling logging and exclusion checks.
   *
   * @param executor The test case to run
   * @throws TestAbortedException if the test case is excluded
   */
  public void run(@Nonnull final YamlTestExecutor executor) {
    executor.log(log);

    // Check if the test case is excluded and skip if no outcome is defined.
    if (executor instanceof final DefaultYamlTestExecutor defaultExecutor
        && defaultExecutor.getExclusion().isPresent()
        && defaultExecutor.getExclusion().get().getOutcome() == null) {
      throw new TestAbortedException(
          "Test case skipped due to exclusion: " + defaultExecutor.getExclusion().get());
    }

    executor.check(createResolverBuilder());

    // If the test case is excluded, throw an exception to mark the test as skipped after any checks 
    // have completed.
    if (executor instanceof final DefaultYamlTestExecutor defaultExecutor
        && defaultExecutor.getExclusion().isPresent()) {
      throw new TestAbortedException(
          "Test case skipped due to exclusion: " + defaultExecutor.getExclusion().get());
    }
  }
}
