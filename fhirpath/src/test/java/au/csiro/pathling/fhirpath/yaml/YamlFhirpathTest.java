package au.csiro.pathling.fhirpath.yaml;

import au.csiro.pathling.test.yaml.YamlCachedTestBase;
import au.csiro.pathling.test.yaml.annotations.YamlTest;
import au.csiro.pathling.test.yaml.annotations.YamlTestConfiguration;
import au.csiro.pathling.test.yaml.executor.YamlTestExecutor;
import jakarta.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Tag;

@Slf4j
@YamlTestConfiguration(
    resourceBase = "fhirpath-ptl/resources"
)
public class YamlFhirpathTest extends YamlCachedTestBase {

  @YamlTest("fhirpath-ptl/cases/math.yaml")
  void testMath(@Nonnull final YamlTestExecutor testCase) {
    run(testCase);
  }

  @YamlTest("fhirpath-ptl/cases/search-params.yaml")
  void testSearchParams(@Nonnull final YamlTestExecutor testCase) {
    run(testCase);
  }
  
  @YamlTest("fhirpath-ptl/cases/datetime_comparison.yaml")
  void testDateTimeComparison(@Nonnull final YamlTestExecutor testCase) {
    run(testCase);
  }
}
