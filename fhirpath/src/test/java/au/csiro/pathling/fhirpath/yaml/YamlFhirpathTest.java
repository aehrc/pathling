package au.csiro.pathling.fhirpath.yaml;

import au.csiro.pathling.test.yaml.YamlConfig;
import au.csiro.pathling.test.yaml.YamlSpec;
import au.csiro.pathling.test.yaml.YamlSpecCachedTestBase;
import jakarta.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Tag;

@Slf4j
@Tag("UnitTest")
@YamlConfig(
    resourceBase = "fhirpath-ptl/resources"
)
public class YamlFhirpathTest extends YamlSpecCachedTestBase {

  @YamlSpec("fhirpath-ptl/cases/math.yaml")
  void testMath(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

  @YamlSpec("fhirpath-ptl/cases/operators.yaml")
  void testOperators(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

  @YamlSpec("fhirpath-ptl/cases/misc.yaml")
  void testMisc(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }
}
