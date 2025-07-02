package au.csiro.pathling.fhirpath.yaml;

import au.csiro.pathling.test.yaml.YamlCachedTestBase;
import au.csiro.pathling.test.yaml.annotations.YamlConfig;
import au.csiro.pathling.test.yaml.annotations.YamlSpec;
import au.csiro.pathling.test.yaml.runtimecase.RuntimeCase;
import jakarta.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;

@Slf4j
@Tag("UnitTest")
@YamlConfig(
    resourceBase = "fhirpath-ptl/resources"
)
public class YamlFhirpathTest extends YamlCachedTestBase {

  @YamlSpec("fhirpath-ptl/cases/math.yaml")
  void testMath(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

  @Disabled("Disabled until we add exclusion rules")
  @YamlSpec("fhirpath-ptl/cases/search-params.yaml")
  void testSearchParams(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }
}
