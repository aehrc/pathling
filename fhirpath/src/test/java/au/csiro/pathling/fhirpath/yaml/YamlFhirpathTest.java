package au.csiro.pathling.fhirpath.yaml;

import au.csiro.pathling.test.yaml.YamlSpec;
import au.csiro.pathling.test.yaml.YamlSpecTestBase;
import jakarta.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Tag;


@Slf4j
@Tag("UnitTest")
public class YamlFhirpathTest extends YamlSpecTestBase {

  @YamlSpec("fhirpath-ptl/cases/literals.yaml")
  void testLiterals(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

  @YamlSpec("fhirpath-ptl/cases/existence_functions.yaml")
  void testExistenceFunctions(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

  @YamlSpec("fhirpath-ptl/cases/quantities.yaml")
  void testQuantities(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }
}
