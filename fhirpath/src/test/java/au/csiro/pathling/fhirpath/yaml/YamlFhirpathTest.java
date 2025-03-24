package au.csiro.pathling.fhirpath.yaml;

import au.csiro.pathling.fhirpath.context.ResourceResolver;
import au.csiro.pathling.test.yaml.YamlConfig;
import au.csiro.pathling.test.yaml.YamlSpec;
import au.csiro.pathling.test.yaml.YamlSpecCachedTestBase;
import jakarta.annotation.Nonnull;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Tag;

@Slf4j
@Tag("UnitTest")
@YamlConfig(
    resourceBase = "fhirpath-ptl/resources"
)
public class YamlFhirpathTest extends YamlSpecCachedTestBase {

  @Nonnull
  private static final Map<Function<RuntimeContext, ResourceResolver>, ResourceResolver> CACHE =
      Collections.synchronizedMap(new HashMap<>());

  @Override
  protected Map<Function<RuntimeContext, ResourceResolver>, ResourceResolver> getResolverCache() {
    return CACHE;
  }

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

  @YamlSpec("fhirpath-ptl/cases/math.yaml")
  void testMath(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

  @YamlSpec("fhirpath-ptl/cases/conversion_functions.yaml")
  void testConversionFunctions(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

  @YamlSpec("fhirpath-ptl/cases/operators.yaml")
  void testOperators(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

}
