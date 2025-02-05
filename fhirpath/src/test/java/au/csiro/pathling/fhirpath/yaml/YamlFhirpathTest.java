package au.csiro.pathling.fhirpath.yaml;

import jakarta.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;


@Slf4j
@Tag("WorkTest")
@Disabled
public class YamlFhirpathTest extends YamlSpecTestBase {
  @YamlSpec("fhirpath/tests/tests.yaml")
  void testTest(@Nonnull final RuntimeCase testCase) {
    run(testCase);
  }

}
