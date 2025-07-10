package au.csiro.pathling.test.dsl;

import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.yaml.YamlCachedTestBase;
import jakarta.annotation.Nonnull;

@SpringBootUnitTest
public abstract class FhirPathDslTestBase extends YamlCachedTestBase {

  @Nonnull
  protected FhirPathTestBuilder builder() {
    return new FhirPathTestBuilder(this);
  }
}
