package au.csiro.pathling.test.dsl;

import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.yaml.YamlSpecCachedTestBase;
import jakarta.annotation.Nonnull;

@SpringBootUnitTest
public abstract class FhirPathDslTestBase extends YamlSpecCachedTestBase {
  
  @Nonnull
  protected FhirPathTestBuilder builder() {
    return new FhirPathTestBuilder(this);
  }
}
