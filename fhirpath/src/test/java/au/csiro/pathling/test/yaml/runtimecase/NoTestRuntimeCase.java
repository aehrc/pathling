package au.csiro.pathling.test.yaml.runtimecase;

import au.csiro.pathling.test.yaml.YamlSpecTestBase.ResolverBuilder;
import jakarta.annotation.Nonnull;
import lombok.Value;
import org.slf4j.Logger;

/**
 * Simple implementation of RuntimeCase for scenarios where no tests are available. Provides a
 * minimal implementation that logs the absence of tests and performs no validation.
 */
@Value(staticConstructor = "of")
public class NoTestRuntimeCase implements RuntimeCase {

  public void log(@Nonnull final Logger log) {
    log.info("No tests");
  }

  @Override
  public void check(@Nonnull final ResolverBuilder rb) {
    // Do nothing, as there are no tests to run.
  }

  @Override
  @Nonnull
  public String getDescription() {
    return "none";
  }
}
