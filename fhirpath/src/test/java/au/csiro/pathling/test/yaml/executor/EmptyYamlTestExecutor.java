package au.csiro.pathling.test.yaml.executor;

import au.csiro.pathling.test.yaml.resolver.ResolverBuilder;
import jakarta.annotation.Nonnull;
import lombok.Value;
import org.slf4j.Logger;

/**
 * Simple implementation of YamlTestExecutor for scenarios where no tests are available. Provides a
 * minimal implementation that logs the absence of tests and performs no validation.
 */
@Value(staticConstructor = "of")
public class EmptyYamlTestExecutor implements YamlTestExecutor {

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
