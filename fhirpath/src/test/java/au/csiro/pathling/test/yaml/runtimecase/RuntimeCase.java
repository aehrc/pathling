package au.csiro.pathling.test.yaml.runtimecase;

import au.csiro.pathling.test.yaml.YamlSpecTestBase.ResolverBuilder;
import jakarta.annotation.Nonnull;
import org.slf4j.Logger;

/**
 * Interface defining the contract for test case execution.
 */
public interface RuntimeCase {

  /**
   * Logs the test case details.
   *
   * @param log The logger instance to use
   */
  void log(@Nonnull Logger log);

  /**
   * Executes the test case validation.
   *
   * @param rb The resolver builder to use for resource resolution
   */
  void check(@Nonnull final ResolverBuilder rb);

  /**
   * Gets a human-readable description of the test case.
   *
   * @return The test case description
   */
  @Nonnull
  String getDescription();
}
