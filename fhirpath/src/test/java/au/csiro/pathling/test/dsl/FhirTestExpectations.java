package au.csiro.pathling.test.dsl;

import java.util.Collections;
import java.util.function.Function;
import jakarta.annotation.Nonnull;

/**
 * Utility class providing static methods for common test expectations. These methods return
 * functions that can be passed to the test() methods.
 */
public final class FhirTestExpectations {

  private FhirTestExpectations() {
    // Utility class, no instantiation
  }

  /**
   * Creates a test case that expects the expression result to equal the provided value.
   *
   * @param expected The expected value
   * @param expression The FHIRPath expression to evaluate
   * @return A function that configures a TestCaseBuilder with this expression and expectation
   */
  @Nonnull
  public static Function<FhirPathTestBuilder.TestCaseBuilder, FhirPathTestBuilder.TestCaseBuilder> expectEquals(
      @Nonnull Object expected, @Nonnull String expression) {
    return builder -> builder.expression(expression).expectResult(expected);
  }

  /**
   * Creates a test case that expects the expression result to be true.
   *
   * @param expression The FHIRPath expression to evaluate
   * @return A function that configures a TestCaseBuilder with this expression and expectation
   */
  @Nonnull
  public static Function<FhirPathTestBuilder.TestCaseBuilder, FhirPathTestBuilder.TestCaseBuilder> expectTrue(
      @Nonnull String expression) {
    return builder -> builder.expression(expression).expectResult(true);
  }

  /**
   * Creates a test case that expects the expression result to be false.
   *
   * @param expression The FHIRPath expression to evaluate
   * @return A function that configures a TestCaseBuilder with this expression and expectation
   */
  @Nonnull
  public static Function<FhirPathTestBuilder.TestCaseBuilder, FhirPathTestBuilder.TestCaseBuilder> expectFalse(
      @Nonnull String expression) {
    return builder -> builder.expression(expression).expectResult(false);
  }

  /**
   * Creates a test case that expects the expression result to be empty.
   *
   * @param expression The FHIRPath expression to evaluate
   * @return A function that configures a TestCaseBuilder with this expression and expectation
   */
  @Nonnull
  public static Function<FhirPathTestBuilder.TestCaseBuilder, FhirPathTestBuilder.TestCaseBuilder> expectEmpty(
      @Nonnull String expression) {
    return builder -> builder.expression(expression).expectResult(Collections.emptyList());
  }

  /**
   * Creates a test case that expects the expression to throw an error.
   *
   * @param expression The FHIRPath expression to evaluate
   * @return A function that configures a TestCaseBuilder with this expression and expectation
   */
  @Nonnull
  public static Function<FhirPathTestBuilder.TestCaseBuilder, FhirPathTestBuilder.TestCaseBuilder> expectError(
      @Nonnull String expression) {
    return builder -> builder.expression(expression).expectError();
  }
}
