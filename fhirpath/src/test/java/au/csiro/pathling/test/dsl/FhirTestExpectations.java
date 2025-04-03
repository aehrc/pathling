package au.csiro.pathling.test.dsl;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import jakarta.annotation.Nonnull;

/**
 * Utility class providing static methods for common test expectations.
 * These methods return functions that can be passed to the test() methods.
 */
public final class FhirTestExpectations {

  private FhirTestExpectations() {
    // Utility class, no instantiation
  }

  /**
   * Expects the result to equal the provided value.
   *
   * @param expected The expected value
   * @return A function that configures a TestCaseBuilder with this expectation
   */
  @Nonnull
  public static Function<FhirPathTestBuilder.TestCaseBuilder, FhirPathTestBuilder.TestCaseBuilder> expectEquals(
      @Nonnull Object expected) {
    return builder -> builder.expectResult(expected);
  }

  /**
   * Expects the result to be true.
   *
   * @return A function that configures a TestCaseBuilder to expect true
   */
  @Nonnull
  public static Function<FhirPathTestBuilder.TestCaseBuilder, FhirPathTestBuilder.TestCaseBuilder> expectTrue() {
    return builder -> builder.expectResult(true);
  }

  /**
   * Expects the result to be false.
   *
   * @return A function that configures a TestCaseBuilder to expect false
   */
  @Nonnull
  public static Function<FhirPathTestBuilder.TestCaseBuilder, FhirPathTestBuilder.TestCaseBuilder> expectFalse() {
    return builder -> builder.expectResult(false);
  }

  /**
   * Expects the result to be empty.
   *
   * @return A function that configures a TestCaseBuilder to expect an empty result
   */
  @Nonnull
  public static Function<FhirPathTestBuilder.TestCaseBuilder, FhirPathTestBuilder.TestCaseBuilder> expectEmpty() {
    return builder -> builder.expectResult(Collections.emptyList());
  }

  /**
   * Expects the result to be null.
   *
   * @return A function that configures a TestCaseBuilder to expect null
   */
  @Nonnull
  public static Function<FhirPathTestBuilder.TestCaseBuilder, FhirPathTestBuilder.TestCaseBuilder> expectNull() {
    return builder -> builder.expectResult(null);
  }

  /**
   * Expects the result to contain the specified items.
   *
   * @param items The items expected in the result
   * @return A function that configures a TestCaseBuilder with this expectation
   */
  @Nonnull
  public static Function<FhirPathTestBuilder.TestCaseBuilder, FhirPathTestBuilder.TestCaseBuilder> expectContains(
      Object... items) {
    return builder -> builder.expectResult(Arrays.asList(items));
  }

  /**
   * Expects the result to contain the specified collection of items.
   *
   * @param items The collection of items expected in the result
   * @return A function that configures a TestCaseBuilder with this expectation
   */
  @Nonnull
  public static Function<FhirPathTestBuilder.TestCaseBuilder, FhirPathTestBuilder.TestCaseBuilder> expectContains(
      @Nonnull Collection<?> items) {
    return builder -> builder.expectResult(items);
  }

  /**
   * Expects the result to be a single item.
   *
   * @param item The single item expected in the result
   * @return A function that configures a TestCaseBuilder with this expectation
   */
  @Nonnull
  public static Function<FhirPathTestBuilder.TestCaseBuilder, FhirPathTestBuilder.TestCaseBuilder> expectSingle(
      @Nonnull Object item) {
    return builder -> builder.expectResult(List.of(item));
  }

  /**
   * Expects the expression to throw an error.
   *
   * @return A function that configures a TestCaseBuilder to expect an error
   */
  @Nonnull
  public static Function<FhirPathTestBuilder.TestCaseBuilder, FhirPathTestBuilder.TestCaseBuilder> expectError() {
    return FhirPathTestBuilder.TestCaseBuilder::expectError;
  }

  /**
   * Combines multiple expectation functions into one.
   *
   * @param expectations The expectation functions to combine
   * @return A function that applies all the given expectations
   */
  @SafeVarargs
  @Nonnull
  public static Function<FhirPathTestBuilder.TestCaseBuilder, FhirPathTestBuilder.TestCaseBuilder> combine(
      Function<FhirPathTestBuilder.TestCaseBuilder, FhirPathTestBuilder.TestCaseBuilder>... expectations) {
    return builder -> {
      FhirPathTestBuilder.TestCaseBuilder result = builder;
      for (Function<FhirPathTestBuilder.TestCaseBuilder, FhirPathTestBuilder.TestCaseBuilder> expectation : expectations) {
        result = expectation.apply(result);
      }
      return result;
    };
  }
}
