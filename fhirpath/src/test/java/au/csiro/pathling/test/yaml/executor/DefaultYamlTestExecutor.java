/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.test.yaml.executor;

import static au.csiro.pathling.test.yaml.YamlTestDefinition.TestCase.ANY_ERROR;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.errors.UnsupportedFhirPathFeatureError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.DecimalCollection;
import au.csiro.pathling.fhirpath.collection.IntegerCollection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.context.ResourceResolver;
import au.csiro.pathling.fhirpath.definition.ChildDefinition;
import au.csiro.pathling.fhirpath.execution.FhirpathEvaluator;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.test.yaml.YamlSupport;
import au.csiro.pathling.test.yaml.YamlTestDefinition.TestCase;
import au.csiro.pathling.test.yaml.format.ExcludeRule;
import au.csiro.pathling.test.yaml.resolver.ResolverBuilder;
import au.csiro.pathling.test.yaml.resolver.RuntimeContext;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import lombok.EqualsAndHashCode.Exclude;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import org.opentest4j.TestAbortedException;
import org.slf4j.Logger;
import scala.collection.mutable.WrappedArray;

/**
 * Standard implementation of {@link YamlTestExecutor} that handles the execution and validation of
 * FHIRPath test cases defined in YAML format.
 *
 * @author Piotr Szul
 * @author John Grimes
 */
@Value(staticConstructor = "of")
@Slf4j
public class DefaultYamlTestExecutor implements YamlTestExecutor {

  /**
   * Shared FHIRPath parser instance used for parsing all FHIRPath expressions.
   */
  private static final Parser PARSER = new Parser();

  /**
   * The test case specification containing the FHIRPath expression, expected results, variables,
   * and other test configuration. This defines what the test should do and what results are
   * expected.
   */
  @Nonnull
  TestCase spec;

  /**
   * Factory function for creating ResourceResolver instances from RuntimeContext. This is excluded
   * from equals/hashCode to avoid issues with function comparison. The resolver factory is used to
   * create the appropriate data context for test execution.
   */
  @Nonnull
  @Exclude
  Function<RuntimeContext, ResourceResolver> resolverFactory;

  /**
   * Optional exclusion configuration that defines how this test should behave if it's marked as
   * excluded. Excluded tests may be expected to fail or throw errors. This is excluded from
   * equals/hashCode as it is test execution metadata.
   */
  @Nonnull
  @Exclude
  Optional<ExcludeRule> exclusion;

  /**
   * Executes the test case with the provided resolver builder. This is the main entry point for
   * test execution and handles both normal test execution and excluded test scenarios.
   * <p>
   * For excluded tests, this method will catch expected failures or errors and validate that they
   * match the expected exclusion outcome. If an excluded test passes when it should fail, or fails
   * in an unexpected way, an AssertionError will be thrown.
   * <p>
   * For normal tests, this method delegates to {@link #doCheck(ResolverBuilder)} and allows any
   * exceptions to propagate normally.
   *
   * @param rb the resolver builder used to create the data context for test execution. Must not be
   * null.
   * @throws AssertionError if the test fails validation, or if an excluded test doesn't behave
   * according to its exclusion configuration
   * @throws Exception if an unexpected error occurs during test execution
   * @see ExcludeRule
   */
  @Override
  public void check(@Nonnull final ResolverBuilder rb) {
    if (exclusion.isPresent()) {
      // Handle excluded tests - these may be expected to fail or throw errors.
      final String outcome = exclusion.get().getOutcome();
      try {
        doCheck(rb);
      } catch (final TestAbortedException e) {
        // Let TestAbortedException propagate normally, this would have been thrown from a test
        // that expected an error but skipped the test instead (e.g. due to unsupported features).
        throw e;
      } catch (final UnsupportedFhirPathFeatureError e) {
        skipDueToUnsupportedFeature(e);
      } catch (final Exception e) {
        // Check if this error was expected for excluded tests.
        if (ExcludeRule.OUTCOME_ERROR.equals(outcome)) {
          log.info("Successfully caught expected error in excluded test: {}",
              e.getMessage());
          return;
        }
        throw e;
      } catch (final AssertionError e) {
        // Check if this failure was expected for excluded tests.
        if (ExcludeRule.OUTCOME_FAILURE.equals(outcome)) {
          log.info("Successfully caught expected failure in excluded test: {}",
              e.getMessage());
          return;
        }
        throw e;
      }
      if (!ExcludeRule.OUTCOME_PASS.equals(outcome)) {
        // If we get here, the excluded test passed when it shouldn't have.
        throw new AssertionError("Excluded test passed when expected outcome was " + outcome);
      }
    } else {
      // Normal test execution.
      try {
        doCheck(rb);
      } catch (final UnsupportedFhirPathFeatureError e) {
        skipDueToUnsupportedFeature(e);
      }
    }
  }

  /**
   * Performs the actual test execution logic without exclusion handling. This method determines the
   * type of test case and delegates to the appropriate verification method.
   * <p>
   * The three types of test cases handled are:
   * <ul>
   *   <li><strong>Error tests</strong> - {@link #verifyError(FhirpathEvaluator)}</li>
   *   <li><strong>Expression-only tests</strong> - {@link #verifyEvaluation(FhirpathEvaluator)}</li>
   *   <li><strong>Result comparison tests</strong> - {@link #verifyExpectedResult(FhirpathEvaluator)}</li>
   * </ul>
   *
   * @param rb the resolver builder used to create the ResourceResolver for the evaluator. Must not
   * be null.
   * @throws Exception if an error occurs during test execution or result validation
   */
  private void doCheck(@Nonnull final ResolverBuilder rb) {
    // Create the FHIRPath evaluator with the provided resolver.
    final FhirpathEvaluator.FhirpathEvaluatorBuilder builder = FhirpathEvaluator
        .fromResolver(rb.create(resolverFactory));

    // If the test specification has variables, convert them to FHIRPath collections
    // and add them to the evaluator for use in expression evaluation.
    if (spec.variables() != null) {
      builder.variables(toVariableCollections(spec.variables()));
    }

    // Build the evaluator and determine which type of test to perform.
    final FhirpathEvaluator evaluator = builder.build();
    if (spec.isError()) {
      // Test expects an error to be thrown.
      verifyError(evaluator);
    } else if (spec.isExpressionOnly()) {
      // Test only needs to verify the expression can be parsed and evaluated.
      verifyEvaluation(evaluator);
    } else {
      // Test needs to verify the expression evaluates to a specific result.
      verifyExpectedResult(evaluator);
    }
  }

  /**
   * Verifies that a FHIRPath expression throws the expected error. This method attempts to evaluate
   * the expression and expects an exception to be thrown. If no exception is thrown, the test
   * fails. If an exception is thrown, it validates that the error message matches the expected
   * error message (unless ANY_ERROR is specified).
   *
   * @param evaluator the FHIRPath evaluator to use for expression evaluation. Must not be null.
   * @throws AssertionError if no error is thrown when one was expected, or if the error message
   * doesn't match the expected message
   */
  private void verifyError(@Nonnull final FhirpathEvaluator evaluator) {
    try {
      // Attempt to evaluate the expression - this should throw an exception.
      final Collection evalResult = verifyEvaluation(evaluator);

      // Extract the actual result for error reporting.
      final ColumnRepresentation actualRepresentation = evalResult.getColumn().asCanonical();
      final Row resultRow = evaluator.createInitialDataset().select(
          actualRepresentation.getValue().alias("actual")
      ).first();
      final Object actual = getResult(resultRow, 0);

      // Fail the test since we expected an error but got a result.
      throw new AssertionError(
          String.format(
              "Expected an error but received a valid result: %s (Expression result: %s)",
              actual, evalResult));
    } catch (final UnsupportedFhirPathFeatureError e) {
      skipDueToUnsupportedFeature(e);
    } catch (final Exception e) {
      // An exception was thrown as expected - now validate the error message.
      log.trace("Received expected error: {}", e.toString());
      final String rootCauseMsg = ExceptionUtils.getRootCause(e).getMessage();
      log.debug("Expected error message: '{}', got: {}", spec.errorMsg(),
          rootCauseMsg);

      // Only check the specific error message if it's not the wildcard ANY_ERROR.
      if (!ANY_ERROR.equals(spec.errorMsg())) {
        assertEquals(spec.errorMsg(), rootCauseMsg);
      }
    }
  }

  private static void skipDueToUnsupportedFeature(final UnsupportedFhirPathFeatureError e) {
    throw new TestAbortedException(
        "This test has been skipped because one or more features required by it are not supported",
        e);
  }

  /**
   * Verifies that a FHIRPath expression can be successfully parsed and evaluated without throwing
   * exceptions. This method is used for expression-only tests that don't need to verify specific
   * results, only that the expression is syntactically valid and can be evaluated.
   *
   * @param evaluator the FHIRPath evaluator to use for expression evaluation. Must not be null.
   * @return the Collection result of evaluating the FHIRPath expression
   */
  @Nonnull
  private Collection verifyEvaluation(@Nonnull final FhirpathEvaluator evaluator) {
    // Parse the FHIRPath expression from the test specification.
    final FhirPath fhirPath = PARSER.parse(spec.expression());
    log.trace("FhirPath expression: {}", fhirPath);

    // Evaluate the parsed expression.
    final Collection result = evaluator.evaluate(fhirPath);
    log.trace("Evaluation result: {}", result);

    // Test passes if no exception is thrown during parsing and evaluation.
    return result;
  }

  /**
   * Verifies that a FHIRPath expression evaluates to the expected result. This method performs a
   * complete evaluation and comparison, checking that the actual result matches the expected result
   * defined in the test specification.
   *
   * @param evaluator the FHIRPath evaluator to use for expression evaluation. Must not be null.
   */
  private void verifyExpectedResult(@Nonnull final FhirpathEvaluator evaluator) {
    // Evaluate the expression to get the actual result.
    final Collection evalResult = verifyEvaluation(evaluator);

    // Get column representations for both actual and expected results.
    final ColumnRepresentation actualRepresentation = evalResult.getColumn().asCanonical();
    final ColumnRepresentation expectedRepresentation = getResultRepresentation();

    // Create a single row with both actual and expected values for comparison.
    final Row resultRow = evaluator.createInitialDataset().select(
        actualRepresentation.getValue().alias("actual"),
        expectedRepresentation.getValue().alias("expected")
    ).first();

    // Extract the actual values from the Spark Row.
    final Object actual = getResult(resultRow, 0);
    final Object expected = getResult(resultRow, 1);

    // Log detailed information for debugging.
    log.trace("Result schema: {}", resultRow.schema().treeString());
    log.debug("Comparing results - Expected: {} | Actual: {}", expected, actual);

    // Assert that the results match.
    assertEquals(expected, actual,
        String.format("Expression evaluation mismatch for '%s'. Expected: %s, but got: %s",
            spec.expression(), expected, actual));
  }

  @Nonnull
  @Override
  public String toString() {
    return spec.toString();
  }

  /**
   * Creates a column representation for the expected result defined in the test specification. This
   * method converts the YAML-defined expected result into a Spark column that can be used for
   * comparison with the actual evaluation result.
   *
   * @return a ColumnRepresentation that can be used to extract the expected result value
   */
  @Nonnull
  private ColumnRepresentation getResultRepresentation() {
    final Object result = requireNonNull(spec.result());

    // Handle single-item lists by unwrapping them to the contained value.
    final Object resultRepresentation = result instanceof final List<?> list && list.size() == 1
                                        ? list.get(0)
                                        : result;

    // Convert the YAML representation to a FHIR element definition.
    final ChildDefinition resultDefinition = YamlSupport.elementFromYaml("result",
        resultRepresentation);

    // Create a Spark schema from the element definition.
    final StructType resultSchema = YamlSupport.childrenToStruct(List.of(resultDefinition));

    // Convert to JSON representation for Spark processing.
    final String resultJson = YamlSupport.omToJson(
        Map.of("result", resultRepresentation));

    // Create and return the column representation.
    return new DefaultRepresentation(
        functions.from_json(functions.lit(resultJson),
            resultSchema).getField("result")).asCanonical();
  }

  @Override
  public void log(@Nonnull final Logger log) {
    // Log exclusion information if present.
    exclusion.ifPresent(s -> log.info("Exclusion: {}", s));

    // Log different information based on test type.
    if (spec.isError()) {
      log.info("assertError({}->'{}'}):[{}]", spec.expression(), spec.errorMsg(),
          spec.description());
    } else if (spec.isExpressionOnly()) {
      log.info("assertParseOnly({}):[{}]", spec.expression(), spec.description());
    } else {
      log.info("assertResult({}=({})):[{}]", spec.result(), spec.expression(),
          spec.description());
    }

    // Log resolver factory details at debug level.
    log.debug("Subject:\n{}", resolverFactory);
  }

  @Override
  @Nonnull
  public String getDescription() {
    return spec.description() != null
           ? spec.description()
           : spec.expression();
  }

  /**
   * Adjusts the type of a result value to ensure consistent comparison semantics. This method
   * handles type conversions that are necessary for proper result comparison in the Spark
   * environment.
   *
   * @param actualRaw the raw result value that may need type adjustment. Can be null.
   * @return the type-adjusted result value, or null if input was null
   */
  @Nullable
  private static Object adjustResultType(@Nullable final Object actualRaw) {
    if (actualRaw instanceof final Integer intValue) {
      // Convert Integer to Long for consistent numeric comparison.
      return intValue.longValue();
    } else if (actualRaw instanceof final BigDecimal bdValue) {
      // Scale BigDecimal to 6 places and convert to Long.
      return bdValue.setScale(6, RoundingMode.HALF_UP).longValue();
    } else {
      // Return other types unchanged.
      return actualRaw;
    }
  }

  /**
   * Extracts and processes a result value from a Spark Row at the specified index. This method
   * handles Spark-specific data types and applies necessary type adjustments for proper
   * comparison.
   *
   * @param row the Spark Row containing the result data. Must not be null.
   * @param index the column index to extract the value from. Must be a valid index.
   * @return the processed result value, which may be null
   */
  @Nullable
  private Object getResult(@Nonnull final Row row, final int index) {
    // Check for null values first.
    final Object actualRaw = row.isNullAt(index)
                             ? null
                             : row.get(index);

    if (actualRaw instanceof final WrappedArray<?> wrappedArray) {
      // Handle Spark WrappedArray - unwrap single-element arrays.
      return (wrappedArray.length() == 1
              ? adjustResultType(wrappedArray.apply(0))
              : wrappedArray);
    } else {
      // Apply type adjustments to non-array values.
      return adjustResultType(actualRaw);
    }
  }

  /**
   * Converts a map of variable names and values from the YAML test specification into a map of
   * FHIRPath literal Collections. This method enables YAML-defined variables to be used within
   * FHIRPath expressions during test execution.
   * <p>
   * <strong>Limitations:</strong>
   * </p>
   * <ul>
   *   <li>Multi-valued (list) variables with more than one element are not supported</li>
   *   <li>Complex object types are not supported</li>
   *   <li>Only single-valued variables can be used in FHIRPath expressions</li>
   * </ul>
   *
   * @param variables map of variable names to values from YAML specification. May be null (returns
   * empty map).
   * @return map of variable names to FHIRPath Collection objects. Never null.
   * @throws IllegalArgumentException if a variable is a list with more than one value, or if a
   * variable has an unsupported type
   */
  @Nonnull
  private static Map<String, Collection> toVariableCollections(
      final Map<String, Object> variables) {
    // Return empty map if input is null.
    if (variables == null) {
      return Map.of();
    }

    final Map<String, Collection> result = new HashMap<>();

    // Process each variable in the input map.
    for (final Map.Entry<String, Object> entry : variables.entrySet()) {
      final String key = entry.getKey();
      final Object value = entry.getValue();
      Object singleValue = value;

      // Handle list values - only single-valued lists are supported.
      if (value instanceof final List<?> l) {
        if (l.size() > 1) {
          // Multi-valued variables are not supported in the current implementation.
          throw new IllegalArgumentException(
              "Test runner does not support multi-valued variable collections for variable: "
                  + key);
        } else if (l.isEmpty()) {
          // Empty list maps to EmptyCollection.
          result.put(key, au.csiro.pathling.fhirpath.collection.EmptyCollection.getInstance());
          continue;
        } else {
          // Single-valued list: extract the single value.
          singleValue = l.get(0);
        }
      }

      // Convert the Java value to the appropriate FHIRPath Collection type.
      final Collection col;
      if (singleValue == null) {
        // Null values map to EmptyCollection.
        col = au.csiro.pathling.fhirpath.collection.EmptyCollection.getInstance();
      } else if (singleValue instanceof Integer) {
        col = IntegerCollection.fromValue((Integer) singleValue);
      } else if (singleValue instanceof String) {
        col = StringCollection.fromValue((String) singleValue);
      } else if (singleValue instanceof Boolean) {
        col = BooleanCollection.fromValue((Boolean) singleValue);
      } else if (singleValue instanceof Double || singleValue instanceof Float) {
        // Decimal values (Double/Float) map to DecimalCollection - convert to string representation 
        // for proper decimal handling.
        col = DecimalCollection.fromLiteral(singleValue.toString());
      } else {
        // Unsupported type - throw an informative error.
        throw new IllegalArgumentException(
            "Test runner does not support variable type for variable: '" + key + "' (type: "
                + singleValue.getClass().getSimpleName() + ")");
      }

      // Add the converted collection to the result map.
      result.put(key, col);
    }

    return result;
  }
}
