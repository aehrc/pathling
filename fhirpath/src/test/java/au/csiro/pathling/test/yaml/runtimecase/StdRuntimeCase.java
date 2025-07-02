package au.csiro.pathling.test.yaml.runtimecase;

import static au.csiro.pathling.test.yaml.FhirPathTestSpec.TestCase.ANY_ERROR;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
import au.csiro.pathling.test.yaml.FhirPathTestSpec.TestCase;
import au.csiro.pathling.test.yaml.resolver.ResolverBuilder;
import au.csiro.pathling.test.yaml.RuntimeContext;
import au.csiro.pathling.test.yaml.TestConfig;
import au.csiro.pathling.test.yaml.YamlSupport;
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
import org.slf4j.Logger;
import scala.collection.mutable.WrappedArray;

/**
 * Standard implementation of RuntimeCase that handles the execution and validation of FHIRPath test
 * cases. This class is responsible for:
 * <ul>
 *   <li>Parsing and evaluating FHIRPath expressions</li>
 *   <li>Comparing actual results with expected outcomes</li>
 *   <li>Handling error cases and validation failures</li>
 *   <li>Providing detailed logging of test execution</li>
 * </ul>
 */
@Value(staticConstructor = "of")
@Slf4j
public class StdRuntimeCase implements RuntimeCase {

  private static final Parser PARSER = new Parser();

  @Nonnull
  TestCase spec;

  @Nonnull
  @Exclude
  Function<RuntimeContext, ResourceResolver> resolverFactory;

  @Nonnull
  @Exclude
  Optional<TestConfig.Exclude> exclusion;

  @Nonnull
  @Override
  public String toString() {
    return spec.toString();
  }

  @Nonnull
  private ColumnRepresentation getResultRepresentation() {
    final Object result = requireNonNull(spec.result());
    final Object resultRepresentation = result instanceof final List<?> list && list.size() == 1
                                        ? list.get(0)
                                        : result;
    final ChildDefinition resultDefinition = YamlSupport.elementFromYaml("result",
        resultRepresentation);
    final StructType resultSchema = YamlSupport.childrendToStruct(List.of(resultDefinition));
    final String resultJson = YamlSupport.omToJson(
        Map.of("result", resultRepresentation));
    return new DefaultRepresentation(
        functions.from_json(functions.lit(resultJson),
            resultSchema).getField("result")).asCanonical();
  }

  @Override
  public void log(@Nonnull final Logger log) {
    exclusion.ifPresent(s -> log.info("Exclusion: {}", s));
    if (spec.isError()) {
      log.info("assertError({}->'{}'}):[{}]", spec.expression(), spec.errorMsg(),
          spec.description());
    } else if (spec.isExpressionOnly()) {
      log.info("assertParseOnly({}):[{}]", spec.expression(), spec.description());
    } else {
      log.info("assertResult({}=({})):[{}]", spec.result(), spec.expression(),
          spec.description());
    }
    log.debug("Subject:\n{}", resolverFactory);
  }

  @Override
  @Nonnull
  public String getDescription() {
    return spec.description() != null
           ?
           spec.description()
           : spec.expression();
  }

  @Nullable
  private static Object adjustResultType(@Nullable final Object actualRaw) {
    if (actualRaw instanceof final Integer intValue) {
      return intValue.longValue();
    } else if (actualRaw instanceof final BigDecimal bdValue) {
      return bdValue.setScale(6, RoundingMode.HALF_UP).longValue();
    } else {
      return actualRaw;
    }
  }

  @Nullable
  private Object getResult(@Nonnull final Row row, final int index) {
    final Object actualRaw = row.isNullAt(index)
                             ? null
                             : row.get(index);
    if (actualRaw instanceof final WrappedArray<?> wrappedArray) {
      return (wrappedArray.length() == 1
              ? adjustResultType(wrappedArray.apply(0))
              : wrappedArray);
    } else {
      return adjustResultType(actualRaw);
    }
  }

  private void doCheck(@Nonnull final ResolverBuilder rb) {
    final FhirpathEvaluator.FhirpathEvaluatorBuilder builder = FhirpathEvaluator
        .fromResolver(rb.create(resolverFactory));

    // If the spec has variables, convert them to FHIRPath collections and add them to the
    // evaluator.
    if (spec.variables() != null) {
      builder.variables(toVariableCollections(spec.variables()));
    }

    // Depending on the type of test, we either expect an error, just parse the expression,
    // or evaluate it and compare the result.
    final FhirpathEvaluator evaluator = builder.build();
    if (spec.isError()) {
      verifyError(evaluator);
    } else if (spec.isExpressionOnly()) {
      verifyEvaluation(evaluator);
    } else {
      verifyExpectedResult(evaluator);
    }
  }

  private void verifyError(@Nonnull final FhirpathEvaluator evaluator) {
    try {
      final Collection evalResult = verifyEvaluation(evaluator);
      final ColumnRepresentation actualRepresentation = evalResult.getColumn().asCanonical();
      final Row resultRow = evaluator.createInitialDataset().select(
          actualRepresentation.getValue().alias("actual")
      ).first();
      final Object actual = getResult(resultRow, 0);
      throw new AssertionError(
          String.format(
              "Expected an error but received a valid result: %s (Expression result: %s)",
              actual, evalResult));
    } catch (final Exception e) {
      log.trace("Received expected error: {}", e.toString());
      final String rootCauseMsg = ExceptionUtils.getRootCause(e).getMessage();
      log.debug("Expected error message: '{}', got: {}", spec.errorMsg(),
          rootCauseMsg);
      if (!ANY_ERROR.equals(spec.errorMsg())) {
        assertEquals(spec.errorMsg(), rootCauseMsg);
      }
    }
  }

  private Collection verifyEvaluation(final FhirpathEvaluator evaluator) {
    final FhirPath fhirPath = PARSER.parse(spec.expression());
    log.trace("FhirPath expression: {}", fhirPath);

    final Collection result = evaluator.evaluate(fhirPath);
    log.trace("Evaluation result: {}", result);
    // Test passes if no exception is thrown during parsing and evaluation

    return result;
  }

  private void verifyExpectedResult(final FhirpathEvaluator evaluator) {
    final Collection evalResult = verifyEvaluation(evaluator);

    final ColumnRepresentation actualRepresentation = evalResult.getColumn().asCanonical();
    final ColumnRepresentation expectedRepresentation = getResultRepresentation();

    final Row resultRow = evaluator.createInitialDataset().select(
        actualRepresentation.getValue().alias("actual"),
        expectedRepresentation.getValue().alias("expected")
    ).first();

    final Object actual = getResult(resultRow, 0);
    final Object expected = getResult(resultRow, 1);

    log.trace("Result schema: {}", resultRow.schema().treeString());
    log.debug("Comparing results - Expected: {} | Actual: {}", expected, actual);
    assertEquals(expected, actual,
        String.format("Expression evaluation mismatch for '%s'. Expected: %s, but got: %s",
            spec.expression(), expected, actual));
  }

  @Override
  public void check(@Nonnull final ResolverBuilder rb) {
    if (exclusion.isPresent()) {
      final String outcome = exclusion.get().getOutcome();
      try {
        doCheck(rb);
      } catch (final Exception e) {
        if (TestConfig.Exclude.OUTCOME_ERROR.equals(outcome)) {
          log.info("Successfully caught expected error in excluded test: {}",
              e.getMessage());
          return;
        }
        throw e;
      } catch (final AssertionError e) {
        if (TestConfig.Exclude.OUTCOME_FAILURE.equals(outcome)) {
          log.info("Successfully caught expected failure in excluded test: {}",
              e.getMessage());
          return;
        }
        throw e;
      }
      throw new AssertionError("Excluded test passed when expected outcome was " + outcome);
    } else {
      doCheck(rb);
    }
  }

  /**
   * Converts a map of variable names and values from the YAML test spec into a map of FHIRPath
   * literal Collections. Only single-valued variables are supported. Multi-valued (list) variables
   * will throw an error.
   *
   * @param variables Map of variable names to values (may be null, single value, or list)
   * @return Map of variable names to {@link Collection} objects
   * @throws IllegalArgumentException if a variable is a list with more than one value, or
   * unsupported type
   */
  @Nonnull
  private static Map<String, Collection> toVariableCollections(
      final Map<String, Object> variables) {
    // Return empty map if input is null.
    if (variables == null) {
      return Map.of();
    }
    final Map<String, Collection> result = new HashMap<>();
    for (final Map.Entry<String, Object> entry : variables.entrySet()) {
      final String key = entry.getKey();
      final Object value = entry.getValue();
      Object singleValue = value;
      // If the value is a list, only allow single-valued lists.
      if (value instanceof final List<?> l) {
        if (l.size() > 1) {
          // Multi-valued variables are not supported.
          throw new IllegalArgumentException(
              "Test runner does not support multi-valued variable collections for variable: "
                  + key);
        } else if (l.isEmpty()) {
          // Empty list: use EmptyCollection.
          result.put(key, au.csiro.pathling.fhirpath.collection.EmptyCollection.getInstance());
          continue;
        } else {
          // Single-valued list: extract the value.
          singleValue = l.get(0);
        }
      }
      final Collection col;
      // Map Java types to FHIRPath Collection types.
      if (singleValue == null) {
        col = au.csiro.pathling.fhirpath.collection.EmptyCollection.getInstance();
      } else if (singleValue instanceof Integer) {
        col = IntegerCollection.fromValue((Integer) singleValue);
      } else if (singleValue instanceof String) {
        col = StringCollection.fromValue((String) singleValue);
      } else if (singleValue instanceof Boolean) {
        col = BooleanCollection.fromValue((Boolean) singleValue);
      } else if (singleValue instanceof Double || singleValue instanceof Float) {
        // For decimals, convert to string and use fromLiteral.
        col = DecimalCollection.fromLiteral(singleValue.toString());
      } else {
        // Unsupported type.
        throw new IllegalArgumentException(
            "Test runner does not support variable type for variable: '" + key + "' (type: "
                + singleValue.getClass().getSimpleName() + ")");
      }
      // Add the collection to the result map.
      result.put(key, col);
    }
    return result;
  }
}
