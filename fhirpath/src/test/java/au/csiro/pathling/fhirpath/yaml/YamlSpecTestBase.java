package au.csiro.pathling.fhirpath.yaml;

import static au.csiro.pathling.test.TestResources.getResourceAsString;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.context.ResourceResolver;
import au.csiro.pathling.fhirpath.definition.ChildDefinition;
import au.csiro.pathling.fhirpath.definition.DefinitionContext;
import au.csiro.pathling.fhirpath.definition.def.DefDefinitionContext;
import au.csiro.pathling.fhirpath.definition.def.DefResourceDefinition;
import au.csiro.pathling.fhirpath.definition.def.DefResourceTag;
import au.csiro.pathling.fhirpath.execution.DefResourceResolver;
import au.csiro.pathling.fhirpath.execution.FhirpathEvaluator;
import au.csiro.pathling.fhirpath.execution.StdFhirpathEvaluator;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.test.SpringBootUnitTest;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.springframework.beans.factory.annotation.Autowired;


@SpringBootUnitTest
@Slf4j
public abstract class YamlSpecTestBase {

  @Autowired
  SparkSession spark;


  @Value(staticConstructor = "of")
  public static class RuntimeCase {

    private static final Parser PARSER = new Parser();

    @Nonnull
    FhipathTestSpec.TestCase spec;

    @Nonnull
    Function<SparkSession, ? extends ResourceResolver> resolverFactory;

    @Nonnull
    @Override
    public String toString() {
      return spec.toString();
    }

    @Nonnull
    ColumnRepresentation getResultRepresentation() {
      final Object result = requireNonNull(spec.getResult());
      final Object resultRepresentation = result instanceof List<?> list && list.size() == 1
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

    void check(@Nonnull final SparkSession spark) {
      final FhirpathEvaluator evaluator = new StdFhirpathEvaluator(
          resolverFactory.apply(spark),
          StaticFunctionRegistry.getInstance(),
          Map.of()
      );
      final Collection evalResult = evaluator.evaluate(
          PARSER.parse(spec.getExpression()));

      final ColumnRepresentation actualRepresentation = evalResult.getColumn().asCanonical();
      final ColumnRepresentation expectedRepresentation = getResultRepresentation();

      final Row resultRow = evaluator.createInitialDataset().select(
          actualRepresentation.getValue().alias("actual"),
          expectedRepresentation.getValue().alias("expected")
      ).first();

      final Object actual = resultRow.isNullAt(0)
                            ? null
                            : resultRow.get(0);

      final Object expected = resultRow.isNullAt(1)
                              ? null
                              : resultRow.get(1);

      System.out.println("Expected: " + expected + " but got: " + actual);
      assertEquals(expected, actual, "Expected: " + expected + " but got: " + actual);
    }
  }

  static class FhirpathArgumentProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
      final String yamlSpecLocation = context.getTestMethod().orElseThrow()
          .getAnnotation(YamlSpec.class)
          .value();
      log.debug("Loading test spec from: {}", yamlSpecLocation);
      final String testSpec = getResourceAsString(yamlSpecLocation);
      final FhipathTestSpec spec = FhipathTestSpec.fromYaml(testSpec);

      // create the default model (or not)

      final Map<Object, Object> subjectOM = spec.getSubject();
      final String subjectResourceCode = Optional.ofNullable(subjectOM.get("resourceType"))
          .map(String.class::cast)
          .orElse("Test");
      final DefResourceDefinition subjectDefinition = (DefResourceDefinition) YamlSupport.yamlToDefinition(
          subjectResourceCode,
          subjectOM);
      final StructType subjectSchema = YamlSupport.defnitiontoStruct(subjectDefinition);
      final String subjectJson = YamlSupport.omToJson(subjectOM);
      final DefinitionContext definitionContext = DefDefinitionContext.of(subjectDefinition);

      log.trace("Yaml definition: {}", subjectDefinition);
      log.trace("Subject schema: {}", subjectSchema.treeString());
      log.trace("Subject json: {}", subjectJson);

      // so the first problem is here how do I create this dataset without spark sessio

      final Function<SparkSession, ? extends ResourceResolver> resolverFactory = sparkSession -> {
        final Dataset<Row> inputDS = sparkSession.read().schema(subjectSchema)
            .json(sparkSession.createDataset(List.of(subjectJson),
                Encoders.STRING()));
        return DefResourceResolver.of(
            DefResourceTag.of(subjectResourceCode),
            definitionContext,
            inputDS
        );
      };

      return spec.getCases()
          .stream()
          .map(ts -> RuntimeCase.of(ts, resolverFactory))
          .map(Arguments::of);
    }
  }

  protected void run(@Nonnull final RuntimeCase testCase) {
    log.info("Description: {}", testCase.spec.getDescription());
    log.info("Expression: {}", testCase.spec.getExpression());
    if (testCase.spec.isError()) {
      log.info("Expecting error");
    } else {
      log.info("Result: {}", testCase.spec.getResult());
    }
    testCase.check(spark);
  }

}
