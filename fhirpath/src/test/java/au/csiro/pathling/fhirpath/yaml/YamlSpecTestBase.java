package au.csiro.pathling.fhirpath.yaml;

import static au.csiro.pathling.test.TestResources.getResourceAsString;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.column.ColumnRepresentation;
import au.csiro.pathling.fhirpath.column.DefaultRepresentation;
import au.csiro.pathling.fhirpath.context.ResourceResolver;
import au.csiro.pathling.fhirpath.definition.ChildDefinition;
import au.csiro.pathling.fhirpath.definition.def.DefDefinitionContext;
import au.csiro.pathling.fhirpath.definition.def.DefResourceDefinition;
import au.csiro.pathling.fhirpath.definition.def.DefResourceTag;
import au.csiro.pathling.fhirpath.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.fhirpath.definition.fhir.FhirResourceTag;
import au.csiro.pathling.fhirpath.execution.DefResourceResolver;
import au.csiro.pathling.fhirpath.execution.FhirpathEvaluator;
import au.csiro.pathling.fhirpath.execution.StdFhirpathEvaluator;
import au.csiro.pathling.fhirpath.function.registry.StaticFunctionRegistry;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.fhirpath.yaml.FhipathTestSpec.TestCase;
import au.csiro.pathling.test.SpringBootUnitTest;
import ca.uhn.fhir.parser.IParser;
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
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.springframework.beans.factory.annotation.Autowired;


@SpringBootUnitTest
@Slf4j
public abstract class YamlSpecTestBase {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirEncoders fhirEncoders;

  @Value(staticConstructor = "of")
  static class RuntimeContext {

    @Nonnull
    SparkSession spark;
    @Nonnull
    FhirEncoders fhirEncoders;
  }

  @Value(staticConstructor = "of")
  public static class RuntimeCase {

    private static final Parser PARSER = new Parser();

    @Nonnull
    FhipathTestSpec.TestCase spec;

    @Nonnull
    Function<RuntimeContext, ResourceResolver> resolverFactory;

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

    void check(@Nonnull final RuntimeContext rt) {
      final FhirpathEvaluator evaluator = new StdFhirpathEvaluator(
          resolverFactory.apply(rt),
          StaticFunctionRegistry.getInstance(),
          Map.of()
      );
      if (spec.isError()) {
        try {
          final Collection evalResult = evaluator.evaluate(PARSER.parse(spec.getExpression()));
          throw new AssertionError("Expected error but got none: " + evalResult);
        } catch (Exception e) {
          log.info("Expected error: {}", e.getMessage());
        }
      } else {
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

        log.info("Result schema: {}", resultRow.schema().treeString());
        log.debug("Expected: " + expected + " but got: " + actual);
        assertEquals(expected, actual, "Expected: " + expected + " but got: " + actual);
      }
    }
  }

  @Value(staticConstructor = "of")
  static class OMResolverFactory implements Function<RuntimeContext, ResourceResolver> {

    @Nonnull
    Map<Object, Object> subjectOM;

    @Override
    @Nonnull
    public ResourceResolver apply(@Nonnull final RuntimeContext rt) {

      final String subjectResourceCode = Optional.ofNullable(subjectOM.get("resourceType"))
          .map(String.class::cast)
          .orElse("Test");

      final DefResourceDefinition subjectDefinition = (DefResourceDefinition) YamlSupport
          .yamlToDefinition(subjectResourceCode, subjectOM);
      final StructType subjectSchema = YamlSupport.defnitiontoStruct(subjectDefinition);
      final Dataset<Row> inputDS = rt.getSpark().read().schema(subjectSchema)
          .json(rt.getSpark().createDataset(List.of(YamlSupport.omToJson(subjectOM)),
              Encoders.STRING()));

      log.trace("Yaml definition: {}", subjectDefinition);
      log.trace("Subject schema: {}", subjectSchema.treeString());

      return DefResourceResolver.of(
          DefResourceTag.of(subjectResourceCode),
          DefDefinitionContext.of(subjectDefinition),
          inputDS
      );
    }

    @Override
    @Nonnull
    public String toString() {
      return YamlSupport.YAML.dump(subjectOM);
    }
  }


  @Value(staticConstructor = "of")
  static class FhirResolverFactory implements Function<RuntimeContext, ResourceResolver> {

    @Nonnull
    String resourceJson;

    @Override
    @Nonnull
    public ResourceResolver apply(@Nonnull final RuntimeContext rt) {

      final IParser jsonParser = rt.getFhirEncoders().getContext().newJsonParser();
      final IBaseResource resource = jsonParser.parseResource(
          resourceJson);
      final Dataset<Row> resourceDS = rt.getSpark().createDataset(List.of(resource),
          rt.getFhirEncoders().of(resource.fhirType())).toDF();

      return DefResourceResolver.of(
          FhirResourceTag.of(ResourceType.fromCode(resource.fhirType())),
          FhirDefinitionContext.of(rt.getFhirEncoders().getContext()),
          resourceDS
      );
    }
  }

  static class FhirpathArgumentProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {

      final String testConfigYaml = getResourceAsString("fhirpath/config.yaml");
      final Function<TestCase, Optional<String>> excluder = TestConfig.fromYaml(
          testConfigYaml).toExcluder();

      final String yamlSpecLocation = context.getTestMethod().orElseThrow()
          .getAnnotation(YamlSpec.class)
          .value();
      log.debug("Loading test spec from: {}", yamlSpecLocation);
      final String testSpec = getResourceAsString(yamlSpecLocation);
      final FhipathTestSpec spec = FhipathTestSpec.fromYaml(testSpec);

      // create the default model (or not)

      final Map<Object, Object> subjectOM = spec.getSubject();
      final Function<RuntimeContext, ResourceResolver> defaultResolverFactory = OMResolverFactory.of(
          subjectOM);

      return spec.getCases()
          .stream()
          .filter(ts -> {
            final Optional<String> exclusion = excluder.apply(ts);
            exclusion.ifPresent(s -> log.info("Excluding test case: {} becasue {}", ts, s));
            return exclusion.isEmpty();
          }).map(ts -> RuntimeCase.of(ts,
              Optional.ofNullable(ts.getInputFile())
                  .map(f -> (Function<RuntimeContext, ResourceResolver>) FhirResolverFactory.of(
                      getResourceAsString("fhirpath/resources/" + f)))
                  .orElse(defaultResolverFactory)
          ))
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
    log.info("Subject:\n{}", testCase.resolverFactory);
    testCase.check(RuntimeContext.of(spark, fhirEncoders));
  }
}
