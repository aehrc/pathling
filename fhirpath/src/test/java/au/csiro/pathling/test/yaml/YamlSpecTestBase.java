package au.csiro.pathling.test.yaml;

import static au.csiro.pathling.test.TestResources.getResourceAsString;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.EvalOptions;
import au.csiro.pathling.fhirpath.FhirPath;
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
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.TestResources;
import au.csiro.pathling.test.yaml.FhipathTestSpec.TestCase;
import ca.uhn.fhir.parser.IParser;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode.Exclude;
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
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import scala.collection.mutable.WrappedArray;


@SpringBootUnitTest
@Slf4j
public abstract class YamlSpecTestBase {

  public static final String PROPERTY_DISABLED_EXCLUSIONS = "au.csiro.pathling.test.yaml.disabledExclusions";
  public static final String PROPERTY_EXCLUSIONS_ONLY = "au.csiro.pathling.test.yaml.exclusionsOnly";

  @Autowired
  SparkSession spark;

  @Autowired
  FhirEncoders fhirEncoders;


  @FunctionalInterface
  protected interface ResolverBuilder {

    @Nonnull
    ResourceResolver create(
        @Nonnull final Function<RuntimeContext, ResourceResolver> resolveFactory);
  }

  @Value(staticConstructor = "of")
  protected static class RuntimeContext implements ResolverBuilder {

    @Nonnull
    SparkSession spark;
    @Nonnull
    FhirEncoders fhirEncoders;

    @Override
    @Nonnull
    public ResourceResolver create(
        @Nonnull final Function<RuntimeContext, ResourceResolver> resolveFactory) {
      return resolveFactory.apply(this);
    }
  }

  protected interface RuntimeCase {

    void log(@Nonnull Logger log);

    void check(@Nonnull final ResolverBuilder rb);
  }

  @Value(staticConstructor = "of")
  protected static class NoTestRuntimeCase implements RuntimeCase {

    public void log(@Nonnull Logger log) {
      log.info("No tests");
    }

    @Override
    public void check(@Nonnull final ResolverBuilder rb) {

    }
  }

  @Value(staticConstructor = "of")
  protected static class StdRuntimeCase implements RuntimeCase {

    private static final Parser PARSER = new Parser();

    @Nonnull
    FhipathTestSpec.TestCase spec;

    @Nonnull
    @Exclude
    Function<RuntimeContext, ResourceResolver> resolverFactory;

    @Nonnull
    @Exclude
    Optional<String> exclusion;

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

    @Override
    public void log(@Nonnull Logger log) {
      log.info("Description: {}", spec.getDescription());
      log.info("Expression: {}", spec.getExpression());
      exclusion.ifPresent(s -> log.info("Exclusion: {}", s));
      if (spec.isError()) {
        log.info("Expecting error");
      } else {
        log.info("Result: {}", spec.getResult());
      }
      log.debug("Subject:\n{}", resolverFactory);
    }

    @Nullable
    Object getResult(@Nonnull final Row row, final int index) {
      final Object actualRaw = row.isNullAt(index)
                               ? null
                               : row.get(index);
      if (actualRaw instanceof WrappedArray<?> wrappedArray) {
        return (wrappedArray.length() == 1
                ? wrappedArray.apply(0)
                : wrappedArray);
      } else if (actualRaw instanceof Integer intValue) {
        return intValue.longValue();
      } else if (actualRaw instanceof BigDecimal bdValue) {
        return bdValue.setScale(6, RoundingMode.HALF_UP).longValue();
      } else {
        return actualRaw;
      }
    }

    @Override
    public void check(@Nonnull final ResolverBuilder rb) {
      final FhirpathEvaluator evaluator = FhirpathEvaluator
          .fromResolver(rb.create(resolverFactory))
          .evalOptions(EvalOptions.builder().allowUndefinedFields(true).build())
          .build();
      if (spec.isError()) {
        try {
          final FhirPath fhipath = PARSER.parse(spec.getExpression());
          log.trace("FhirPath: {}", fhipath);
          final Collection evalResult = evaluator.evaluate(fhipath);
          log.trace("Evaluated: {}", evalResult);
          final ColumnRepresentation actualRepresentation = evalResult.getColumn().asCanonical();
          final Row resultRow = evaluator.createInitialDataset().select(
              actualRepresentation.getValue().alias("actual")
          ).first();
          final Object actual = getResult(resultRow, 0);
          throw new AssertionError(
              "Expected error but got value: " + actual + " (" + evalResult + ")");
        } catch (Exception e) {
          log.info("Expected error: {}", e.getMessage());
        }
      } else {
        final FhirPath fhipath = PARSER.parse(spec.getExpression());
        log.trace("FhirPath: {}", fhipath);
        final Collection evalResult = evaluator.evaluate(fhipath);
        log.trace("Evaluated: {}", evalResult);

        final ColumnRepresentation actualRepresentation = evalResult.getColumn().asCanonical();
        final ColumnRepresentation expectedRepresentation = getResultRepresentation();

        final Row resultRow = evaluator.createInitialDataset().select(
            actualRepresentation.getValue().alias("actual"),
            expectedRepresentation.getValue().alias("expected")
        ).first();

        final Object actual = getResult(resultRow, 0);
        final Object expected = getResult(resultRow, 1);

        log.trace("Result schema: {}", resultRow.schema().treeString());
        log.debug("Expected: " + expected + " but got: " + actual);
        assertEquals(expected, actual, "Expected: " + expected + " but got: " + actual);
      }
    }
  }


  @Value
  @AllArgsConstructor(access = lombok.AccessLevel.PRIVATE)
  static class EmptyResolverFactory implements Function<RuntimeContext, ResourceResolver> {


    // singleton
    private static final EmptyResolverFactory INSTANCE = new EmptyResolverFactory();

    static EmptyResolverFactory getInstance() {
      return INSTANCE;
    }

    @Override
    public ResourceResolver apply(final RuntimeContext runtimeContext) {

      DefResourceTag subjectResourceTag = DefResourceTag.of("Empty");
      return DefResourceResolver.of(
          subjectResourceTag,
          DefDefinitionContext.of(DefResourceDefinition.of(subjectResourceTag)),
          runtimeContext.getSpark().emptyDataFrame()
      );
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
      
      final String subjectOMJson = YamlSupport.omToJson(subjectOM);
      log.trace("subjectOMJson: \n" + subjectOMJson);
      final Dataset<Row> inputDS = rt.getSpark().read().schema(subjectSchema)
          .json(rt.getSpark().createDataset(List.of(subjectOMJson),
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

      final boolean exclusionsOnly = "true".equals(
          System.getProperty(PROPERTY_EXCLUSIONS_ONLY));
      if (exclusionsOnly) {
        log.warn(
            "Running excluded test only (`au.csiro.pathling.test.yaml.exclusionsOnly` is set)!!!");
      }

      final Set<String> disabledExlusionIds = Optional.ofNullable(
              System.getProperty(PROPERTY_DISABLED_EXCLUSIONS)).stream()
          .flatMap(s -> Stream.of(s.split(",")))
          .map(String::trim)
          .filter(s -> !s.isBlank())
          .collect(Collectors.toUnmodifiableSet());

      if (!disabledExlusionIds.isEmpty()) {
        log.warn("Disabling exclusions with ids: {}", disabledExlusionIds);
      }

      final Optional<String> testConfigPath = context.getTestClass()
          .flatMap(c -> Optional.ofNullable(c.getAnnotation(YamlConfig.class)))
          .map(YamlConfig::config)
          .filter(s -> !s.isBlank());

      final Optional<String> resourceBase = context.getTestClass()
          .flatMap(c -> Optional.ofNullable(c.getAnnotation(YamlConfig.class)))
          .map(YamlConfig::resourceBase)
          .filter(s -> !s.isBlank());

      testConfigPath.ifPresent(s -> log.info("Loading test config from: {}", s));
      resourceBase.ifPresent(s -> log.info("Resource base : {}", s));

      final TestConfig testConfig = testConfigPath
          .map(TestResources::getResourceAsString)
          .map(TestConfig::fromYaml)
          .orElse(TestConfig.getDefault());
      final Function<TestCase, Optional<String>> excluder = testConfig.toExcluder(
          disabledExlusionIds);

      final String yamlSpecLocation = context.getTestMethod().orElseThrow()
          .getAnnotation(YamlSpec.class)
          .value();
      log.debug("Loading test spec from: {}", yamlSpecLocation);
      final String testSpec = getResourceAsString(yamlSpecLocation);
      final FhipathTestSpec spec = FhipathTestSpec.fromYaml(testSpec);

      // create the default model (or not)

      final Function<RuntimeContext, ResourceResolver> defaultResolverFactory =
          Optional.ofNullable(spec.getSubject())
              .<Function<RuntimeContext, ResourceResolver>>map(OMResolverFactory::of)
              .orElse(EmptyResolverFactory.getInstance());

      List<Arguments> cases = spec.getCases()
          .stream()
          .filter(ts -> {
            if (ts.isDisable()) {
              log.warn("Disabling test case: {}", ts);
            }
            return !ts.isDisable();
          })
          .map(ts -> StdRuntimeCase.of(ts,
              Optional.ofNullable(ts.getInputFile())
                  .map(f -> (Function<RuntimeContext, ResourceResolver>) FhirResolverFactory.of(
                      getResourceAsString(resourceBase.orElse("") + File.separator + f)))
                  .orElse(defaultResolverFactory),
              excluder.apply(ts)
          ))
          .filter(rtc -> {
            if (exclusionsOnly) {
              return rtc.getExclusion().isPresent();
            } else {
              rtc.getExclusion()
                  .ifPresent(s -> log.warn("Excluding test case: {} becasue {}", rtc.getSpec(), s));
              return rtc.getExclusion().isEmpty();
            }
          })
          .map(Arguments::of)
          .toList();
      return cases.isEmpty()
             ? Stream.of(Arguments.of(NoTestRuntimeCase.of()))
             : cases.stream();
    }
  }

  @Nonnull
  protected ResolverBuilder createResolverBuilder() {
    return RuntimeContext.of(spark, fhirEncoders);
  }

  protected void run(@Nonnull final RuntimeCase testCase) {
    testCase.log(log);
    testCase.check(createResolverBuilder());
  }
}
