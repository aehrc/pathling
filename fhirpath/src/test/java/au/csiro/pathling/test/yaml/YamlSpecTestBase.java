package au.csiro.pathling.test.yaml;

import static au.csiro.pathling.test.TestResources.getResourceAsString;
import static au.csiro.pathling.test.yaml.FhipathTestSpec.TestCase.ANY_ERROR;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.encoders.FhirEncoders;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode.Exclude;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.opentest4j.TestAbortedException;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import scala.collection.mutable.WrappedArray;


/**
 * Base class for YAML-based FHIRPath specification tests. This class provides the infrastructure
 * for running FHIRPath tests defined in YAML files, with support for test exclusions, resource
 * resolution, and result validation.
 * <p>
 * The class uses a combination of Spring Boot test infrastructure and custom test utilities to
 * execute FHIRPath expressions against test data and validate the results against expected
 * outcomes.
 */
@SpringBootUnitTest
@Slf4j
public abstract class YamlSpecTestBase {

  private static final String PROPERTY_DISABLED_EXCLUSIONS = "au.csiro.pathling.test.yaml.disabledExclusions";
  private static final String PROPERTY_EXCLUSIONS_ONLY = "au.csiro.pathling.test.yaml.exclusionsOnly";

  @Autowired
  protected SparkSession spark;

  @Autowired
  protected FhirEncoders fhirEncoders;


  /**
   * Interface for building resource resolvers with specific context.
   */
  @FunctionalInterface
  public interface ResolverBuilder {

    @Nonnull
    ResourceResolver create(
        @Nonnull final Function<RuntimeContext, ResourceResolver> resolveFactory);
  }

  /**
   * Represents a runtime context for test execution, providing access to Spark session and FHIR
   * encoders.
   */
  @Value(staticConstructor = "of")
  public static class RuntimeContext implements ResolverBuilder {

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

  /**
   * Simple implementation of RuntimeCase for scenarios where no tests are available. Provides a
   * minimal implementation that logs the absence of tests and performs no validation.
   */
  @Value(staticConstructor = "of")
  protected static class NoTestRuntimeCase implements RuntimeCase {

    public void log(@Nonnull final Logger log) {
      log.info("No tests");
    }

    @Override
    public void check(@Nonnull final ResolverBuilder rb) {
    }

    @Override
    @Nonnull
    public String getDescription() {
      return "none";
    }
  }

  /**
   * Standard implementation of RuntimeCase that handles the execution and validation of FHIRPath
   * test cases. This class is responsible for:
   * <ul>
   *   <li>Parsing and evaluating FHIRPath expressions</li>
   *   <li>Comparing actual results with expected outcomes</li>
   *   <li>Handling error cases and validation failures</li>
   *   <li>Providing detailed logging of test execution</li>
   * </ul>
   */
  @Value(staticConstructor = "of")
  public static class StdRuntimeCase implements RuntimeCase {

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
    private ColumnRepresentation getResultRepresentation() {
      final Object result = requireNonNull(spec.getResult());
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
        log.info("assertError({}->'{}'}):[{}]", spec.getExpression(), spec.getErrorMsg(),
            spec.getDescription());
      } else {
        log.info("assertResult({}=({})):[{}]", spec.getResult(), spec.getExpression(),
            spec.getDescription());
      }
      log.debug("Subject:\n{}", resolverFactory);
    }

    @Override
    @Nonnull
    public String getDescription() {
      return spec.getDescription() != null
             ?
             spec.getDescription()
             : spec.getExpression();
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

    @Override
    public void check(@Nonnull final ResolverBuilder rb) {
      final FhirpathEvaluator evaluator = FhirpathEvaluator
          .fromResolver(rb.create(resolverFactory))
          .build();
      if (spec.isError()) {
        try {
          final FhirPath fhirPath = PARSER.parse(spec.getExpression());
          log.trace("FhirPath expression: {}", fhirPath);
          final Collection evalResult = evaluator.evaluate(fhirPath);
          log.trace("Evaluation result: {}", evalResult);
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
          log.debug("Expected error message: '{}', got: {}", spec.getErrorMsg(), rootCauseMsg);
          if (!ANY_ERROR.equals(spec.getErrorMsg())) {
            assertEquals(spec.getErrorMsg(), rootCauseMsg);
          }
        }
      } else {
        final FhirPath fhirPath = PARSER.parse(spec.getExpression());
        log.trace("FhirPath expression: {}", fhirPath);
        final Collection evalResult = evaluator.evaluate(fhirPath);
        log.trace("Evaluation result: {}", evalResult);

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
                spec.getExpression(), expected, actual));
      }
    }
  }


  /**
   * Factory for creating empty resource resolvers. This implementation provides a resolver that
   * returns an empty DataFrame, useful for testing expressions that don't require input data.
   */
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

      final DefResourceTag subjectResourceTag = DefResourceTag.of("Empty");
      return DefResourceResolver.of(
          subjectResourceTag,
          DefDefinitionContext.of(DefResourceDefinition.of(subjectResourceTag)),
          runtimeContext.getSpark().emptyDataFrame()
      );
    }
  }

  /**
   * Factory for creating resource resolvers from Object Model representations. This class handles
   * the conversion of YAML-defined test data into a format that can be used for FHIRPath expression
   * evaluation.
   */
  @Value(staticConstructor = "of")
  public static class OMResolverFactory implements Function<RuntimeContext, ResourceResolver> {

    @Nonnull
    Map<Object, Object> subjectOM;  // Changed back to Map<Object, Object>

    @Override
    @Nonnull
    public ResourceResolver apply(final RuntimeContext rt) {
      final String subjectResourceCode = Optional.ofNullable(subjectOM.get("resourceType"))
          .map(String.class::cast)
          .orElse("Test");

      final DefResourceDefinition subjectDefinition = (DefResourceDefinition) YamlSupport
          .yamlToDefinition(subjectResourceCode, subjectOM);
      final StructType subjectSchema = YamlSupport.defnitiontoStruct(subjectDefinition);

      final String subjectOMJson = YamlSupport.omToJson(subjectOM);
      log.trace("subjectOMJson: \n{}", subjectOMJson);
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


  /**
   * Factory for creating resource resolvers from FHIR JSON resources. This implementation handles
   * the parsing and conversion of FHIR resources into a format suitable for FHIRPath expression
   * evaluation.
   */
  @Value(staticConstructor = "of")
  public static class FhirResolverFactory implements Function<RuntimeContext, ResourceResolver> {

    @Nonnull
    String resourceJson;

    @Override
    @Nonnull
    public ResourceResolver apply(final RuntimeContext rt) {

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

  /**
   * Factory for creating resource resolvers from HAPI FHIR resources. This implementation handles
   * the conversion of HAPI FHIR resource objects into a format suitable for FHIRPath expression
   * evaluation.
   */
  @Value(staticConstructor = "of")
  public static class HapiResolverFactory implements Function<RuntimeContext, ResourceResolver> {

    @Nonnull
    IBaseResource resource;

    @Override
    @Nonnull
    public ResourceResolver apply(final RuntimeContext rt) {
      final Dataset<Row> resourceDS = rt.getSpark().createDataset(List.of(resource),
          rt.getFhirEncoders().of(resource.fhirType())).toDF();

      return DefResourceResolver.of(
          FhirResourceTag.of(ResourceType.fromCode(resource.fhirType())),
          FhirDefinitionContext.of(rt.getFhirEncoders().getContext()),
          resourceDS
      );
    }
  }

  /**
   * Provides test arguments for parameterized test execution. This provider handles loading and
   * processing of YAML test specifications, configuration management, and test case creation.
   */
  static class FhirpathArgumentProvider implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
      final TestConfiguration config = loadTestConfiguration(context);
      final FhipathTestSpec spec = loadTestSpec(context);
      final Function<RuntimeContext, ResourceResolver> defaultResolverFactory = createDefaultResolverFactory(
          spec);

      return createTestCases(spec, config, defaultResolverFactory);
    }

    private TestConfiguration loadTestConfiguration(final ExtensionContext context) {
      final boolean exclusionsOnly = "true".equals(System.getProperty(PROPERTY_EXCLUSIONS_ONLY));
      if (exclusionsOnly) {
        log.warn("Running excluded tests only (system property '{}' is set)",
            PROPERTY_EXCLUSIONS_ONLY);
      }

      final Set<String> disabledExclusionIds = parseDisabledExclusions();
      if (!disabledExclusionIds.isEmpty()) {
        log.warn("Disabling exclusions with IDs: {}", disabledExclusionIds);
      }

      return new TestConfiguration(
          getTestConfigPath(context),
          getResourceBase(context),
          disabledExclusionIds,
          exclusionsOnly
      );
    }

    private Set<String> parseDisabledExclusions() {
      return Optional.ofNullable(System.getProperty(PROPERTY_DISABLED_EXCLUSIONS))
          .stream()
          .flatMap(s -> Stream.of(s.split(",")))
          .map(String::trim)
          .filter(s -> !s.isBlank())
          .collect(Collectors.toUnmodifiableSet());
    }

    private Optional<String> getTestConfigPath(final ExtensionContext context) {
      return context.getTestClass()
          .flatMap(c -> Optional.ofNullable(c.getAnnotation(YamlConfig.class)))
          .map(YamlConfig::config)
          .filter(s -> !s.isBlank());
    }

    private Optional<String> getResourceBase(final ExtensionContext context) {
      return context.getTestClass()
          .flatMap(c -> Optional.ofNullable(c.getAnnotation(YamlConfig.class)))
          .map(YamlConfig::resourceBase)
          .filter(s -> !s.isBlank());
    }

    private FhipathTestSpec loadTestSpec(final ExtensionContext context) {
      final String yamlSpecLocation = context.getTestMethod()
          .orElseThrow(() -> new IllegalStateException("Test method not found in context"))
          .getAnnotation(YamlSpec.class)
          .value();

      log.debug("Loading test specification from: {}", yamlSpecLocation);
      final String testSpec = getResourceAsString(yamlSpecLocation);
      return FhipathTestSpec.fromYaml(testSpec);
    }

    private Function<RuntimeContext, ResourceResolver> createDefaultResolverFactory(
        final FhipathTestSpec spec) {
      return Optional.ofNullable(spec.getSubject())
          .map(subject -> {
            @SuppressWarnings("unchecked")
            final Map<Object, Object> convertedSubject = new HashMap<>(subject);
            return createResolverFactoryFromSubject(convertedSubject);
          })
          .orElse(EmptyResolverFactory.getInstance());
    }

    private Function<RuntimeContext, ResourceResolver> createResolverFactoryFromSubject(
        final Map<Object, Object> subject) {
      final String resourceTypeStr = Optional.ofNullable(subject.get("resourceType"))
          .map(String.class::cast)
          .orElse(null);

      if (resourceTypeStr != null) {
        try {
          Objects.requireNonNull(FHIRDefinedType.fromCode(resourceTypeStr));
          final String jsonStr = YamlSupport.omToJson(subject);
          return FhirResolverFactory.of(jsonStr);
        } catch (final Exception e) {
          log.debug("Invalid FHIR resource type '{}', falling back to OMResolverFactory",
              resourceTypeStr);
        }
      }
      return OMResolverFactory.of(subject);
    }

    private Stream<Arguments> createTestCases(
        final FhipathTestSpec spec,
        final TestConfiguration config,
        final Function<RuntimeContext, ResourceResolver> defaultResolverFactory) {

      final List<Arguments> cases = spec.getCases()
          .stream()
          .filter(this::filterDisabledTests)
          .map(testCase -> createRuntimeCase(testCase, config, defaultResolverFactory))
          .map(Arguments::of)
          .toList();

      return cases.isEmpty()
             ? Stream.of(Arguments.of(NoTestRuntimeCase.of()))
             : cases.stream();
    }

    private boolean filterDisabledTests(final TestCase testCase) {
      if (testCase.isDisable()) {
        log.warn("Skipping disabled test case: {}", testCase);
        return false;
      }
      return true;
    }

    private RuntimeCase createRuntimeCase(
        final TestCase testCase,
        final TestConfiguration config,
        final Function<RuntimeContext, ResourceResolver> defaultResolverFactory) {

      final Function<RuntimeContext, ResourceResolver> resolverFactory = Optional.ofNullable(
              testCase.getInputFile())
          .map(f -> createFileBasedResolver(f, config.resourceBase()))
          .orElse(defaultResolverFactory);

      return StdRuntimeCase.of(
          testCase,
          resolverFactory,
          config.excluder().apply(testCase)
      );
    }

    private Function<RuntimeContext, ResourceResolver> createFileBasedResolver(
        final String inputFile, final Optional<String> resourceBase) {
      final String path = resourceBase.orElse("") + File.separator + inputFile;
      return FhirResolverFactory.of(getResourceAsString(path));
    }

    /**
     * Configuration record for test execution settings. Encapsulates:
     * <ul>
     *   <li>Test configuration file path</li>
     *   <li>Resource base directory</li>
     *   <li>Disabled exclusion IDs</li>
     *   <li>Exclusions-only mode flag</li>
     * </ul>
     */
    private record TestConfiguration(
        Optional<String> configPath,
        Optional<String> resourceBase,
        Set<String> disabledExclusionIds,
        boolean exclusionsOnly
    ) {

      private Function<TestCase, Optional<String>> excluder() {
        final TestConfig config = configPath
            .map(TestResources::getResourceAsString)
            .map(TestConfig::fromYaml)
            .orElse(TestConfig.getDefault());
        return config.toExcluder(disabledExclusionIds);
      }
    }
  }

  /**
   * Creates a new resolver builder instance for test execution.
   *
   * @return A new resolver builder configured with the current Spark session and FHIR encoders
   */
  @Nonnull
  protected ResolverBuilder createResolverBuilder() {
    return RuntimeContext.of(spark, fhirEncoders);
  }

  /**
   * Executes a runtime test case, handling logging and exclusion checks.
   *
   * @param testCase The test case to run
   * @throws TestAbortedException if the test case is excluded
   */
  public void run(@Nonnull final RuntimeCase testCase) {
    testCase.log(log);

    // Check if the test case is excluded and skip.
    if (testCase instanceof final StdRuntimeCase stdCase && stdCase.getExclusion().isPresent()) {
      throw new TestAbortedException(
          "Test case skipped due to exclusion: " + stdCase.getExclusion().get());
    }

    testCase.check(createResolverBuilder());
  }
}
