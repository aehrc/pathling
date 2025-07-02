package au.csiro.pathling.test.yaml;

import static au.csiro.pathling.test.TestResources.getResourceAsString;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.context.ResourceResolver;
import au.csiro.pathling.fhirpath.definition.def.DefDefinitionContext;
import au.csiro.pathling.fhirpath.definition.def.DefResourceDefinition;
import au.csiro.pathling.fhirpath.definition.def.DefResourceTag;
import au.csiro.pathling.fhirpath.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.fhirpath.definition.fhir.FhirResourceTag;
import au.csiro.pathling.fhirpath.execution.DefResourceResolver;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.TestResources;
import au.csiro.pathling.test.yaml.FhirPathTestSpec.TestCase;
import au.csiro.pathling.test.yaml.runtimecase.NoTestRuntimeCase;
import au.csiro.pathling.test.yaml.runtimecase.RuntimeCase;
import au.csiro.pathling.test.yaml.runtimecase.StdRuntimeCase;
import ca.uhn.fhir.parser.IParser;
import jakarta.annotation.Nonnull;
import java.io.File;
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
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.opentest4j.TestAbortedException;
import org.springframework.beans.factory.annotation.Autowired;


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
      final FhirPathTestSpec spec = loadTestSpec(context);
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

    private FhirPathTestSpec loadTestSpec(final ExtensionContext context) {
      final String yamlSpecLocation = context.getTestMethod()
          .orElseThrow(() -> new IllegalStateException("Test method not found in context"))
          .getAnnotation(YamlSpec.class)
          .value();

      log.debug("Loading test specification from: {}", yamlSpecLocation);
      final String testSpec = getResourceAsString(yamlSpecLocation);
      return FhirPathTestSpec.fromYaml(testSpec);
    }

    private Function<RuntimeContext, ResourceResolver> createDefaultResolverFactory(
        final FhirPathTestSpec spec) {
      return Optional.ofNullable(spec.subject())
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
        final FhirPathTestSpec spec,
        final TestConfiguration config,
        final Function<RuntimeContext, ResourceResolver> defaultResolverFactory) {

      final List<Arguments> cases = spec.cases()
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
      if (testCase.disable()) {
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
              testCase.inputFile())
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

      @Nonnull
      private Function<TestCase, Optional<TestConfig.Exclude>> excluder() {
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

    // Check if the test case is excluded and skip if no outcome is defined.
    if (testCase instanceof final StdRuntimeCase stdCase && stdCase.getExclusion().isPresent()
        && stdCase.getExclusion().get().getOutcome() == null) {
      throw new TestAbortedException(
          "Test case skipped due to exclusion: " + stdCase.getExclusion().get());
    }

    testCase.check(createResolverBuilder());
  }
}
