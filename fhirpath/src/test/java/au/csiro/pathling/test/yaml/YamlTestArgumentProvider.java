package au.csiro.pathling.test.yaml;

import static au.csiro.pathling.test.TestResources.getResourceAsString;

import au.csiro.pathling.fhirpath.context.ResourceResolver;
import au.csiro.pathling.test.TestResources;
import au.csiro.pathling.test.yaml.YamlTestDefinition.TestCase;
import au.csiro.pathling.test.yaml.annotations.YamlTest;
import au.csiro.pathling.test.yaml.annotations.YamlTestConfiguration;
import au.csiro.pathling.test.yaml.executor.DefaultYamlTestExecutor;
import au.csiro.pathling.test.yaml.executor.EmptyYamlTestExecutor;
import au.csiro.pathling.test.yaml.executor.YamlTestExecutor;
import au.csiro.pathling.test.yaml.format.ExcludeRule;
import au.csiro.pathling.test.yaml.format.YamlTestFormat;
import au.csiro.pathling.test.yaml.resolver.ArbitraryObjectResolverFactory;
import au.csiro.pathling.test.yaml.resolver.EmptyResolverFactory;
import au.csiro.pathling.test.yaml.resolver.FhirResolverFactory;
import au.csiro.pathling.test.yaml.resolver.RuntimeContext;
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
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

/**
 * Provides test arguments for parameterized test execution. This provider handles loading and
 * processing of YAML test specifications, configuration management, and test case creation.
 */
@Slf4j
public class YamlTestArgumentProvider implements ArgumentsProvider {

  @Override
  public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
    final TestConfiguration config = loadTestConfiguration(context);
    final YamlTestDefinition spec = loadTestSpec(context);
    final Function<RuntimeContext, ResourceResolver> defaultResolverFactory = createDefaultResolverFactory(
        spec);

    return createTestCases(spec, config, defaultResolverFactory);
  }

  private TestConfiguration loadTestConfiguration(final ExtensionContext context) {
    final boolean exclusionsOnly = "true".equals(System.getProperty(
        YamlTestBase.PROPERTY_EXCLUSIONS_ONLY));
    if (exclusionsOnly) {
      log.warn("Running excluded tests only (system property '{}' is set)",
          YamlTestBase.PROPERTY_EXCLUSIONS_ONLY);
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
    return Optional.ofNullable(System.getProperty(YamlTestBase.PROPERTY_DISABLED_EXCLUSIONS))
        .stream()
        .flatMap(s -> Stream.of(s.split(",")))
        .map(String::trim)
        .filter(s -> !s.isBlank())
        .collect(Collectors.toUnmodifiableSet());
  }

  private Optional<String> getTestConfigPath(final ExtensionContext context) {
    return context.getTestClass()
        .flatMap(c -> Optional.ofNullable(c.getAnnotation(YamlTestConfiguration.class)))
        .map(YamlTestConfiguration::config)
        .filter(s -> !s.isBlank());
  }

  private Optional<String> getResourceBase(final ExtensionContext context) {
    return context.getTestClass()
        .flatMap(c -> Optional.ofNullable(c.getAnnotation(YamlTestConfiguration.class)))
        .map(YamlTestConfiguration::resourceBase)
        .filter(s -> !s.isBlank());
  }

  private YamlTestDefinition loadTestSpec(final ExtensionContext context) {
    final String yamlSpecLocation = context.getTestMethod()
        .orElseThrow(() -> new IllegalStateException("Test method not found in context"))
        .getAnnotation(YamlTest.class)
        .value();

    log.debug("Loading test specification from: {}", yamlSpecLocation);
    final String testSpec = getResourceAsString(yamlSpecLocation);
    return YamlTestDefinition.fromYaml(testSpec);
  }

  private Function<RuntimeContext, ResourceResolver> createDefaultResolverFactory(
      final YamlTestDefinition spec) {
    return Optional.ofNullable(spec.subject())
        .map(subject -> {
          final Map<Object, Object> convertedSubject = new HashMap<>(
              subject);
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
        log.debug(
            "Invalid FHIR resource type '{}', falling back to ArbitraryObjectResolverFactory",
            resourceTypeStr);
      }
    }
    return ArbitraryObjectResolverFactory.of(subject);
  }

  private Stream<Arguments> createTestCases(
      final YamlTestDefinition spec,
      final TestConfiguration config,
      final Function<RuntimeContext, ResourceResolver> defaultResolverFactory) {

    final List<Arguments> cases = spec.cases()
        .stream()
        .filter(this::filterDisabledTests)
        .map(testCase -> createRuntimeCase(testCase, config, defaultResolverFactory))
        .map(Arguments::of)
        .toList();

    return cases.isEmpty()
           ? Stream.of(Arguments.of(EmptyYamlTestExecutor.of()))
           : cases.stream();
  }

  private boolean filterDisabledTests(final TestCase testCase) {
    if (testCase.disable()) {
      log.warn("Skipping disabled test case: {}", testCase);
      return false;
    }
    return true;
  }

  private YamlTestExecutor createRuntimeCase(
      final TestCase testCase,
      final TestConfiguration config,
      final Function<RuntimeContext, ResourceResolver> defaultResolverFactory) {

    final Function<RuntimeContext, ResourceResolver> resolverFactory = Optional.ofNullable(
            testCase.inputFile())
        .map(f -> createFileBasedResolver(f, config.resourceBase()))
        .orElse(defaultResolverFactory);

    return DefaultYamlTestExecutor.of(
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
    private Function<TestCase, Optional<ExcludeRule>> excluder() {
      final YamlTestFormat config = configPath
          .map(TestResources::getResourceAsString)
          .map(YamlTestFormat::fromYaml)
          .orElse(YamlTestFormat.getDefault());
      return config.toExcluder(disabledExclusionIds);
    }
  }
}
