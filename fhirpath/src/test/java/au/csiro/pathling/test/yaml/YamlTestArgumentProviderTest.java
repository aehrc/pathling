/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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
package au.csiro.pathling.test.yaml;

import static au.csiro.pathling.test.TestResources.getResourceAsString;
import static au.csiro.pathling.test.TestResources.getResourceAsUrl;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.yaml.YamlFhirPathTest;
import au.csiro.pathling.fhirpath.yaml.YamlReferenceImplTest;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.layout.PofDatasets;
import au.csiro.pathling.test.yaml.annotations.YamlTest;
import au.csiro.pathling.test.yaml.annotations.YamlTestConfiguration;
import au.csiro.pathling.test.yaml.executor.DefaultYamlTestExecutor;
import au.csiro.pathling.test.yaml.resolver.FhirResolverFactory;
import au.csiro.pathling.test.yaml.resolver.RuntimeContext;
import au.csiro.pathling.test.yaml.resolver.SuiteSchemas;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests that the YAML suites load their FHIR resources with the schemas of the suite (path 1 of
 * R11, FR-021b): every case of a suite whose subject or input file is a FHIR resource is loaded
 * with one set of suite schemas, which is inferred from every file of the suite's resource
 * directory and every inline subject of the suite that is a FHIR resource.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
class YamlTestArgumentProviderTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  @Nonnull
  static Stream<Arguments> suites() {
    return Stream.of(
        arguments(YamlReferenceImplTest.class, 19), arguments(YamlFhirPathTest.class, 4));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("suites")
  void everyFhirFactoryOfASuiteSharesOneSuiteSchemas(@Nonnull final Class<?> suiteClass) {
    // The fhirpath-ptl suite has one FHIR subject, so there this only checks the wiring.
    final Set<SuiteSchemas> shared = Collections.newSetFromMap(new IdentityHashMap<>());
    suiteSchemasOfEveryFactory(suiteClass).forEach(shared::add);

    assertThat(shared).hasSize(1);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("suites")
  void suiteInputsAreTheResourceFilesAndTheInlineFhirSubjects(
      @Nonnull final Class<?> suiteClass, final int resourceFiles) {
    final List<String> files = resourceFiles(suiteClass);
    assertThat(files).hasSize(resourceFiles);
    final List<String> expected =
        Stream.concat(files.stream(), inlineFhirSubjects(suiteClass).stream()).toList();

    assertThat(SuiteInputs.documentsOf(suiteClass)).containsExactlyInAnyOrderElementsOf(expected);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("suites")
  void sharedSuiteSchemasAreInferredFromTheSuiteInputs(@Nonnull final Class<?> suiteClass) {
    final SuiteSchemas shared = suiteSchemasOfEveryFactory(suiteClass).findFirst().orElseThrow();
    final List<String> patients =
        Stream.concat(resourceFiles(suiteClass).stream(), inlineFhirSubjects(suiteClass).stream())
            .filter(json -> "Patient".equals(resourceType(json)))
            .toList();
    assertThat(patients).isNotEmpty();

    final StructType schema = shared.schemaFor(RuntimeContext.of(spark, fhirEncoders), "Patient");

    assertThat(schema.catalogString()).isEqualTo(inferred(patients).catalogString());
  }

  /** The suite schemas of every FHIR resource factory the provider gives the suite's cases. */
  @Nonnull
  private static Stream<SuiteSchemas> suiteSchemasOfEveryFactory(
      @Nonnull final Class<?> suiteClass) {
    final List<SuiteSchemas> found =
        yamlTestMethods(suiteClass)
            .flatMap(method -> new YamlTestArgumentProvider().provideArguments(context(method)))
            .map(arguments -> arguments.get()[0])
            .filter(DefaultYamlTestExecutor.class::isInstance)
            .map(executor -> ((DefaultYamlTestExecutor) executor).getEvaluatorFactory())
            .filter(FhirResolverFactory.class::isInstance)
            .map(factory -> ((FhirResolverFactory) factory).getSuiteSchemas())
            .toList();
    assertThat(found).isNotEmpty();
    return found.stream();
  }

  @Nonnull
  private static ExtensionContext context(@Nonnull final Method method) {
    final ExtensionContext context = mock(ExtensionContext.class);
    when(context.getTestClass()).thenReturn(Optional.of(method.getDeclaringClass()));
    when(context.getTestMethod()).thenReturn(Optional.of(method));
    return context;
  }

  @Nonnull
  private static Stream<Method> yamlTestMethods(@Nonnull final Class<?> suiteClass) {
    return Stream.of(suiteClass.getDeclaredMethods())
        .filter(method -> method.isAnnotationPresent(YamlTest.class));
  }

  /** The contents of every file in the suite's resource directory. */
  @Nonnull
  private static List<String> resourceFiles(@Nonnull final Class<?> suiteClass) {
    final String base = suiteClass.getAnnotation(YamlTestConfiguration.class).resourceBase();
    try (final Stream<Path> files = Files.list(Path.of(getResourceAsUrl(base).toURI()))) {
      return files.sorted().map(YamlTestArgumentProviderTest::read).toList();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    } catch (final URISyntaxException e) {
      throw new IllegalStateException(e);
    }
  }

  /** The subject of each of the suite's YAML files that is a FHIR resource, as FHIR JSON. */
  @Nonnull
  private static List<String> inlineFhirSubjects(@Nonnull final Class<?> suiteClass) {
    return yamlTestMethods(suiteClass)
        .map(method -> method.getAnnotation(YamlTest.class).value())
        .map(location -> YamlTestDefinition.fromYaml(getResourceAsString(location)).subject())
        .filter(subject -> subject != null && isFhirResourceType(subject.get("resourceType")))
        .map(subject -> YamlSupport.omToJson(new HashMap<>(subject)))
        .toList();
  }

  private static boolean isFhirResourceType(final Object resourceType) {
    try {
      return resourceType instanceof final String code && FHIRDefinedType.fromCode(code) != null;
    } catch (final Exception e) {
      return false;
    }
  }

  /** The schema one inferred read of the resources gives, as the suites read them today. */
  @Nonnull
  private StructType inferred(@Nonnull final List<String> patients) {
    return PofDatasets.fromJson(spark, fhirEncoders.getContext(), "Patient", patients).schema();
  }

  @Nonnull
  private static String resourceType(@Nonnull final String json) {
    try {
      return MAPPER.readTree(json).path("resourceType").asText();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Nonnull
  private static String read(@Nonnull final Path file) {
    try {
      return Files.readString(file);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
