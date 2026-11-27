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

import au.csiro.pathling.test.yaml.annotations.YamlTest;
import au.csiro.pathling.test.yaml.annotations.YamlTestConfiguration;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * The input resources of a YAML suite (path 1 of R11, FR-021b), which its suite schemas are
 * inferred from. A suite is a test class: its inputs are every file in the resource directory its
 * {@code YamlTestConfiguration} names, and the subject of each of its YAML files that is a FHIR
 * resource.
 *
 * @author Piotr Szul
 */
public final class SuiteInputs {

  private SuiteInputs() {}

  /**
   * Returns the input resources of the suite a test class runs.
   *
   * @param suiteClass the test class
   * @return the suite's input resources, one FHIR JSON document each, of any type
   */
  @Nonnull
  public static List<String> documentsOf(@Nonnull final Class<?> suiteClass) {
    return Stream.concat(resourceFiles(suiteClass), inlineFhirSubjects(suiteClass)).toList();
  }

  /**
   * Returns whether a subject is a FHIR resource, which is when its resource type is the code of a
   * FHIR type. Any other subject is loaded through its own definition (path 2).
   *
   * @param subject the subject of a YAML file
   * @return true where the subject is a FHIR resource
   */
  public static boolean isFhirResource(@Nonnull final Map<Object, Object> subject) {
    if (!(subject.get("resourceType") instanceof final String code)) {
      return false;
    }
    try {
      return FHIRDefinedType.fromCode(code) != null;
    } catch (final Exception e) {
      return false;
    }
  }

  /**
   * Returns a subject that is a FHIR resource as the FHIR JSON it is loaded from.
   *
   * @param subject the subject of a YAML file
   * @return the subject as FHIR JSON
   */
  @Nonnull
  public static String toJson(@Nonnull final Map<Object, Object> subject) {
    return YamlSupport.omToJson(new HashMap<>(subject));
  }

  @Nonnull
  private static Stream<String> resourceFiles(@Nonnull final Class<?> suiteClass) {
    final String base = configuration(suiteClass).resourceBase();
    if (base.isEmpty()) {
      return Stream.empty();
    }
    try (final Stream<Path> files = Files.list(Path.of(getResourceAsUrl(base).toURI()))) {
      return files.filter(Files::isRegularFile).sorted().map(SuiteInputs::read).toList().stream();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    } catch (final URISyntaxException e) {
      throw new IllegalStateException(e);
    }
  }

  @Nonnull
  private static Stream<String> inlineFhirSubjects(@Nonnull final Class<?> suiteClass) {
    return Stream.of(suiteClass.getDeclaredMethods())
        .map(method -> method.getAnnotation(YamlTest.class))
        .filter(Objects::nonNull)
        .map(YamlTest::value)
        .map(location -> subjectOf(location))
        .filter(Objects::nonNull)
        .filter(SuiteInputs::isFhirResource)
        .map(SuiteInputs::toJson);
  }

  @Nullable
  private static Map<Object, Object> subjectOf(@Nonnull final String location) {
    return YamlTestDefinition.fromYaml(getResourceAsString(location)).subject();
  }

  @Nonnull
  private static YamlTestConfiguration configuration(@Nonnull final Class<?> suiteClass) {
    return Objects.requireNonNull(
        suiteClass.getAnnotation(YamlTestConfiguration.class),
        () -> suiteClass.getSimpleName() + " has no YamlTestConfiguration");
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
