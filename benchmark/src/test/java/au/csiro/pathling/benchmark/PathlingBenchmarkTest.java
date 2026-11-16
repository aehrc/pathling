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

package au.csiro.pathling.benchmark;

import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;

/**
 * Tests for the naming of the benchmarks in {@link PathlingBenchmark}.
 *
 * <p>Each benchmark takes no JMH parameters, and the state that it uses works out what to measure
 * from the name of the benchmark method. A method whose name does not follow the convention would
 * fail only after the setup of a benchmark run, so these tests check the convention without running
 * anything.
 *
 * @author John Grimes
 */
class PathlingBenchmarkTest {

  /** The state class that each phase prefix of a benchmark method name must be paired with. */
  private static final Map<String, Class<?>> PHASE_STATES =
      Map.of(
          "encode", PathlingBenchmarkEncodeState.class,
          "decode", PathlingBenchmarkDecodeState.class,
          "plan", PathlingBenchmarkViewState.class,
          "execute", PathlingBenchmarkViewState.class);

  // Parsing of benchmark names.

  @Test
  void subjectIsMethodNameAfterPhase() {
    // JMH identifies a benchmark by the fully qualified name of its method.
    assertEquals(
        "UsCoreBloodPressures",
        PathlingBenchmarkResources.subjectOf(
            "au.csiro.pathling.benchmark.PathlingBenchmark.executeUsCoreBloodPressures"));
  }

  @Test
  void subjectOfAcceptsUnqualifiedMethodName() {
    assertEquals("Patient", PathlingBenchmarkResources.subjectOf("encodePatient"));
  }

  @Test
  void subjectOfRejectsNameWithoutSubject() {
    // A method named after its phase alone does not say what it measures.
    assertThrows(
        IllegalArgumentException.class,
        () -> PathlingBenchmarkResources.subjectOf("au.csiro.pathling.benchmark.Foo.encode"));
  }

  @Test
  void subjectOfRejectsNameWithoutPhase() {
    // A method named after its subject alone does not say which phase it measures.
    assertThrows(
        IllegalArgumentException.class,
        () -> PathlingBenchmarkResources.subjectOf("au.csiro.pathling.benchmark.Foo.Patient"));
  }

  // The benchmarks declared in PathlingBenchmark.

  @Test
  void everyBenchmarkUsesTheStateOfItsPhase() {
    // A method that pairs a phase with the wrong state measures the wrong thing under its name.
    for (final Method method : benchmarkMethods()) {
      final Class<?> expectedState = PHASE_STATES.get(phaseOf(method));
      assertNotNull(expectedState, "Unknown phase in benchmark name: " + method.getName());
      assertEquals(
          List.of(expectedState),
          List.of(method.getParameterTypes()),
          "Wrong state for benchmark: " + method.getName());
    }
  }

  @Test
  void everyEncodeAndDecodeBenchmarkHasTestData() {
    for (final Method method : benchmarkMethods()) {
      final String phase = phaseOf(method);
      if (phase.equals("encode") || phase.equals("decode")) {
        final String subject = PathlingBenchmarkResources.subjectOf(method.getName());
        assertNotNull(
            getClass().getClassLoader().getResource("bulk/fhir/" + subject + ".ndjson"),
            "No test data for benchmark: " + method.getName());
      }
    }
  }

  @Test
  void everyViewBenchmarkHasViewDefinition() {
    for (final Method method : benchmarkMethods()) {
      final String phase = phaseOf(method);
      if (phase.equals("plan") || phase.equals("execute")) {
        final String subject = PathlingBenchmarkResources.subjectOf(method.getName());
        assertTrue(
            PathlingBenchmarkViewState.VIEW_SUBJECTS.containsKey(subject),
            "No subject resource for benchmark: " + method.getName());
        assertNotNull(
            getClass().getClassLoader().getResource(subject + ".json"),
            "No view definition for benchmark: " + method.getName());
      }
    }
  }

  @Test
  void everyViewIsPlannedAndExecuted() {
    // The combined score of each phase is only comparable between phases if they cover the same
    // views.
    assertEquals(PathlingBenchmarkViewState.VIEW_SUBJECTS.keySet(), subjectsOf("plan"));
    assertEquals(PathlingBenchmarkViewState.VIEW_SUBJECTS.keySet(), subjectsOf("execute"));
  }

  @Test
  void everyEncodedResourceTypeIsDecoded() {
    assertEquals(
        Set.of("Patient", "Condition", "Encounter", "Observation", "QuestionnaireResponse"),
        subjectsOf("encode"));
    assertEquals(subjectsOf("encode"), subjectsOf("decode"));
  }

  // Helpers.

  /**
   * Returns the benchmark methods declared in {@link PathlingBenchmark}.
   *
   * @return the methods annotated with {@link Benchmark}
   */
  private static List<Method> benchmarkMethods() {
    return Arrays.stream(PathlingBenchmark.class.getDeclaredMethods())
        .filter(method -> method.isAnnotationPresent(Benchmark.class))
        .toList();
  }

  /**
   * Returns the phase prefix of a benchmark method name.
   *
   * @param method the benchmark method
   * @return the leading lower-case word of the method name
   */
  private static String phaseOf(final Method method) {
    final String name = method.getName();
    final String subject = PathlingBenchmarkResources.subjectOf(name);
    return name.substring(0, name.length() - subject.length());
  }

  /**
   * Returns the subjects of the benchmarks of one phase.
   *
   * @param phase the phase prefix
   * @return the subjects of the benchmark methods with that prefix
   */
  private static Set<String> subjectsOf(final String phase) {
    return benchmarkMethods().stream()
        .filter(method -> phaseOf(method).equals(phase))
        .map(method -> PathlingBenchmarkResources.subjectOf(method.getName()))
        .collect(toSet());
  }
}
