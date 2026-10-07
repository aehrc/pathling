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

package au.csiro.pathling.io;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests the public entry point for reading: that a format is selected by name or by media type, and
 * that every route refuses a type that may not be stored before anything is read (FR-007, decision
 * 84).
 */
class FhirReaderTest {

  /** A path that does not exist, so that a read which reached the files would fail differently. */
  @Nonnull private static final String MISSING = "/does/not/exist";

  @Test
  void selectsAFormatByItsMediaType() {
    final FhirReader reader = TransformFixtures.fhirReader();

    assertSame(reader.json(), reader.format("application/fhir+json"));
    assertSame(reader.xml(), reader.format("application/fhir+xml"));
  }

  @Test
  void refusesAMediaTypeNamingNeitherFormat() {
    final FhirReader reader = TransformFixtures.fhirReader();

    assertThrows(IllegalArgumentException.class, () -> reader.format("application/json"));
  }

  /**
   * Every route refuses {@code Bundle} and a name that is not a resource type. An {@link
   * IllegalArgumentException} rather than a failure to read shows that the refusal comes before
   * Spark is asked to read anything: the path does not exist, and the documents are not FHIR.
   */
  @ParameterizedTest(name = "{0} refuses {1}")
  @MethodSource("routesAndTypes")
  void refusesATypeThatMayNotBeStored(
      @Nonnull final String route,
      @Nonnull final String resourceType,
      @Nonnull final Consumer<String> read) {
    assertThrows(IllegalArgumentException.class, () -> read.accept(resourceType));
  }

  @Nonnull
  static Stream<Arguments> routesAndTypes() {
    final FhirReader reader = TransformFixtures.fhirReader();
    final Dataset<String> documents =
        TransformFixtures.spark().createDataset(List.of("not FHIR"), Encoders.STRING());
    final List<Arguments> routes =
        List.of(
            route("json files", type -> reader.json().read(type, MISSING)),
            route("json documents", type -> reader.json().read(type, documents)),
            route("json bundles", type -> reader.json().readBundles(type, documents)),
            route("xml documents", type -> reader.xml().read(type, documents)),
            route("xml bundles", type -> reader.xml().readBundles(type, documents)),
            route(
                "documents by media type",
                type -> reader.format("application/fhir+xml").read(type, documents)),
            route(
                "bundles by media type",
                type -> reader.format("application/fhir+json").readBundles(type, documents)));
    return routes.stream()
        .flatMap(
            route ->
                Stream.of("Bundle", "NotAResource")
                    .map(type -> Arguments.of(route.get()[0], type, route.get()[1])));
  }

  @Nonnull
  private static Arguments route(@Nonnull final String name, @Nonnull final Consumer<String> read) {
    return Arguments.of(name, read);
  }
}
