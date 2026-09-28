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
package au.csiro.pathling.test.layout;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests that the layout and schema mode dimensions accept exactly the values they can honour, and
 * refuse every other request loudly rather than falling back to a default (T038).
 *
 * @author Piotr Szul
 */
class TestDimensionsTest {

  @Test
  void unsetLayoutIsTheNewLayout() {
    assertThat(TestLayout.parse(null)).isSameAs(TestLayout.POF);
    assertThat(TestLayout.fromProperties(new Properties())).isSameAs(TestLayout.POF);
  }

  @Test
  void knownLayoutsAreAccepted() {
    assertThat(TestLayout.parse("previous")).isSameAs(TestLayout.PREVIOUS);
    assertThat(TestLayout.parse("pof")).isSameAs(TestLayout.POF);
    assertThat(TestLayout.POF.isPof()).isTrue();
    assertThat(TestLayout.PREVIOUS.isPof()).isFalse();
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "true", "POF", "Previous", "new", "parquet", " pof"})
  void unknownLayoutFails(final String value) {
    assertThatThrownBy(() -> TestLayout.parse(value))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unknown test layout")
        .hasMessageContaining(TestLayout.PROPERTY);
  }

  @Test
  void unsetSchemaModeIsPruned() {
    assertThat(TestSchemaMode.parse(null)).isSameAs(TestSchemaMode.PRUNED);
    assertThat(TestSchemaMode.fromProperties(new Properties())).isSameAs(TestSchemaMode.PRUNED);
    assertThat(TestSchemaMode.parse("pruned")).isSameAs(TestSchemaMode.PRUNED);
  }

  @Test
  void denseSchemaModeFailsUntilItExists() {
    assertThatThrownBy(() -> TestSchemaMode.parse("dense"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not exist until M6 (T037b)");
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "true", "Pruned", "fitted", "sparse"})
  void unknownSchemaModeFails(final String value) {
    assertThatThrownBy(() -> TestSchemaMode.parse(value))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unknown schema mode")
        .hasMessageContaining(TestSchemaMode.PROPERTY);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "pathling.testlayout",
        "pathling.test.layout",
        "PATHLING.TESTLAYOUT",
        "pathling_testLayout"
      })
  void nearMissOfTheLayoutPropertyFails(final String name) {
    final Properties properties = new Properties();
    properties.setProperty(name, "pof");
    assertThatThrownBy(() -> TestLayout.fromProperties(properties))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(name)
        .hasMessageContaining(TestLayout.PROPERTY);
  }

  @ParameterizedTest
  @ValueSource(strings = {"pathling.testschemamode", "pathling.test.schemaMode"})
  void nearMissOfTheSchemaModePropertyFails(final String name) {
    final Properties properties = new Properties();
    properties.setProperty(name, "pruned");
    assertThatThrownBy(() -> TestSchemaMode.fromProperties(properties))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(name)
        .hasMessageContaining(TestSchemaMode.PROPERTY);
  }

  @Test
  void exactPropertyIsRead() {
    final Properties properties = new Properties();
    properties.setProperty(TestLayout.PROPERTY, "pof");
    properties.setProperty("pathling.testForkCount", "4");
    assertThat(TestLayout.fromProperties(properties)).isSameAs(TestLayout.POF);
  }
}
