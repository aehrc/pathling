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

package au.csiro.pathling.io.transform;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests that a decimal is stored as the lexical form of the source (FR-002).
 *
 * <p>The lexical form survives only if nothing ever parses the value as a number: a trailing zero,
 * exponent notation, a leading sign and a digit count beyond any fixed-point type all have to come
 * back as they were written. The numeric annotation that supplies the value is deferred to M5, so
 * nothing here asserts one.
 */
class DecimalTransformTest {

  /**
   * The lexical forms the layout must preserve, keyed by the identifier of the resource carrying
   * them. A leading {@code +} is not tested because neither JSON nor the FHIR decimal regular
   * expression admits one.
   */
  @Nonnull private static final Map<String, String> LEXICAL_FORMS = lexicalForms();

  @Nonnull
  private static Map<String, String> lexicalForms() {
    final Map<String, String> forms = new LinkedHashMap<>();
    forms.put("trailing-zero", "1.50");
    forms.put("exponent", "1e2");
    forms.put("negative-exponent", "1.0e-7");
    forms.put("forty-digits", "1234567890123456789012345678901234567890.5");
    forms.put("small", "0.000000001");
    forms.put("negative", "-1.50");
    forms.put("integral", "100");
    return forms;
  }

  @Test
  void storesDecimalInTheLexicalFormOfTheSource(@TempDir @Nonnull final Path directory) {
    final String[] documents =
        LEXICAL_FORMS.entrySet().stream()
            .map(
                form ->
                    "{\"resourceType\":\"Observation\",\"id\":\""
                        + form.getKey()
                        + "\",\"status\":\"final\",\"valueQuantity\":{\"value\":"
                        + form.getValue()
                        + "}}")
            .toArray(String[]::new);
    final String path = TransformFixtures.corpus(directory, documents);

    final Dataset<Row> transformed =
        TransformFixtures.transformer().read(TransformFixtures.spark(), "Observation", path);

    final Map<String, String> stored =
        transformed.select("id", "valueQuantity.value").collectAsList().stream()
            .collect(Collectors.toMap(row -> row.getString(0), row -> row.getString(1)));
    assertEquals(LEXICAL_FORMS, new LinkedHashMap<>(stored));
  }

  @Test
  void storesDecimalAsText(@TempDir @Nonnull final Path directory) {
    final String path =
        TransformFixtures.corpus(
            directory,
            "{\"resourceType\":\"Observation\",\"id\":\"1\",\"status\":\"final\","
                + "\"valueQuantity\":{\"value\":1.50,\"unit\":\"mg\"}}");

    final Dataset<Row> transformed =
        TransformFixtures.transformer().read(TransformFixtures.spark(), "Observation", path);

    final StructType quantity = (StructType) transformed.schema().apply("valueQuantity").dataType();
    assertEquals(DataTypes.StringType, quantity.apply("value").dataType());
  }

  @Test
  void readsEveryPrimitiveAsTextSoThatNoNumberIsParsed(@TempDir @Nonnull final Path directory) {
    final String path =
        TransformFixtures.corpus(
            directory,
            "{\"resourceType\":\"Observation\",\"id\":\"1\",\"status\":\"final\","
                + "\"valueQuantity\":{\"value\":1.50}}");

    final Dataset<Row> source =
        TransformFixtures.spark().read().options(DecimalTransform.lexicalReadOptions()).json(path);

    final StructType quantity = (StructType) source.schema().apply("valueQuantity").dataType();
    assertEquals(DataTypes.StringType, quantity.apply("value").dataType());
    assertEquals("1.50", source.select("valueQuantity.value").first().getString(0));
  }

  @Test
  void storesEveryValueOfARepeatingDecimalLexically(@TempDir @Nonnull final Path directory) {
    final String path =
        TransformFixtures.corpus(
            directory,
            "{\"resourceType\":\"Observation\",\"id\":\"1\",\"status\":\"final\","
                + "\"component\":[{\"valueQuantity\":{\"value\":1.50}},"
                + "{\"valueQuantity\":{\"value\":1e2}}]}");

    final Dataset<Row> transformed =
        TransformFixtures.transformer().read(TransformFixtures.spark(), "Observation", path);

    final List<Row> components = transformed.first().getList(indexOf(transformed, "component"));
    assertTrue(components.size() == 2, "both components are stored");
    assertEquals(
        List.of("1.50", "1e2"),
        components.stream()
            .map(component -> component.<Row>getAs("valueQuantity").<String>getAs("value"))
            .toList());
  }

  private static int indexOf(@Nonnull final Dataset<Row> dataset, @Nonnull final String field) {
    return dataset.schema().fieldIndex(field);
  }
}
