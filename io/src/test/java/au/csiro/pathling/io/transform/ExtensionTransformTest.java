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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests that extensions on complex elements are stored inline (FR-003).
 *
 * <p>Inline means the element the definitions describe, in the place the definitions describe it:
 * an {@code extension} field on the structure carrying the extension, recursing as far as the
 * source does. The previous layout hoisted them into a map at the root of the resource, keyed by a
 * field identifier carried on every composite; neither of those fields belongs to this layout, so
 * neither may appear in a stored schema.
 */
class ExtensionTransformTest {

  @Nonnull private static final String SIMPLE_URL = "http://example.org/simple";

  @Nonnull private static final String NESTED_URL = "http://example.org/nested";

  @Nonnull private static final String ON_COMPLEX_URL = "http://example.org/on-complex";

  @Nonnull
  private static final String PATIENT =
      "{\"resourceType\":\"Patient\",\"id\":\"1\","
          + "\"extension\":["
          + "{\"url\":\""
          + SIMPLE_URL
          + "\",\"valueString\":\"root\"},"
          + "{\"url\":\""
          + NESTED_URL
          + "\",\"extension\":[{\"url\":\"inner\",\"valueString\":\"deep\"}]}],"
          + "\"address\":[{\"city\":\"Adelaide\",\"extension\":["
          + "{\"url\":\""
          + ON_COMPLEX_URL
          + "\",\"valueString\":\"beside the city\"}]}]}";

  @Test
  void storesAnExtensionOnAComplexElementInline(@TempDir @Nonnull final Path directory) {
    final Dataset<Row> transformed = transform(directory);

    final StructType address = (StructType) elementType(transformed.schema(), "address");
    assertTrue(
        Stream.of(address.fieldNames()).anyMatch("extension"::equals),
        "the extension is a field of the element carrying it");

    final Row addressRow = transformed.first().<List<Row>>getAs("address").get(0);
    final List<Row> extensions = addressRow.getAs("extension");
    assertEquals(1, extensions.size());
    assertEquals(ON_COMPLEX_URL, extensions.get(0).<String>getAs("url"));
    assertEquals("beside the city", extensions.get(0).<String>getAs("valueString"));
  }

  @Test
  void storesAnExtensionOnAResourceInline(@TempDir @Nonnull final Path directory) {
    final Dataset<Row> transformed = transform(directory);

    final List<Row> extensions = transformed.first().getAs("extension");
    assertEquals(2, extensions.size());
    assertEquals(SIMPLE_URL, extensions.get(0).<String>getAs("url"));
    assertEquals("root", extensions.get(0).<String>getAs("valueString"));
  }

  @Test
  void storesAnExtensionOfAnExtensionInline(@TempDir @Nonnull final Path directory) {
    final Dataset<Row> transformed = transform(directory);

    final List<Row> nested =
        transformed.first().<List<Row>>getAs("extension").get(1).getAs("extension");
    assertEquals(1, nested.size());
    assertEquals("inner", nested.get(0).<String>getAs("url"));
    assertEquals("deep", nested.get(0).<String>getAs("valueString"));
  }

  @Test
  void emitsNoFieldIdentifierAndNoRootLevelExtensionMap(@TempDir @Nonnull final Path directory) {
    final Dataset<Row> transformed = transform(directory);

    final List<String> names = allFieldNames(transformed.schema());
    assertFalse(names.contains("_fid"), "the previous layout's field identifier is not emitted");
    assertFalse(names.contains("_extension"), "the previous layout's extension map is not emitted");
  }

  @Nonnull
  private static Dataset<Row> transform(@Nonnull final Path directory) {
    final String path = TransformFixtures.corpus(directory, PATIENT);
    return TransformFixtures.transformer().read(TransformFixtures.spark(), "Patient", path);
  }

  /** Returns the type of a field, unwrapping the array where the element repeats. */
  @Nonnull
  private static DataType elementType(
      @Nonnull final StructType structure, @Nonnull final String name) {
    final DataType type = structure.apply(name).dataType();
    return type instanceof final ArrayType array ? array.elementType() : type;
  }

  /** Returns every field name in a schema, at every depth. */
  @Nonnull
  private static List<String> allFieldNames(@Nonnull final StructType structure) {
    return Stream.of(structure.fields())
        .flatMap(
            field -> Stream.concat(Stream.of(field.name()), descend(field.dataType()).stream()))
        .toList();
  }

  @Nonnull
  private static List<String> descend(@Nonnull final DataType type) {
    if (type instanceof final ArrayType array) {
      return descend(array.elementType());
    }
    return type instanceof final StructType structure ? allFieldNames(structure) : List.of();
  }
}
