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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.io.transform.NonConformantContent;
import au.csiro.pathling.io.transform.TransformFixtures;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Pins what M1 stores and writes where it does not prune (decision 71 and its addendum, T080c).
 *
 * <p>Conformant input leaves two kinds of emptiness in the layout, and both are kept on purpose
 * until M5 stores primitive metadata. A structure whose only content was a primitive's id and
 * extensions is an element that exists: FHIRPath counts it, and so does the released encoder, so
 * pruning it would change query answers. It is kept whatever other conformant documents are read
 * with it, because the primitive is stored as a null of the type the definitions give it (decision
 * 72); where that primitive repeats, the released encoder drops it and the layout does not. A
 * repeating primitive's null is positional, and FHIR aligns it with the metadata array. Neither is
 * written as conformant FHIR until that metadata is written beside it. The last case is residue
 * rather than intent: a conformant document emptied by another document in the same file.
 *
 * <p>No conformance suite carries most of these shapes, so a later change that pruned them would
 * pass everything else. These tests are what fails.
 */
class DeferredPruningTest {

  /** The data-absent-reason extension, the usual reason a primitive carries no value. */
  @Nonnull
  private static final String DATA_ABSENT =
      "{\"extension\":[{\"url\":\"http://hl7.org/fhir/StructureDefinition/data-absent-reason\","
          + "\"valueCode\":\"masked\"}]}";

  @Test
  void keepsAStructureWhoseOnlyContentWasPrimitiveMetadataInItsSlot(
      @TempDir @Nonnull final Path directory) {
    final String path =
        TransformFixtures.corpus(
            directory,
            "{\"resourceType\":\"Patient\",\"id\":\"1\",\"name\":[{\"family\":\"x\"},"
                + "{\"_family\":"
                + DATA_ABSENT
                + "},{\"family\":\"y\"}]}");

    final Dataset<Row> stored = TransformFixtures.reader().read("Patient", path);
    final Row slots =
        stored.selectExpr("size(name)", "name[1] is not null", "name[1].family is null").first();

    assertEquals(3, slots.getInt(0), "the metadata-only name keeps its place");
    assertTrue(slots.getBoolean(1), "the metadata-only name is present, not null");
    assertTrue(slots.getBoolean(2), "nothing of the metadata-only name is stored");
    assertTrue(
        document(stored).contains("\"name\":[{\"family\":\"x\"},{},{\"family\":\"y\"}]"),
        document(stored));
    assertEquals(List.of("Patient.name._family"), findingPaths(path));
  }

  @Test
  void keepsAStructureWhoseOnlyContentWasPrimitiveMetadataWhereNothingElseKeepsItsColumn(
      @TempDir @Nonnull final Path directory) {
    // The same name as above, but the only one in the file, so nothing else observed gives its
    // column a type. The definitions do, so the primitive is stored as a null and the name keeps
    // its place, as it does in the released encoder (decision 72).
    final String path =
        TransformFixtures.corpus(
            directory,
            "{\"resourceType\":\"Patient\",\"id\":\"1\",\"name\":[{\"_family\":"
                + DATA_ABSENT
                + "}]}");

    final Dataset<Row> stored = TransformFixtures.reader().read("Patient", path);
    final Row slots =
        stored.selectExpr("size(name)", "name[0] is not null", "name[0].family is null").first();

    assertEquals(1, slots.getInt(0), "the lone metadata-only name is stored");
    assertTrue(slots.getBoolean(1), "the lone metadata-only name is present, not null");
    assertTrue(slots.getBoolean(2), "nothing of the lone metadata-only name is stored");
    assertEquals("{\"resourceType\":\"Patient\",\"id\":\"1\",\"name\":[{}]}", document(stored));
    assertEquals(List.of("Patient.name._family"), findingPaths(path));
  }

  @Test
  void keepsAStructureWhoseOnlyContentWasARepeatingPrimitivesMetadata(
      @TempDir @Nonnull final Path directory) {
    // Here the layout departs from the released encoder, and on purpose. The parser that encoder
    // reads through discards a _given array with no given array beside it, so it drops the name.
    // FHIR says the element exists, and the layout keeps it (decision 72).
    final String path =
        TransformFixtures.corpus(
            directory,
            "{\"resourceType\":\"Patient\",\"id\":\"1\",\"name\":[{\"_given\":["
                + DATA_ABSENT
                + ","
                + DATA_ABSENT
                + "]}]}");

    final Dataset<Row> stored = TransformFixtures.reader().read("Patient", path);
    final Row slots =
        stored.selectExpr("size(name)", "name[0] is not null", "name[0].given is null").first();

    assertEquals(1, slots.getInt(0), "the name holding only _given is stored");
    assertTrue(slots.getBoolean(1), "the name holding only _given is present, not null");
    assertTrue(slots.getBoolean(2), "the given it held is stored as a null array");
    assertEquals("{\"resourceType\":\"Patient\",\"id\":\"1\",\"name\":[{}]}", document(stored));
    assertEquals(List.of("Patient.name._given"), findingPaths(path));
  }

  @Test
  void keepsANestedStructureWhoseOnlyContentWasPrimitiveMetadata(
      @TempDir @Nonnull final Path directory) {
    // A singular structure inside a repeating one, holding a singular and a repeating primitive
    // that carry nothing but metadata. Each level keeps its place.
    final String path =
        TransformFixtures.corpus(
            directory,
            "{\"resourceType\":\"Patient\",\"id\":\"1\",\"contact\":[{\"name\":{\"_family\":"
                + DATA_ABSENT
                + ",\"_given\":["
                + DATA_ABSENT
                + "]}}]}");

    final Dataset<Row> stored = TransformFixtures.reader().read("Patient", path);

    assertTrue(
        stored.selectExpr("contact[0].name is not null").first().getBoolean(0),
        "the metadata-only contact name is present, not null");
    assertEquals(
        "{\"resourceType\":\"Patient\",\"id\":\"1\",\"contact\":[{\"name\":{}}]}",
        document(stored));
    assertEquals(
        List.of("Patient.contact.name._family", "Patient.contact.name._given"), findingPaths(path));
  }

  @Test
  void keepsASingularStructureWhoseOnlyContentWasPrimitiveMetadata(
      @TempDir @Nonnull final Path directory) {
    // A singular structure at the root of a resource, alone in its input.
    final String path =
        TransformFixtures.corpus(
            directory,
            "{\"resourceType\":\"Encounter\",\"id\":\"1\",\"status\":\"finished\","
                + "\"period\":{\"_start\":"
                + DATA_ABSENT
                + "}}");

    final Dataset<Row> stored = TransformFixtures.reader().read("Encounter", path);

    assertTrue(
        stored.selectExpr("period is not null").first().getBoolean(0),
        "the metadata-only period is present, not null");
    assertEquals(
        "{\"resourceType\":\"Encounter\",\"id\":\"1\",\"status\":\"finished\",\"period\":{}}",
        TransformFixtures.writer().write("Encounter", stored).collectAsList().get(0));
    assertEquals(
        List.of("Encounter.period._start"),
        TransformFixtures.transformer()
            .findings("Encounter", TransformFixtures.inferred(path).schema())
            .stream()
            .map(NonConformantContent::getPath)
            .toList());
  }

  @Test
  void keepsThePositionalNullsOfARepeatingPrimitive(@TempDir @Nonnull final Path directory) {
    final String path =
        TransformFixtures.corpus(
            directory,
            "{\"resourceType\":\"Patient\",\"id\":\"1\",\"name\":[{\"given\":[\"Ann\",null,\"Bee\"],"
                + "\"_given\":[null,"
                + DATA_ABSENT
                + ",null]}]}",
            "{\"resourceType\":\"Patient\",\"id\":\"2\",\"name\":[{\"family\":\"F\","
                + "\"given\":[null],\"_given\":["
                + DATA_ABSENT
                + "]}]}");

    final Dataset<Row> stored = TransformFixtures.reader().read("Patient", path);
    final String documents =
        String.join("\n", TransformFixtures.writer().write("Patient", stored).collectAsList());

    assertEquals(
        List.of("[Ann, null, Bee]", "[null]"),
        stored.orderBy("id").selectExpr("cast(name[0].given as string)").collectAsList().stream()
            .map(row -> row.getString(0))
            .toList(),
        "the nulls keep their positions");
    assertTrue(documents.contains("\"given\":[\"Ann\",null,\"Bee\"]"), documents);
    assertTrue(documents.contains("\"given\":[null]"), documents);
    assertFalse(documents.contains("_given"), "the metadata they align with is not written yet");
    assertEquals(List.of("Patient.name._given"), findingPaths(path));
  }

  @Test
  void writesAnEmptyObjectWhereANeighbourRetypedAConformantElementsOnlyContent(
      @TempDir @Nonnull final Path directory) {
    // The first document is conformant on its own. The second carries a size that is not an
    // unsignedInt, which types the column for the whole file, so the first photo's only content
    // is lost with it (FR-016 item 6).
    final String path =
        TransformFixtures.corpus(
            directory,
            "{\"resourceType\":\"Patient\",\"id\":\"1\","
                + "\"photo\":[{\"contentType\":\"image/png\"},{\"size\":10}]}",
            "{\"resourceType\":\"Patient\",\"id\":\"2\",\"photo\":[{\"size\":1.5}]}");

    final Dataset<Row> stored = TransformFixtures.reader().read("Patient", path);
    final String documents =
        String.join("\n", TransformFixtures.writer().write("Patient", stored).collectAsList());

    assertTrue(
        documents.contains("\"id\":\"1\",\"photo\":[{\"contentType\":\"image/png\"},{}]"),
        documents);
    assertEquals(List.of("Patient.photo.size"), findingPaths(path));
  }

  /** Returns the one document written from a dataset holding one resource. */
  @Nonnull
  private static String document(@Nonnull final Dataset<Row> stored) {
    return TransformFixtures.writer().write("Patient", stored).collectAsList().get(0);
  }

  /** Returns the paths of what the transform reports it does not store. */
  @Nonnull
  private static List<String> findingPaths(@Nonnull final String path) {
    final List<NonConformantContent> findings =
        TransformFixtures.transformer()
            .findings("Patient", TransformFixtures.inferred(path).schema());
    return findings.stream().map(NonConformantContent::getPath).toList();
  }
}
