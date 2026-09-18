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

package au.csiro.pathling.schema;

import static au.csiro.pathling.schema.SchemaFixtures.array;
import static au.csiro.pathling.schema.SchemaFixtures.builder;
import static au.csiro.pathling.schema.SchemaFixtures.field;
import static au.csiro.pathling.schema.SchemaFixtures.names;
import static au.csiro.pathling.schema.SchemaFixtures.struct;
import static au.csiro.pathling.schema.SchemaFixtures.structAt;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

/**
 * Tests that the pruned schema carries a complex element only where some descendant leaf is
 * populated, so that a field-less structure never arises (FR-011).
 *
 * <p>Presence comes from the structure standing in for the inferred read schema. A branch the data
 * reaches but never populates is a branch the schema must not carry, because a structure with no
 * fields is not a type anything can be read into.
 */
class SchemaPruningTest {

  @Nonnull
  private static StructType prune(@Nonnull final StructType observed) {
    return builder().pruned("Patient", observed);
  }

  @Nonnull
  private static StructType observed(@Nonnull final StructField... rest) {
    return struct(
        Stream.concat(Stream.of(field("resourceType", DataTypes.StringType)), Stream.of(rest))
            .toArray(StructField[]::new));
  }

  /** Asserts that no structure anywhere beneath the given type has an empty field set. */
  private static void assertNoFieldLessStructure(@Nonnull final DataType type) {
    if (type instanceof final ArrayType array) {
      assertNoFieldLessStructure(array.elementType());
    } else if (type instanceof final StructType structure) {
      assertTrue(
          structure.fields().length > 0, "A structure with no fields reached the derived schema");
      Stream.of(structure.fields()).forEach(f -> assertNoFieldLessStructure(f.dataType()));
    }
  }

  @Test
  void keepsAComplexElementWhereADescendantLeafIsPopulated() {
    final StructType derived =
        prune(observed(field("name", array(struct(field("family", DataTypes.StringType))))));

    assertTrue(names(derived).contains("name"));
    assertEquals(List.of("family"), names(structAt(derived, "name")));
  }

  @Test
  void dropsAComplexElementThatCarriesNoFieldsAtAll() {
    final StructType derived = prune(observed(field("name", array(struct()))));

    assertFalse(names(derived).contains("name"));
  }

  @Test
  void dropsAComplexElementWhoseOnlyFieldsAreOutsideTheDefinitions() {
    // The inferred schema saw something here, but nothing the definitions describe, so there is no
    // leaf to keep and therefore no structure to build.
    final StructType derived =
        prune(
            observed(field("maritalStatus", struct(field("notAnElement", DataTypes.StringType)))));

    assertFalse(names(derived).contains("maritalStatus"));
  }

  @Test
  void dropsABranchThatIsUnpopulatedAllTheWayDown() {
    // Three levels of structure and not one leaf: the whole branch goes.
    final StructType derived =
        prune(
            observed(
                field(
                    "contact",
                    array(struct(field("name", struct()), field("address", struct()))))));

    assertFalse(names(derived).contains("contact"));
  }

  @Test
  void keepsOnlyTheBranchThatLeadsToAPopulatedLeaf() {
    final StructType derived =
        prune(
            observed(
                field(
                    "contact",
                    array(
                        struct(
                            field("name", struct(field("family", DataTypes.StringType))),
                            field("address", struct()))))));

    assertEquals(List.of("name"), names(structAt(derived, "contact")));
    assertEquals(List.of("family"), names(structAt(derived, "contact", "name")));
  }

  @Test
  void neverProducesAFieldLessStructureAnywhereInTheSchema() {
    final StructType derived =
        prune(
            observed(
                field("name", array(struct(field("given", DataTypes.StringType)))),
                field("maritalStatus", struct()),
                field(
                    "contact",
                    array(
                        struct(
                            field("name", struct()),
                            field("period", struct(field("start", DataTypes.StringType))))))));

    assertNoFieldLessStructure(derived);
    assertEquals(List.of("period"), names(structAt(derived, "contact")));
  }

  @Test
  void omitsAnElementTheDataNeverCarried() {
    final StructType derived = prune(observed(field("birthDate", DataTypes.StringType)));

    assertEquals(List.of("resourceType", "birthDate"), names(derived));
  }
}
