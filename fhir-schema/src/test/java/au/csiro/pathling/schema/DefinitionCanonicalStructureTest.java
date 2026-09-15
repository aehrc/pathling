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

import static au.csiro.pathling.schema.SchemaFixtures.DEFINITIONS;
import static au.csiro.pathling.schema.SchemaFixtures.array;
import static au.csiro.pathling.schema.SchemaFixtures.field;
import static au.csiro.pathling.schema.SchemaFixtures.names;
import static au.csiro.pathling.schema.SchemaFixtures.struct;
import static au.csiro.pathling.schema.SchemaFixtures.structAt;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.utilities.CanonicalStructure;
import au.csiro.pathling.utilities.StructureMerge;
import jakarta.annotation.Nonnull;
import java.util.List;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Tests the canonical structure taken from the FHIR definitions: the order of the fields at a node,
 * descent by field name, and termination over a graph that is cyclic (FR-057).
 *
 * <p>The order includes the layout's own fields, which have no definition element to take a
 * position from: {@code resourceType} at the head of a resource, and the metadata group and
 * annotations immediately after the element they accompany.
 */
class DefinitionCanonicalStructureTest {

  @Nonnull
  private static CanonicalStructure patient() {
    return DefinitionCanonicalStructure.forResource(DEFINITIONS, "Patient");
  }

  @Nonnull
  private static CanonicalStructure descend(
      @Nonnull final CanonicalStructure structure, @Nonnull final String name) {
    return structure.field(name).orElseThrow();
  }

  @Test
  void ordersAResourceByDefinitionOrderWithTheLayoutFieldsAtTheirFixedPositions() {
    final List<String> expected =
        List.of(
            "resourceType",
            "id",
            "_id",
            "meta",
            "implicitRules",
            "_implicitRules",
            "language",
            "_language",
            "text",
            "extension",
            "modifierExtension",
            "identifier",
            "active",
            "_active",
            "name",
            "telecom",
            "gender",
            "_gender",
            "birthDate",
            "_birthDate",
            "__birthDate_start",
            "__birthDate_end",
            "deceasedBoolean",
            "_deceasedBoolean",
            "deceasedDateTime",
            "_deceasedDateTime",
            "__deceasedDateTime_start",
            "__deceasedDateTime_end",
            "address",
            "maritalStatus",
            "multipleBirthInteger",
            "_multipleBirthInteger",
            "multipleBirthBoolean",
            "_multipleBirthBoolean",
            "photo",
            "contact",
            "communication",
            "generalPractitioner",
            "managingOrganization",
            "link");
    assertEquals(expected, patient().fieldOrder());
  }

  @Test
  void placesTheCanonicalAnnotationImmediatelyAfterAQuantity() {
    final List<String> order =
        DefinitionCanonicalStructure.forResource(DEFINITIONS, "Observation").fieldOrder();
    final int quantity = order.indexOf("valueQuantity");
    assertTrue(quantity >= 0, "The expanded quantity variant of the value choice is missing");
    assertEquals(LayoutFields.canonicalAnnotationName("valueQuantity"), order.get(quantity + 1));
  }

  @Test
  void descendsByFieldNameToTheStructureForThatElement() {
    assertEquals(
        List.of(
            "id",
            "_id",
            "extension",
            "use",
            "_use",
            "text",
            "_text",
            "family",
            "_family",
            "given",
            "_given",
            "prefix",
            "_prefix",
            "suffix",
            "_suffix",
            "period"),
        descend(patient(), "name").fieldOrder());
    assertEquals(
        List.of("start", "end"), descend(descend(patient(), "name"), "period").fieldOrder());
  }

  @Test
  void describesTheMetadataGroupBesideAPrimitive() {
    final CanonicalStructure group = descend(patient(), "_birthDate");
    assertEquals(List.of("id", "extension"), group.fieldOrder());
    assertTrue(group.field("extension").isPresent());
  }

  @Test
  void knowsNoStructureBeneathAPrimitiveOrAnAnnotation() {
    assertTrue(patient().field("birthDate").isEmpty());
    assertTrue(patient().field("__birthDate_start").isEmpty());
    assertTrue(patient().field("resourceType").isEmpty());
  }

  @Test
  void distinguishesTwoBackboneElementsOfTheSameResource() {
    // Every backbone element reports the same FHIR type, so a structure memoised by type would
    // hand the children of one of these to the other.
    final List<String> contact = descend(patient(), "contact").fieldOrder();
    final List<String> link = descend(patient(), "link").fieldOrder();

    assertEquals(
        List.of(
            "id",
            "_id",
            "extension",
            "modifierExtension",
            "relationship",
            "name",
            "telecom",
            "address",
            "gender",
            "_gender",
            "organization",
            "period"),
        contact);
    assertEquals(
        List.of("id", "_id", "extension", "modifierExtension", "other", "type", "_type"), link);
    assertNotEquals(contact, link);
  }

  @Test
  void reachesTheSameStructureForTheSameTypeByAnyPath() {
    // The memo is what makes a merge that descends the same types repeatedly affordable.
    final CanonicalStructure patient = patient();
    final CanonicalStructure fromPatient = descend(patient, "name");
    final CanonicalStructure fromContact = descend(descend(patient, "contact"), "name");
    assertSame(fromPatient, fromContact);
  }

  @Test
  @Timeout(30)
  void descendsASelfRecursiveTypeToAnArbitraryDepthWithoutExpandingIt() {
    CanonicalStructure extension = descend(patient(), "extension");
    final List<String> expected = extension.fieldOrder();
    assertEquals(List.of("id", "_id", "extension", "url", "_url"), expected.subList(0, 5));

    for (int i = 0; i < 100; i++) {
      final CanonicalStructure next = descend(extension, "extension");
      assertEquals(expected, next.fieldOrder());
      // The same structure answers at every depth, so nothing is expanded by descending further.
      assertSame(extension, next);
      extension = next;
    }
  }

  @Test
  void expandsNothingUntilItIsAsked() {
    final CountingNodeDefinition counting =
        new CountingNodeDefinition(DEFINITIONS.findResourceDefinition("Patient"));
    final CanonicalStructure structure = DefinitionCanonicalStructure.of(counting, true);
    assertEquals(0, counting.getChildrenCalls());

    structure.fieldOrder();
    assertEquals(1, counting.getChildrenCalls());

    structure.fieldOrder();
    structure.field("name");
    structure.field("contact");
    assertEquals(1, counting.getChildrenCalls());
  }

  @Test
  void mergesTwoPrunedStructuresIntoDefinitionOrderRatherThanDiscoveryOrder() {
    // Neither operand determines the order: each is a subsequence of the canonical one, at the top
    // level and within the nested structure alike.
    final StructType left =
        struct(
            field("gender", DataTypes.StringType),
            field("birthDate", DataTypes.StringType),
            field("name", array(struct(field("given", array(DataTypes.StringType))))));
    final StructType right =
        struct(
            field("resourceType", DataTypes.StringType),
            field("active", DataTypes.BooleanType),
            field("_birthDate", struct(field("id", DataTypes.StringType))),
            field("name", array(struct(field("family", DataTypes.StringType)))));

    final StructType merged = StructureMerge.merge(left, right, patient());
    assertEquals(
        List.of("resourceType", "active", "name", "gender", "birthDate", "_birthDate"),
        names(merged));
    assertEquals(List.of("family", "given"), names(structAt(merged, "name")));

    // The merge is commutative, so the other order gives the same result.
    assertEquals(merged, StructureMerge.merge(right, left, patient()));
  }
}
