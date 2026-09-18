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
import static au.csiro.pathling.schema.SchemaFixtures.builder;
import static au.csiro.pathling.schema.SchemaFixtures.elementTypeOf;
import static au.csiro.pathling.schema.SchemaFixtures.field;
import static au.csiro.pathling.schema.SchemaFixtures.names;
import static au.csiro.pathling.schema.SchemaFixtures.struct;
import static au.csiro.pathling.schema.SchemaFixtures.structAt;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.annotation.Nonnull;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

/**
 * Tests that the dense schema is the pruned derivation with pruning skipped (FR-010), and that
 * every derived structure orders its fields in definition order (FR-057).
 *
 * <p>Field order is part of the type: structures compare positionally while ignoring names, so an
 * order that differs between the two modes is a silent wrong answer rather than an untidy one.
 */
class SchemaModeParityTest {

  /**
   * A structure standing in for the schema inferred from a document that populates a scattering of
   * elements across several levels, none of them extensions and none deeper than the dense bounds
   * reach, so that the dense mode carries every branch this one does.
   */
  @Nonnull
  private static StructType observedPatient() {
    return struct(
        field("resourceType", DataTypes.StringType),
        field("id", DataTypes.StringType),
        field("birthDate", DataTypes.StringType),
        field("_birthDate", struct(field("id", DataTypes.StringType))),
        field("active", DataTypes.BooleanType),
        field("gender", DataTypes.StringType),
        field("deceasedDateTime", DataTypes.StringType),
        field(
            "name",
            array(
                struct(
                    field("given", array(DataTypes.StringType)),
                    field("family", DataTypes.StringType),
                    field("period", struct(field("start", DataTypes.StringType)))))),
        field("maritalStatus", struct(field("text", DataTypes.StringType))),
        field(
            "contact",
            array(
                struct(
                    field("gender", DataTypes.StringType),
                    field("name", struct(field("family", DataTypes.StringType)))))));
  }

  /**
   * Returns the projection of a type onto the field names another type carries, recursively, so
   * that two schemas can be compared over the branches they share alone.
   */
  @Nonnull
  private static DataType project(@Nonnull final DataType subject, @Nonnull final DataType onto) {
    if (subject instanceof final ArrayType subjectArray
        && onto instanceof final ArrayType ontoArray) {
      return DataTypes.createArrayType(
          project(subjectArray.elementType(), ontoArray.elementType()),
          subjectArray.containsNull());
    }
    if (subject instanceof final StructType subjectStruct
        && onto instanceof final StructType ontoStruct) {
      final Set<String> keep = new LinkedHashSet<>(names(ontoStruct));
      final StructField[] fields =
          Stream.of(subjectStruct.fields())
              .filter(f -> keep.contains(f.name()))
              .map(
                  f ->
                      new StructField(
                          f.name(),
                          project(f.dataType(), ontoStruct.apply(f.name()).dataType()),
                          f.nullable(),
                          f.metadata()))
              .toArray(StructField[]::new);
      return new StructType(fields);
    }
    return subject;
  }

  /** Asserts that the first list is a subsequence of the second. */
  private static void assertSubsequence(
      @Nonnull final List<String> candidate,
      @Nonnull final List<String> whole,
      @Nonnull final String where) {
    int index = 0;
    for (final String name : candidate) {
      final int found = whole.subList(index, whole.size()).indexOf(name);
      assertTrue(
          found >= 0,
          "At "
              + where
              + ", the pruned order "
              + candidate
              + " is not a subsequence of the dense order "
              + whole);
      index += found + 1;
    }
  }

  /** Asserts the subsequence property at every structure the two schemas share. */
  private static void assertSubsequenceEverywhere(
      @Nonnull final StructType pruned,
      @Nonnull final StructType dense,
      @Nonnull final String where) {
    assertSubsequence(names(pruned), names(dense), where);
    for (final StructField prunedField : pruned.fields()) {
      final Optional<StructField> denseField =
          Stream.of(dense.fields()).filter(f -> f.name().equals(prunedField.name())).findFirst();
      if (denseField.isPresent()
          && elementTypeOf(prunedField.dataType()) instanceof final StructType prunedChild
          && elementTypeOf(denseField.get().dataType()) instanceof final StructType denseChild) {
        assertSubsequenceEverywhere(prunedChild, denseChild, where + "." + prunedField.name());
      }
    }
  }

  @Test
  void derivesTheSameTypesCardinalityAndOrderForTheBranchesBothModesCarry() {
    final StructType dense = builder().dense("Patient");
    final StructType pruned = builder().pruned("Patient", observedPatient());

    // Each is restricted to the fields the other carries, so that what remains is exactly the
    // branches both modes have, compared in full: names, types, cardinality and order.
    assertEquals(project(dense, pruned), project(pruned, dense));
  }

  @Test
  void ordersTheFieldsOfAResourceInDefinitionOrder() {
    // The elements of Patient in R4 declaration order, with the choices expanded in place, the
    // metadata group beside each primitive, contained omitted and resourceType first. Extensions
    // are absent because the dense bounds exclude them.
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
            "identifier",
            "active",
            "_active",
            "name",
            "telecom",
            "gender",
            "_gender",
            "birthDate",
            "_birthDate",
            "deceasedBoolean",
            "_deceasedBoolean",
            "deceasedDateTime",
            "_deceasedDateTime",
            "address",
            "maritalStatus",
            "multipleBirthBoolean",
            "_multipleBirthBoolean",
            "multipleBirthInteger",
            "_multipleBirthInteger",
            "photo",
            "contact",
            "communication",
            "generalPractitioner",
            "managingOrganization",
            "link");
    assertEquals(expected, names(builder().dense("Patient")));
  }

  @Test
  void ordersTheFieldsOfANestedStructureInDefinitionOrder() {
    // Order below the top level is the assertion that catches a derivation that orders the root
    // canonically and everything under it by discovery order.
    final StructType dense = builder().dense("Patient");
    assertEquals(
        List.of(
            "id",
            "_id",
            "relationship",
            "name",
            "telecom",
            "address",
            "gender",
            "_gender",
            "organization",
            "period"),
        names(structAt(dense, "contact")));
  }

  @Test
  void ordersAPrunedStructureAsASubsequenceOfTheDenseOne() {
    final StructType dense = builder().dense("Patient");
    final StructType pruned = builder().pruned("Patient", observedPatient());

    assertSubsequenceEverywhere(pruned, dense, "Patient");
  }

  @Test
  void derivesAndOrdersAChoiceByTheSameDeclarationOrder() {
    // The derivation and the canonical structure reach the expansion of a choice by their own
    // routes. If those routes disagree the two orders drift apart, and structures that compare
    // positionally compare the wrong fields.
    final List<String> declared =
        List.of(
            "valueQuantity",
            "valueCodeableConcept",
            "valueString",
            "valueBoolean",
            "valueInteger",
            "valueRange",
            "valueRatio",
            "valueSampledData",
            "valueTime",
            "valueDateTime",
            "valuePeriod");

    assertEquals(declared, variantsOf(names(builder().dense("Observation"))));
    assertEquals(
        declared,
        variantsOf(
            DefinitionCanonicalStructure.forResource(DEFINITIONS, "Observation").fieldOrder()));
  }

  /**
   * Returns the expanded variants of the value choice from a field order, dropping the metadata
   * groups and annotations that accompany them.
   */
  @Nonnull
  private static List<String> variantsOf(@Nonnull final List<String> fieldOrder) {
    return fieldOrder.stream().filter(name -> name.startsWith("value")).toList();
  }
}
