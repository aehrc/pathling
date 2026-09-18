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
package au.csiro.pathling.utilities;

import static au.csiro.pathling.utilities.CanonicalStructureFixture.ordering;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

/**
 * Tests the recursive field-wise union of structure types under a supplied canonical structure.
 *
 * <p>The canonical structures here are hand-built, so that nothing in these tests depends on a
 * definition source.
 */
class StructureMergeTest {

  private static final DataType STRING = DataTypes.StringType;

  // A canonical structure standing in for a resource, four levels deep through the contact branch,
  // and self-recursive through the extension branch.

  @Nonnull
  private static CanonicalStructure patient() {
    return ordering("id", "name", "contact", "extension")
        .child("name", StructureMergeTest::humanName)
        .child("contact", StructureMergeTest::contact)
        .child("extension", StructureMergeTest::extension);
  }

  @Nonnull
  private static CanonicalStructure humanName() {
    return ordering("id", "use", "family", "given", "period")
        .child("period", StructureMergeTest::period);
  }

  @Nonnull
  private static CanonicalStructure contact() {
    return ordering("id", "relationship", "name", "telecom", "period")
        .child("name", StructureMergeTest::humanName)
        .child("telecom", StructureMergeTest::contactPoint)
        .child("period", StructureMergeTest::period);
  }

  @Nonnull
  private static CanonicalStructure contactPoint() {
    return ordering("system", "value", "use");
  }

  @Nonnull
  private static CanonicalStructure period() {
    return ordering("start", "end");
  }

  @Nonnull
  private static CanonicalStructure extension() {
    return ordering("id", "url", "valueString", "extension")
        .child("extension", StructureMergeTest::extension);
  }

  @Test
  void takesFieldOrderFromTheCanonicalStructureRatherThanFromTheOperands() {
    // Neither operand determines the relative position of family and given: [id, given] and
    // [id, family] are both subsequences of the canonical order and neither says which comes
    // first. Only the canonical structure settles it.
    final StructType left = struct(field("id", STRING), field("given", array(STRING)));
    final StructType right = struct(field("id", STRING), field("family", STRING));
    final StructType expected =
        struct(field("id", STRING), field("family", STRING), field("given", array(STRING)));

    assertEquals(expected, StructureMerge.merge(left, right, humanName()));
    assertEquals(expected, StructureMerge.merge(right, left, humanName()));
  }

  @Test
  void ordersEveryLevelCanonicallyAndNotOnlyTheTop() {
    // Both operands carry deliberately non-canonical internal order, at every level. An
    // implementation that orders the top level canonically and the levels beneath it by discovery
    // order passes the previous test and fails this one.
    final StructType left =
        struct(
            field("name", struct(field("given", array(STRING)), field("id", STRING))),
            field("id", STRING));
    final StructType right =
        struct(
            field(
                "contact",
                struct(
                    field(
                        "name",
                        struct(
                            field("period", struct(field("end", STRING), field("start", STRING))),
                            field("family", STRING))),
                    field("id", STRING))),
            field(
                "name",
                struct(field("family", STRING), field("period", struct(field("start", STRING))))));

    final StructType expected =
        struct(
            field("id", STRING),
            field(
                "name",
                struct(
                    field("id", STRING),
                    field("family", STRING),
                    field("given", array(STRING)),
                    field("period", struct(field("start", STRING))))),
            field(
                "contact",
                struct(
                    field("id", STRING),
                    field(
                        "name",
                        struct(
                            field("family", STRING),
                            field(
                                "period",
                                struct(field("start", STRING), field("end", STRING))))))));

    assertEquals(expected, StructureMerge.merge(left, right, patient()));
    assertEquals(expected, StructureMerge.merge(right, left, patient()));
  }

  @Test
  void includesFieldsCarriedByOnlyOneOperandAndMakesThemNullable() {
    // The result is a union, so a field only one side carries survives; it becomes nullable
    // because the rows from the other side have no value for it.
    final StructType left = struct(new StructField("family", STRING, false, Metadata.empty()));
    final StructType right =
        struct(new StructField("given", array(STRING), false, Metadata.empty()));

    final StructType merged = StructureMerge.merge(left, right, humanName());

    assertEquals(struct(field("family", STRING), field("given", array(STRING))), merged);
    assertTrue(merged.apply("family").nullable());
    assertTrue(merged.apply("given").nullable());
  }

  @Test
  void unionsNullabilityWhereBothOperandsCarryTheField() {
    final StructType left =
        struct(
            new StructField("family", STRING, false, Metadata.empty()),
            new StructField("given", array(STRING), true, Metadata.empty()));
    final StructType right =
        struct(
            new StructField("family", STRING, false, Metadata.empty()),
            new StructField("given", array(STRING), false, Metadata.empty()));

    final StructType expected =
        struct(
            new StructField("family", STRING, false, Metadata.empty()),
            new StructField("given", array(STRING), true, Metadata.empty()));

    // Asserted in both directions, because an implementation that simply takes the nullability of
    // its first operand is right in one direction and wrong in the other.
    assertEquals(expected, StructureMerge.merge(left, right, humanName()));
    assertEquals(expected, StructureMerge.merge(right, left, humanName()));
  }

  @Test
  void omitsCanonicalFieldsThatNeitherOperandCarries() {
    // Canonical order is definition order restricted to the fields present, so use and period do
    // not appear.
    final StructType left = struct(field("given", array(STRING)));
    final StructType right = struct(field("id", STRING));

    assertEquals(
        struct(field("id", STRING), field("given", array(STRING))),
        StructureMerge.merge(left, right, humanName()));
  }

  @Test
  void mergesArrayElementTypesRecursively() {
    final StructType left =
        struct(
            field(
                "name",
                DataTypes.createArrayType(
                    struct(field("id", STRING), field("given", array(STRING))), false)));
    final StructType right =
        struct(
            field(
                "name",
                DataTypes.createArrayType(
                    struct(field("id", STRING), field("family", STRING)), true)));

    final StructType expected =
        struct(
            field(
                "name",
                DataTypes.createArrayType(
                    struct(
                        field("id", STRING),
                        field("family", STRING),
                        field("given", array(STRING))),
                    true)));

    assertEquals(expected, StructureMerge.merge(left, right, patient()));
    assertEquals(expected, StructureMerge.merge(right, left, patient()));
  }

  @Test
  void treatsTheNullTypeAsAnIdentity() {
    // An absent operand is typed as the bottom type, so the merge must accept it and yield the
    // other side. The other side is still canonicalised.
    final StructType left =
        struct(field("family", DataTypes.NullType), field("period", DataTypes.NullType));
    final StructType right =
        struct(
            field("family", STRING),
            field("period", struct(field("end", STRING), field("start", STRING))));

    final StructType expected =
        struct(
            field("family", STRING),
            field("period", struct(field("start", STRING), field("end", STRING))));

    assertEquals(expected, StructureMerge.merge(left, right, humanName()));
    assertEquals(expected, StructureMerge.merge(right, left, humanName()));
  }

  @Test
  void keepsTheNullTypeWhereBothOperandsAreAbsent() {
    final StructType both = struct(field("family", DataTypes.NullType));

    assertEquals(both, StructureMerge.merge(both, both, humanName()));
  }

  @Test
  void treatsAnArrayOfTheNullTypeAsAnIdentity() {
    final StructType left = struct(field("name", array(DataTypes.NullType)));
    final StructType right =
        struct(field("name", array(struct(field("family", STRING), field("id", STRING)))));

    final StructType expected =
        struct(field("name", array(struct(field("id", STRING), field("family", STRING)))));

    assertEquals(expected, StructureMerge.merge(left, right, patient()));
    assertEquals(expected, StructureMerge.merge(right, left, patient()));
  }

  @Test
  void canonicalisesASubtreeCarriedByOnlyOneOperand() {
    // A structure that is copied through unchanged, rather than canonicalised, leaves the result's
    // order depending on which fields happened to overlap.
    final StructType deep =
        struct(
            field(
                "name",
                struct(
                    field("period", struct(field("end", STRING), field("start", STRING))),
                    field("family", STRING))));
    final StructType expected =
        struct(
            field(
                "name",
                struct(
                    field("family", STRING),
                    field("period", struct(field("start", STRING), field("end", STRING))))));

    assertEquals(expected, StructureMerge.merge(deep, new StructType(), patient()));
    assertEquals(expected, StructureMerge.merge(new StructType(), deep, patient()));
    assertEquals(
        StructureMerge.merge(deep, deep, patient()),
        StructureMerge.merge(deep, new StructType(), patient()));
  }

  @Test
  void appendsFieldsOutsideTheCanonicalStructureInNameOrder() {
    // The canonical structure has nothing to say about these fields, so they go after the ones it
    // does order, by name. Any order derived from the operands would not be commutative.
    final StructType left = struct(field("zebra", STRING), field("family", STRING));
    final StructType right = struct(field("alpha", STRING), field("id", STRING));

    final StructType expected =
        struct(
            field("id", STRING),
            field("family", STRING),
            field("alpha", STRING),
            field("zebra", STRING));

    assertEquals(expected, StructureMerge.merge(left, right, humanName()));
    assertEquals(expected, StructureMerge.merge(right, left, humanName()));
  }

  @Test
  void ordersFieldsBeneathAnUnknownFieldByName() {
    final StructType left =
        struct(field("custom", struct(field("zeta", STRING), field("alpha", STRING))));
    final StructType right = struct(field("custom", struct(field("mid", STRING))));

    final StructType expected =
        struct(
            field(
                "custom",
                struct(field("alpha", STRING), field("mid", STRING), field("zeta", STRING))));

    assertEquals(expected, StructureMerge.merge(left, right, humanName()));
    assertEquals(expected, StructureMerge.merge(right, left, humanName()));
  }

  @Test
  void rejectsOperandsThatDisagreeOnTheTypeOfAField() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            StructureMerge.merge(
                struct(field("family", STRING)),
                struct(field("family", DataTypes.IntegerType)),
                humanName()));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            StructureMerge.merge(
                struct(field("name", STRING)),
                struct(field("name", struct(field("family", STRING)))),
                patient()));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            StructureMerge.merge(
                struct(field("name", struct(field("family", STRING)))),
                struct(field("name", array(struct(field("family", STRING))))),
                patient()));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            StructureMerge.merge(
                struct(field("value", DataTypes.createDecimalType(10, 2))),
                struct(field("value", DataTypes.createDecimalType(12, 4))),
                humanName()));
  }

  @Test
  void namesThePathOfTheFieldOnWhichTheOperandsDisagree() {
    final IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                StructureMerge.merge(
                    struct(field("name", struct(field("family", STRING)))),
                    struct(field("name", struct(field("family", DataTypes.IntegerType)))),
                    patient()));

    assertTrue(
        error.getMessage().contains("name.family"),
        "Expected the message to name the conflicting field, but was: " + error.getMessage());
  }

  @Test
  void keepsMetadataOnlyWhereBothOperandsAgreeOnIt() {
    final Metadata leftMetadata = new MetadataBuilder().putString("comment", "left").build();
    final Metadata rightMetadata = new MetadataBuilder().putString("comment", "right").build();
    final StructType left =
        struct(
            new StructField("family", STRING, true, leftMetadata),
            new StructField("given", array(STRING), true, leftMetadata));
    final StructType right =
        struct(
            new StructField("family", STRING, true, rightMetadata),
            new StructField("given", array(STRING), true, leftMetadata));

    final StructType expected =
        struct(
            new StructField("family", STRING, true, Metadata.empty()),
            new StructField("given", array(STRING), true, leftMetadata));

    assertEquals(expected, StructureMerge.merge(left, right, humanName()));
    assertEquals(expected, StructureMerge.merge(right, left, humanName()));
  }

  @Test
  void isCommutativeOverEveryPairOfOperands() {
    final List<StructType> operands = awkwardOperands();
    for (final StructType left : operands) {
      for (final StructType right : operands) {
        assertEquals(
            StructureMerge.merge(left, right, patient()),
            StructureMerge.merge(right, left, patient()),
            "Merge was not commutative for "
                + left.simpleString()
                + " and "
                + right.simpleString());
      }
    }
  }

  @Test
  void isAssociativeOverEveryTripleOfOperands() {
    final List<StructType> operands = awkwardOperands();
    for (final StructType first : operands) {
      for (final StructType second : operands) {
        for (final StructType third : operands) {
          assertEquals(
              StructureMerge.merge(
                  StructureMerge.merge(first, second, patient()), third, patient()),
              StructureMerge.merge(
                  first, StructureMerge.merge(second, third, patient()), patient()),
              "Merge was not associative for "
                  + first.simpleString()
                  + ", "
                  + second.simpleString()
                  + " and "
                  + third.simpleString());
        }
      }
    }
  }

  @Test
  void givesTheSameResultForEveryOrderOfTheSameOperands() {
    // Commutativity and associativity together mean the result cannot depend on merge order, which
    // is what makes a merged schema reproducible no matter which file or which operand is seen
    // first.
    final List<StructType> operands = awkwardOperands();
    final StructType expected = StructureMerge.merge(operands, patient());

    for (final List<StructType> permutation : permutations(operands)) {
      assertEquals(
          expected,
          StructureMerge.merge(permutation, patient()),
          "Merge depended on the order of its operands");
      assertEquals(
          expected,
          permutation.stream()
              .reduce((left, right) -> StructureMerge.merge(left, right, patient()))
              .map(folded -> StructureMerge.merge(folded, folded, patient()))
              .orElseThrow(),
          "The list form disagreed with the folded binary form");
    }
  }

  @Test
  void leavesAnAlreadyCanonicalStructureUnchanged() {
    final StructType merged = StructureMerge.merge(awkwardOperands(), patient());

    assertEquals(merged, StructureMerge.merge(merged, merged, patient()));
  }

  @Test
  void canonicalisesASingleStructure() {
    final StructType nonCanonical =
        struct(field("given", array(STRING)), field("family", STRING), field("id", STRING));

    assertEquals(
        struct(field("id", STRING), field("family", STRING), field("given", array(STRING))),
        StructureMerge.merge(List.of(nonCanonical), humanName()));
  }

  @Test
  void rejectsAnEmptyListOfStructures() {
    assertThrows(
        IllegalArgumentException.class, () -> StructureMerge.merge(List.of(), humanName()));
  }

  @Test
  void descendsTheCanonicalStructureOnlyAsFarAsTheOperandsRequire() {
    // The canonical structure is self-recursive, as FHIR's extensions are, so an implementation
    // that expands children ahead of need would not terminate. The counters record how many times
    // a child was actually forced.
    final AtomicInteger extensionsForced = new AtomicInteger();
    final AtomicInteger namesForced = new AtomicInteger();
    final CanonicalStructure canonical =
        ordering("id", "name", "extension")
            .child(
                "name",
                () -> {
                  namesForced.incrementAndGet();
                  return ordering("family", "given");
                })
            .child("extension", () -> countingExtension(extensionsForced));

    final StructType left =
        struct(
            field("name", STRING),
            field(
                "extension",
                struct(field("url", STRING), field("extension", struct(field("url", STRING))))));
    final StructType right =
        struct(field("name", STRING), field("extension", struct(field("id", STRING))));

    final StructType expected =
        struct(
            field("name", STRING),
            field(
                "extension",
                struct(
                    field("id", STRING),
                    field("url", STRING),
                    field("extension", struct(field("url", STRING))))));

    assertEquals(expected, StructureMerge.merge(left, right, canonical));

    // The operands carry two levels of extension, so exactly two extension nodes are needed. A
    // third would mean the merge expanded past its operands, and on a cyclic structure that does
    // not terminate.
    assertEquals(2, extensionsForced.get());
    // The name field is a primitive on both sides, so nothing beneath it is ever needed. An
    // implementation that resolves every canonical name, rather than only the ones it descends
    // into, forces this one.
    assertEquals(0, namesForced.get());
  }

  @Test
  void fieldOrderIsPartOfTheType() {
    // Two structures carrying the same fields in a different order are different types, so every
    // assertion about order above is an assertion about correctness rather than about tidiness.
    assertNotEquals(
        struct(field("family", STRING), field("id", STRING)),
        struct(field("id", STRING), field("family", STRING)));
  }

  @Nonnull
  private static CanonicalStructure countingExtension(@Nonnull final AtomicInteger counter) {
    counter.incrementAndGet();
    return ordering("id", "url", "valueString", "extension")
        .child("extension", () -> countingExtension(counter));
  }

  /**
   * Operands chosen to exercise the cases a merge can get wrong: non-canonical internal order, a
   * structure present on one side only at depth, a field typed as the bottom type, and fields the
   * canonical structure does not know about.
   */
  @Nonnull
  private static List<StructType> awkwardOperands() {
    return List.of(
        struct(
            field(
                "name",
                struct(
                    field("given", array(STRING)),
                    field("id", STRING),
                    field("period", struct(field("start", STRING)))))),
        struct(
            field(
                "contact",
                struct(
                    field(
                        "name",
                        struct(
                            field("period", struct(field("end", STRING), field("start", STRING))),
                            field("family", STRING))),
                    field("telecom", array(struct(field("value", STRING))))))),
        struct(field("name", struct(field("period", DataTypes.NullType))), field("zebra", STRING)),
        struct(
            field("id", STRING),
            field("name", struct(field("family", STRING))),
            field("alpha", struct(field("y", STRING), field("x", STRING)))));
  }

  @Nonnull
  private static <T> List<List<T>> permutations(@Nonnull final List<T> items) {
    if (items.isEmpty()) {
      return List.of(List.of());
    }
    return IntStream.range(0, items.size())
        .boxed()
        .flatMap(
            index -> {
              final List<T> remaining = new ArrayList<>(items);
              final T head = remaining.remove(index.intValue());
              return permutations(remaining).stream()
                  .map(tail -> Stream.concat(Stream.of(head), tail.stream()).toList());
            })
        .toList();
  }

  @Nonnull
  private static StructType struct(@Nonnull final StructField... fields) {
    return new StructType(fields);
  }

  @Nonnull
  private static StructField field(@Nonnull final String name, @Nonnull final DataType type) {
    return new StructField(name, type, true, Metadata.empty());
  }

  @Nonnull
  private static DataType array(@Nonnull final DataType elementType) {
    return DataTypes.createArrayType(elementType, true);
  }
}
