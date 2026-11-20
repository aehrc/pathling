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

package au.csiro.pathling.library.io.source;

import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.io.FhirReader;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.TestHelpers;
import au.csiro.pathling.schema.DefinitionCanonicalStructure;
import au.csiro.pathling.utilities.CanonicalStructure;
import au.csiro.pathling.utilities.StructureMerge;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests that a structure whose fields are in a non-canonical order, as a schema obtained from
 * storage may be, is reconciled with one in definition order by name rather than by position
 * (decision 85, T118d).
 *
 * <p>The table is a Delta table widened by an append. Its first batch carries {@code name.family}
 * and {@code maritalStatus.coding.display} alone, and its second adds {@code name.text} and {@code
 * maritalStatus.coding.code}, which Delta's schema evolution appends after the fields the table
 * already has. So {@code Patient.name} is stored as {@code (family, text)} and {@code
 * Patient.maritalStatus.coding} as {@code (display, code)}, while {@code Patient.contact.name} and
 * {@code Patient.communication.language.coding}, first seen in the second batch, are stored in
 * definition order. Each pair has the same fields of the same type in different orders, so a
 * positional comparison would compare one field with another and give a wrong answer silently
 * rather than fail.
 *
 * <p>Equality and {@code union} are exercised over {@code Coding}, because the engine supports
 * equality on no other complex type: on {@code HumanName} both are refused as unsupported, whatever
 * the field order. {@code combine} is exercised over both.
 *
 * @author Piotr Szul
 */
class FieldOrderReconciliationTest {

  private static final String VIEW =
      """
      {
        "resourceType": "ViewDefinition",
        "resource": "Patient",
        "status": "active",
        "select": [{
          "column": [
            {"name": "id", "path": "id"},
            {"name": "equal", "type": "boolean",
             "path": "maritalStatus.coding.first() = communication.first().language.coding.first()"},
            {"name": "unionCount", "type": "integer",
             "path": "maritalStatus.coding.union(communication.language.coding).count()"},
            {"name": "unionCodes", "type": "string",
             "path": "maritalStatus.coding.union(communication.language.coding).code.join(',')"},
            {"name": "combinedCodes", "type": "string",
             "path": "maritalStatus.coding.combine(communication.language.coding).code.join(',')"},
            {"name": "combinedFamilies", "type": "string",
             "path": "name.combine(contact.name).family.join(',')"}
          ]
        }]
      }
      """;

  @TempDir static Path warehouse;

  private static SparkSession spark;
  private static PathlingContext context;

  /** Set up Spark and the Pathling context, and write the widened table. */
  @BeforeAll
  static void setUp() {
    spark = TestHelpers.spark();
    context = PathlingContext.create(spark);
    write(
        List.of(
            """
            {"resourceType":"Patient","id":"p0","name":[{"family":"Z"}],
             "maritalStatus":{"coding":[{"display":"Z"}]}}\
            """),
        "overwrite");
    // In p1 each pair carries each other's values, so the two differ, but read positionally across
    // the two orders they look the same. In p2 each pair is the same.
    write(
        List.of(
            """
            {"resourceType":"Patient","id":"p1","name":[{"family":"A","text":"B"}],
             "contact":[{"name":{"family":"B","text":"A"}}],
             "maritalStatus":{"coding":[{"code":"A","display":"B"}]},
             "communication":[{"language":{"coding":[{"code":"B","display":"A"}]}}]}\
            """,
            """
            {"resourceType":"Patient","id":"p2","name":[{"family":"C","text":"D"}],
             "contact":[{"name":{"family":"C","text":"D"}}],
             "maritalStatus":{"coding":[{"code":"C","display":"D"}]},
             "communication":[{"language":{"coding":[{"code":"C","display":"D"}]}}]}\
            """),
        "append");
  }

  /** Stop Spark. */
  @AfterAll
  static void tearDown() {
    spark.stop();
  }

  /** The structures of each pair really are stored in different orders. */
  @Test
  void eachPairIsStoredInDifferentOrders() {
    final StructType schema = patients().schema();
    final StructType contact = elementOf(schema, "contact");
    final StructType maritalStatus = (StructType) schema.apply("maritalStatus").dataType();
    final StructType language =
        (StructType) elementOf(schema, "communication").apply("language").dataType();

    assertEquals(List.of("family", "text"), fieldNames(elementOf(schema, "name")));
    assertEquals(
        List.of("text", "family"), fieldNames((StructType) contact.apply("name").dataType()));
    assertEquals(List.of("display", "code"), fieldNames(elementOf(maritalStatus, "coding")));
    assertEquals(List.of("code", "display"), fieldNames(elementOf(language, "coding")));

    // Compared positionally, by a struct cast of equal arity, p1's two codings look equal, so a
    // reconciliation that did not project by name would give the wrong answer below.
    final String codingType = elementOf(maritalStatus, "coding").sql();
    final boolean positionallyEqual =
        patients()
            .where("id = 'p1'")
            .selectExpr(
                "maritalStatus.coding[0] = CAST(communication[0].language.coding[0] AS "
                    + codingType
                    + ")")
            .first()
            .getBoolean(0);
    assertEquals(true, positionallyEqual);
  }

  /** Equality compares the fields by name. */
  @Test
  void equalityComparesByName() {
    final Map<String, Row> rows = rowsById();

    assertEquals(false, rows.get("p1").getAs("equal"));
    assertEquals(true, rows.get("p2").getAs("equal"));
  }

  /** {@code union} removes only the items that are equal by name. */
  @Test
  void unionDeduplicatesByName() {
    final Map<String, Row> rows = rowsById();

    assertEquals(2, (Integer) rows.get("p1").getAs("unionCount"));
    assertEquals("A,B", rows.get("p1").getAs("unionCodes"));
    assertEquals(1, (Integer) rows.get("p2").getAs("unionCount"));
    assertEquals("C", rows.get("p2").getAs("unionCodes"));
  }

  /** {@code combine} keeps each item's own value in each field. */
  @Test
  void combineKeepsEachFieldsValues() {
    final Map<String, Row> rows = rowsById();

    assertEquals("A,B", rows.get("p1").getAs("combinedCodes"));
    assertEquals("A,B", rows.get("p1").getAs("combinedFamilies"));
    assertEquals("C,C", rows.get("p2").getAs("combinedCodes"));
    assertEquals("C,C", rows.get("p2").getAs("combinedFamilies"));
  }

  /** The merged type is in definition order whichever order the operands arrive in. */
  @Test
  void theMergedTypeIsInDefinitionOrderWhateverTheOperandOrder() {
    final StructType schema = patients().schema();
    final StructType stored = elementOf(schema, "name");
    final StructType canonical =
        new StructType()
            .add("text", DataTypes.StringType)
            .add("family", DataTypes.StringType)
            .add("given", DataTypes.createArrayType(DataTypes.StringType));
    final CanonicalStructure humanName =
        DefinitionCanonicalStructure.forResource(
                FhirDefinitionContext.of(context.getFhirContext()), "Patient")
            .field("name")
            .orElseThrow();

    final StructType leftFirst = StructureMerge.merge(stored, canonical, humanName);
    final StructType rightFirst = StructureMerge.merge(canonical, stored, humanName);

    assertEquals(List.of("text", "family", "given"), fieldNames(leftFirst));
    assertEquals(leftFirst, rightFirst);
  }

  private static void write(@Nonnull final List<String> json, @Nonnull final String saveMode) {
    final Dataset<Row> batch =
        FhirReader.of(spark, FhirDefinitionContext.of(context.getFhirContext()))
            .json()
            .read("Patient", spark.createDataset(json, Encoders.STRING()));
    context
        .read()
        .datasets()
        .dataset("Patient", batch)
        .write()
        .saveMode(saveMode)
        .delta(warehouse.toString());
  }

  @Nonnull
  private static Dataset<Row> patients() {
    return context.read().delta(warehouse.toString()).read("Patient");
  }

  @Nonnull
  private static Map<String, Row> rowsById() {
    final Map<String, Row> rows = new TreeMap<>();
    context
        .read()
        .delta(warehouse.toString())
        .view("Patient")
        .json(VIEW)
        .execute()
        .collectAsList()
        .forEach(row -> rows.put(row.getAs("id"), row));
    return rows;
  }

  /** Returns the struct held by an array field. */
  @Nonnull
  private static StructType elementOf(
      @Nonnull final StructType struct, @Nonnull final String name) {
    return (StructType) ((ArrayType) struct.apply(name).dataType()).elementType();
  }

  @Nonnull
  private static List<String> fieldNames(@Nonnull final StructType struct) {
    return Arrays.asList(struct.fieldNames());
  }
}
