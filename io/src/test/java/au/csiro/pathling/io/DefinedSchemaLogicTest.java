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

import au.csiro.pathling.schema.SchemaConfiguration;
import jakarta.annotation.Nonnull;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

/**
 * Tests the derivation of the observed shape and the layout schema from a schema defined up front
 * (T007, FR-007, FR-008, SC-009).
 */
class DefinedSchemaLogicTest {

  /** A Patient schema in canonical field order, which holds every primitive kind it tests. */
  @Nonnull
  private static final StructType PATIENT =
      StructType.fromDDL(
          "resourceType STRING, id STRING, active BOOLEAN, gender STRING, birthDate STRING,"
              + " multipleBirthInteger INT, photo ARRAY<STRUCT<data: BINARY, size: INT>>");

  /** An Observation schema in canonical field order, without annotations. */
  @Nonnull
  private static final StructType OBSERVATION =
      StructType.fromDDL(
          "resourceType STRING, id STRING, status STRING, code STRUCT<text: STRING>,"
              + " effectiveDateTime STRING,"
              + " valueQuantity STRUCT<value: STRING, unit: STRING, system: STRING, code: STRING>");

  /** The Observation schema with the annotations of the default configuration. */
  @Nonnull
  private static final StructType ANNOTATED_OBSERVATION =
      StructType.fromDDL(
          "resourceType STRING, id STRING, status STRING, code STRUCT<text: STRING>,"
              + " effectiveDateTime STRING,"
              + " _effectiveDateTime STRUCT<__start: TIMESTAMP, __end: TIMESTAMP>,"
              + " valueQuantity STRUCT<value: STRING, _value: STRUCT<__numeric: DECIMAL(38,6)>,"
              + " unit: STRING, system: STRING, code: STRING, __canonical_code: STRING,"
              + " __canonical_value_exact: STRUCT<value: DECIMAL(38,0), scale: INT>>");

  /**
   * Returns the catalog string of a type with every integer written as {@code int}, because the
   * ingest walk takes a FHIR integer from either Spark integer type (research R2).
   */
  @Nonnull
  private static String withIntegers(@Nonnull final StructType type) {
    return type.catalogString().replace("bigint", "int");
  }

  @Nonnull
  private static DefinedSchemaLogic logic() {
    return DefinedSchemaLogic.of(TransformFixtures.spark(), TransformFixtures.DEFINITIONS);
  }

  @Test
  void mapsEachPrimitiveKindToItsIngestType() {
    // A base64Binary is read as its text, and a decimal as a string (research R2, R3).
    final StructType expected =
        StructType.fromDDL(
            "resourceType STRING, id STRING, active BOOLEAN, gender STRING, birthDate STRING,"
                + " multipleBirthInteger INT, photo ARRAY<STRUCT<data: STRING, size: INT>>");

    assertEquals(withIntegers(expected), withIntegers(logic().observedShape("Patient", PATIENT)));
    assertEquals(
        OBSERVATION.catalogString(),
        logic().observedShape("Observation", OBSERVATION).catalogString());
  }

  @Test
  void mapsMetadataGroupsChoicesAndComplexElements() {
    final StructType schema =
        StructType.fromDDL(
            "resourceType STRING, id STRING,"
                + " name ARRAY<STRUCT<family: STRING, given: ARRAY<STRING>,"
                + " _given: ARRAY<STRUCT<id: STRING>>>>,"
                + " birthDate STRING,"
                + " _birthDate STRUCT<id: STRING,"
                + " extension: ARRAY<STRUCT<url: STRING, valueCode: STRING>>>,"
                + " deceasedBoolean BOOLEAN,"
                + " maritalStatus STRUCT<coding: ARRAY<STRUCT<system: STRING, code: STRING>>,"
                + " text: STRING>");

    assertEquals(schema.catalogString(), logic().observedShape("Patient", schema).catalogString());
  }

  @Test
  void skipsAnnotationFields() {
    // A metadata group that holds only annotations is dropped with them.
    assertEquals(
        OBSERVATION.catalogString(),
        logic().observedShape("Observation", ANNOTATED_OBSERVATION).catalogString());

    final StructType patient =
        StructType.fromDDL(
            "resourceType STRING, id STRING, birthDate STRING,"
                + " _birthDate STRUCT<id: STRING, __start: TIMESTAMP, __end: TIMESTAMP>");
    assertEquals(
        "struct<resourceType:string,id:string,birthDate:string,_birthDate:struct<id:string>>",
        logic().observedShape("Patient", patient).catalogString());
  }

  @Test
  void acceptsAnnotationFieldsTheDefinitionsDoNotDescribe() {
    final StructType schema =
        StructType.fromDDL(
            "resourceType STRING, id STRING, __custom STRING, status STRING,"
                + " valueQuantity STRUCT<value: STRING, __custom: INT, unit: STRING,"
                + " system: STRING, code: STRING>");
    final StructType base =
        StructType.fromDDL(
            "resourceType STRING, id STRING, status STRING,"
                + " valueQuantity STRUCT<value: STRING, unit: STRING, system: STRING,"
                + " code: STRING>");

    assertEquals(
        base.catalogString(), logic().observedShape("Observation", schema).catalogString());
    assertEquals(base, logic().layoutSchema("Observation", schema));
  }

  @Test
  void layoutSchemaEqualsBaseSchema() {
    assertEquals(PATIENT, logic().layoutSchema("Patient", PATIENT));
    assertEquals(OBSERVATION, logic().layoutSchema("Observation", ANNOTATED_OBSERVATION));
  }

  @Test
  void layoutSchemaKeepsGivenFieldOrder() {
    // Out of canonical order at the top level, inside a struct, inside an array of structs, and
    // inside a primitive metadata group and the extensions it holds (SC-009).
    final StructType schema =
        StructType.fromDDL(
            "birthDate STRING, id STRING,"
                + " maritalStatus STRUCT<text: STRING,"
                + " coding: ARRAY<STRUCT<code: STRING, system: STRING>>>,"
                + " resourceType STRING,"
                + " name ARRAY<STRUCT<given: ARRAY<STRING>, family: STRING>>,"
                + " _birthDate STRUCT<extension: ARRAY<STRUCT<valueCode: STRING, url: STRING>>,"
                + " id: STRING>,"
                + " active BOOLEAN");

    assertEquals(schema, logic().layoutSchema("Patient", schema));
  }

  @Test
  void annotationsGoToTheirCanonicalPlacesWhateverTheGivenOrder() {
    // The given schema is out of canonical order, and carries annotations out of their places.
    final StructType schema =
        StructType.fromDDL(
            "id STRING, effectiveDateTime STRING, resourceType STRING,"
                + " valueQuantity STRUCT<__canonical_code: STRING, unit: STRING, value: STRING,"
                + " code: STRING, system: STRING>,"
                + " _effectiveDateTime STRUCT<__end: TIMESTAMP, __start: TIMESTAMP>,"
                + " status STRING");
    final StructType layout = logic().layoutSchema("Observation", schema);

    final StructType annotated =
        Annotator.of(TransformFixtures.DEFINITIONS, SchemaConfiguration.builder().build())
            .annotate(
                "Observation", TransformFixtures.spark().createDataFrame(List.<Row>of(), layout))
            .schema();

    assertEquals(
        "struct<id:string,effectiveDateTime:string,"
            + "_effectiveDateTime:struct<__start:timestamp,__end:timestamp>,resourceType:string,"
            + "valueQuantity:struct<unit:string,value:string,_value:struct<__numeric:decimal(38,6)>,"
            + "code:string,system:string,__canonical_code:string,"
            + "__canonical_value_exact:struct<value:decimal(38,0),scale:int>>,status:string>",
        annotated.catalogString());
  }

  @Test
  void layoutSchemaIsNullableAndCarriesNoMetadata() {
    final Metadata metadata = new MetadataBuilder().putString("comment", "given").build();
    final StructType schema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("resourceType", DataTypes.StringType, false, metadata),
              DataTypes.createStructField("id", DataTypes.StringType, false),
              DataTypes.createStructField(
                  "name",
                  DataTypes.createArrayType(
                      DataTypes.createStructType(
                          new StructField[] {
                            DataTypes.createStructField(
                                "family", DataTypes.StringType, false, metadata),
                            DataTypes.createStructField(
                                "given",
                                DataTypes.createArrayType(DataTypes.StringType, false),
                                false)
                          }),
                      false),
                  false)
            });

    assertEquals(
        StructType.fromDDL(
            "resourceType STRING, id STRING,"
                + " name ARRAY<STRUCT<family: STRING, given: ARRAY<STRING>>>"),
        logic().layoutSchema("Patient", schema));
  }
}
