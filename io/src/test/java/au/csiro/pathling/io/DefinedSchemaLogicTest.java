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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.schema.SchemaConfiguration;
import au.csiro.pathling.test.JobCountingListener;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

/**
 * Tests the derivation of the observed shape and the layout schema from a schema defined up front
 * (T007, FR-007, FR-008, SC-009), and its canonicalisation (T057, FR-027).
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

  /**
   * Patients that between them populate a choice element with two of its types, an open choice at
   * the top level and inside a primitive metadata group, metadata groups at the top level and
   * inside an array of structs, and nested complex elements.
   */
  @Nonnull
  private static final List<String> PATIENTS =
      List.of(
          "{\"resourceType\":\"Patient\",\"id\":\"a\","
              + "\"extension\":[{\"url\":\"u1\",\"valueString\":\"x\"}],\"active\":true,"
              + "\"name\":[{\"family\":\"F\",\"given\":[\"G\"],\"_given\":[{\"id\":\"g1\"}]}],"
              + "\"gender\":\"male\",\"birthDate\":\"2000-01-01\","
              + "\"_birthDate\":{\"id\":\"b\",\"extension\":[{\"url\":\"u2\",\"valueCode\":\"c\"}]},"
              + "\"deceasedBoolean\":false,"
              + "\"maritalStatus\":{\"coding\":[{\"system\":\"s\",\"code\":\"M\"}],"
              + "\"text\":\"Married\"},\"multipleBirthInteger\":2}",
          "{\"resourceType\":\"Patient\",\"id\":\"b\","
              + "\"extension\":[{\"url\":\"u3\",\"valueInteger\":1}],"
              + "\"birthDate\":\"2001\","
              + "\"_birthDate\":{\"extension\":[{\"url\":\"u4\",\"valueString\":\"s\"}]},"
              + "\"deceasedDateTime\":\"2020-01-01\"}");

  /** An Observation whose quantity and date carry annotations under the default configuration. */
  @Nonnull
  private static final List<String> OBSERVATIONS =
      List.of(
          "{\"resourceType\":\"Observation\",\"id\":\"o\",\"status\":\"final\","
              + "\"code\":{\"text\":\"t\"},\"effectiveDateTime\":\"2020-01-01\","
              + "\"valueQuantity\":{\"value\":1.5,\"unit\":\"mg\","
              + "\"system\":\"http://unitsofmeasure.org\",\"code\":\"mg\"}}");

  /**
   * Returns the schema an inferred load of FHIR JSON gives, with primitive metadata stored and the
   * annotations of the default configuration, which is canonical by construction (SC-009).
   */
  @Nonnull
  private static StructType inferred(
      @Nonnull final String resourceType, @Nonnull final List<String> documents) {
    return Annotator.of(TransformFixtures.DEFINITIONS, SchemaConfiguration.builder().build())
        .annotate(
            resourceType,
            TransformFixtures.metadataReader()
                .json()
                .read(
                    resourceType,
                    TransformFixtures.spark()
                        .createDataset(new ArrayList<>(documents), Encoders.STRING())))
        .schema();
  }

  /** Returns a type with the fields of every struct within it in reverse order. */
  @Nonnull
  private static DataType reversed(@Nonnull final DataType type) {
    if (type instanceof final StructType struct) {
      final List<StructField> fields = new ArrayList<>();
      for (final StructField field : struct.fields()) {
        fields.add(
            new StructField(
                field.name(), reversed(field.dataType()), field.nullable(), field.metadata()));
      }
      Collections.reverse(fields);
      return DataTypes.createStructType(fields);
    }
    if (type instanceof final ArrayType array) {
      return DataTypes.createArrayType(reversed(array.elementType()), array.containsNull());
    }
    return type;
  }

  @Nonnull
  private static StructType reversed(@Nonnull final StructType type) {
    return (StructType) reversed((DataType) type);
  }

  @Test
  void canonicalisesPatientOutOfOrderAtEveryDepth() {
    final StructType canonical = inferred("Patient", PATIENTS);
    // The oracle holds every structure the canonical order places.
    final String catalog = canonical.catalogString();
    assertTrue(catalog.contains("deceasedBoolean:boolean,deceasedDateTime:"), catalog);
    assertTrue(catalog.contains("_birthDate:struct<"), catalog);
    assertTrue(catalog.contains("_given:array<"), catalog);
    assertTrue(catalog.contains("valueCode:"), catalog);

    final StructType outOfOrder = reversed(canonical);

    assertEquals(canonical, logic().canonicalSchema("Patient", outOfOrder));
  }

  @Test
  void canonicalisesAnnotationsToTheirCanonicalPlaces() {
    final StructType canonical = inferred("Observation", OBSERVATIONS);
    assertTrue(canonical.catalogString().contains("__canonical_code"), canonical.catalogString());

    assertEquals(canonical, logic().canonicalSchema("Observation", reversed(canonical)));
  }

  @Test
  void returnsCanonicalSchemaUnchanged() {
    final StructType patient = inferred("Patient", PATIENTS);
    final StructType observation = inferred("Observation", OBSERVATIONS);

    assertEquals(patient, logic().canonicalSchema("Patient", patient));
    assertEquals(observation, logic().canonicalSchema("Observation", observation));
  }

  @Test
  void keepsNullabilityAndFieldMetadataOfEachField() {
    // Canonicalisation only reorders, so a field keeps its nullability and metadata wherever it
    // goes.
    final Metadata metadata = new MetadataBuilder().putString("comment", "given").build();
    final StructField family =
        DataTypes.createStructField("family", DataTypes.StringType, false, metadata);
    final StructField given =
        DataTypes.createStructField(
            "given", DataTypes.createArrayType(DataTypes.StringType, false), false);
    final StructField resourceType =
        DataTypes.createStructField("resourceType", DataTypes.StringType, false, metadata);
    final StructField id = DataTypes.createStructField("id", DataTypes.StringType, false);
    final StructType schema =
        DataTypes.createStructType(
            Arrays.asList(
                DataTypes.createStructField(
                    "name",
                    DataTypes.createArrayType(
                        DataTypes.createStructType(Arrays.asList(given, family)), false),
                    false),
                id,
                resourceType));
    final StructType expected =
        DataTypes.createStructType(
            Arrays.asList(
                resourceType,
                id,
                DataTypes.createStructField(
                    "name",
                    DataTypes.createArrayType(
                        DataTypes.createStructType(Arrays.asList(family, given)), false),
                    false)));

    assertEquals(expected, logic().canonicalSchema("Patient", schema));
  }

  @Test
  void dropsFieldsTheDefinitionsDoNotDescribe() {
    final StructType schema =
        StructType.fromDDL(
            "unknown STRING, name ARRAY<STRUCT<given: ARRAY<STRING>, nickname: STRING,"
                + " family: STRING>>, id STRING, resourceType STRING");

    assertEquals(
        StructType.fromDDL(
            "resourceType STRING, id STRING,"
                + " name ARRAY<STRUCT<family: STRING, given: ARRAY<STRING>>>"),
        logic().canonicalSchema("Patient", schema));
  }

  @Test
  void refusesFieldWhoseTypeContradictsTheDefinitions() {
    final StructType schema = StructType.fromDDL("id STRING, resourceType STRING, birthDate INT");

    final IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class, () -> logic().canonicalSchema("Patient", schema));
    assertTrue(error.getMessage().contains("Patient"), error.getMessage());
    assertTrue(error.getMessage().contains("birthDate"), error.getMessage());
  }

  @Test
  void refusesResourceTypeAndMetadataGroupFieldsThatContradictTheDefinitions() {
    // The resource type held as a number, the id of a metadata group held as a number, its
    // extensions held as a single struct or as an array of strings, and the url of one of its
    // extensions held as a number.
    final Map<String, String> pathsByDdl =
        Map.of(
            "resourceType INT", "resourceType",
            "resourceType STRING, _birthDate STRUCT<id: INT>", "_birthDate.id",
            "resourceType STRING, _birthDate STRUCT<extension: STRUCT<url: STRING>>",
                "_birthDate.extension",
            "resourceType STRING, _birthDate STRUCT<extension: ARRAY<STRING>>",
                "_birthDate.extension",
            "resourceType STRING, _birthDate STRUCT<extension: ARRAY<STRUCT<url: INT>>>",
                "_birthDate.extension.url");
    pathsByDdl.forEach(
        (ddl, path) -> {
          final StructType schema = StructType.fromDDL(ddl);

          final IllegalArgumentException error =
              assertThrows(
                  IllegalArgumentException.class,
                  () -> logic().canonicalSchema("Patient", schema),
                  ddl);
          assertTrue(error.getMessage().contains("Patient"), error.getMessage());
          assertTrue(error.getMessage().contains(path), error.getMessage());
        });
  }

  @Test
  void refusesStructureThatNamesAFieldMoreThanOnce() {
    // A field named twice at the top level, within an element, and within a metadata group.
    final StructType gender = new StructType().add("gender", DataTypes.StringType);
    final Map<StructType, String> pathsBySchema =
        Map.of(
            gender.add("resourceType", DataTypes.StringType).add("gender", DataTypes.StringType),
            "gender",
            new StructType()
                .add("resourceType", DataTypes.StringType)
                .add(
                    "name",
                    DataTypes.createArrayType(
                        new StructType()
                            .add("family", DataTypes.StringType)
                            .add("family", DataTypes.StringType))),
            "name.family",
            new StructType()
                .add("resourceType", DataTypes.StringType)
                .add(
                    "_birthDate",
                    new StructType()
                        .add("id", DataTypes.StringType)
                        .add("id", DataTypes.StringType)),
            "_birthDate.id");
    pathsBySchema.forEach(
        (schema, path) -> {
          final IllegalArgumentException error =
              assertThrows(
                  IllegalArgumentException.class,
                  () -> logic().canonicalSchema("Patient", schema),
                  schema.simpleString());
          assertTrue(error.getMessage().contains("Patient"), error.getMessage());
          assertTrue(error.getMessage().contains(path + " more than once"), error.getMessage());
        });
  }

  @Test
  void refusesFieldWhoseCardinalityContradictsTheDefinitions() {
    // A repeating element held as a single struct, a single element held as an array, and a
    // nested repeating element held as a single value.
    final Map<String, String> pathsByDdl =
        Map.of(
            "resourceType STRING, name STRUCT<family: STRING>", "name",
            "resourceType STRING, gender ARRAY<STRING>", "gender",
            "resourceType STRING, name ARRAY<STRUCT<given: STRING>>", "name.given");
    pathsByDdl.forEach(
        (ddl, path) -> {
          final StructType schema = StructType.fromDDL(ddl);

          final IllegalArgumentException error =
              assertThrows(
                  IllegalArgumentException.class,
                  () -> logic().canonicalSchema("Patient", schema),
                  ddl);
          assertTrue(error.getMessage().contains("Patient"), error.getMessage());
          assertTrue(error.getMessage().contains(path), error.getMessage());
        });
  }

  @Test
  void canonicalisesWithoutRunningAJob() throws TimeoutException {
    final StructType outOfOrder = reversed(inferred("Patient", PATIENTS));
    assertEquals(
        0,
        JobCountingListener.jobsRunBy(
            TransformFixtures.spark().sparkContext(),
            () -> logic().canonicalSchema("Patient", outOfOrder)));
  }
}
