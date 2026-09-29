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

package au.csiro.pathling.fhirpath.encoding;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.annotation.Nonnull;
import java.util.stream.Stream;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Coding;
import org.junit.jupiter.api.Test;

/**
 * Tests that a Coding is decoded by field name, against the schema of the row being decoded, rather
 * than by position in the canonical structure (T083, T084, FR-031).
 *
 * <p>Spark hands a user-defined function a row that carries its schema, so these tests build the
 * same kind of row directly. A structure narrower than the canonical one is what a pruned
 * new-layout schema produces for a Coding that populates only some of its elements.
 *
 * @author Piotr Szul
 */
class CodingSchemaTest {

  private static final String SYSTEM = "http://snomed.info/sct";

  @Test
  void fullCanonicalStructureDecodesAsItDoesToday() {
    final Coding coding =
        new Coding(SYSTEM, "404684003", "Clinical finding")
            .setVersion("http://snomed.info/sct/32506021000036107");
    coding.setUserSelected(true);
    final Row row =
        new GenericRowWithSchema(values(CodingSchema.encode(coding)), CodingSchema.DATA_TYPE);

    final Coding decoded = CodingSchema.decode(row);

    assertNotNull(decoded);
    assertTrue(decoded.equalsDeep(coding));
  }

  @Test
  void positionalRowWithoutSchemaDecodesAsItDoesToday() {
    // A row built without a schema can only be read by the canonical positions.
    final Coding coding = new Coding(SYSTEM, "404684003", "Clinical finding").setVersion("v1");

    final Coding decoded = CodingSchema.decode(CodingSchema.encode(coding));

    assertNotNull(decoded);
    assertTrue(decoded.equalsDeep(coding));
  }

  @Test
  void narrowStructureDecodesWithTheRemainingPropertiesNull() {
    final StructType schema =
        new StructType()
            .add(CodingSchema.CODE_FIELD, DataTypes.StringType)
            .add(CodingSchema.SYSTEM_FIELD, DataTypes.StringType);
    final Row row = new GenericRowWithSchema(new Object[] {"404684003", SYSTEM}, schema);

    final Coding decoded = CodingSchema.decode(row);

    assertNotNull(decoded);
    assertEquals(SYSTEM, decoded.getSystem());
    assertEquals("404684003", decoded.getCode());
    assertNull(decoded.getVersion());
    assertNull(decoded.getDisplay());
    assertFalse(decoded.hasUserSelected());
  }

  @Test
  void reorderedFieldsFollowNamesRatherThanPositions() {
    // The new layout orders a structure's fields as its definition does and may add fields the
    // canonical structure does not have, such as an inline extension.
    final StructType schema =
        new StructType()
            .add(CodingSchema.USER_SELECTED_FIELD, DataTypes.BooleanType)
            .add(CodingSchema.DISPLAY_FIELD, DataTypes.StringType)
            .add("extension", DataTypes.createArrayType(DataTypes.StringType))
            .add(CodingSchema.CODE_FIELD, DataTypes.StringType)
            .add(CodingSchema.VERSION_FIELD, DataTypes.StringType)
            .add(CodingSchema.SYSTEM_FIELD, DataTypes.StringType)
            .add(CodingSchema.ID_FIELD, DataTypes.StringType);
    final Row row =
        new GenericRowWithSchema(
            new Object[] {false, "Clinical finding", null, "404684003", "v1", SYSTEM, "c1"},
            schema);

    final Coding decoded = CodingSchema.decode(row);

    assertNotNull(decoded);
    assertEquals(SYSTEM, decoded.getSystem());
    assertEquals("v1", decoded.getVersion());
    assertEquals("404684003", decoded.getCode());
    assertEquals("Clinical finding", decoded.getDisplay());
    assertTrue(decoded.hasUserSelected());
    assertFalse(decoded.getUserSelected());
  }

  @Test
  void previousLayoutStructureWithFieldIdentifierDecodesByName() {
    // The previous layout's Coding carries a field identifier after its canonical fields.
    final StructType schema =
        new StructType()
            .add(CodingSchema.SYSTEM_FIELD, DataTypes.StringType)
            .add(CodingSchema.CODE_FIELD, DataTypes.StringType)
            .add(CodingSchema.FID_FIELD, DataTypes.IntegerType);
    final Row row = new GenericRowWithSchema(new Object[] {SYSTEM, "404684003", 7}, schema);

    final Coding decoded = CodingSchema.decode(row);

    assertNotNull(decoded);
    assertEquals(SYSTEM, decoded.getSystem());
    assertEquals("404684003", decoded.getCode());
  }

  @Test
  void nullRowDecodesAsNull() {
    assertNull(CodingSchema.decode(null));
  }

  @Test
  void structureWithoutAnyCodingFieldIsRejected() {
    final StructType schema =
        new StructType()
            .add("family", DataTypes.StringType)
            .add("given", DataTypes.createArrayType(DataTypes.StringType));
    final Row row = new GenericRowWithSchema(new Object[] {"Smith", null}, schema);

    final IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> CodingSchema.decode(row));
    // The message names the fields a Coding may have and the fields the structure has.
    Stream.of("system", "code", "userSelected", "family", "given")
        .forEach(name -> assertTrue(error.getMessage().contains(name), error.getMessage()));
  }

  @Test
  void unnamedStructureIsRejected() {
    // An unnamed struct, whose fields Spark names col1 to col6, is not read by position: a Coding
    // must carry its field names.
    final StructType schema =
        new StructType()
            .add("col1", DataTypes.NullType)
            .add("col2", DataTypes.StringType)
            .add("col3", DataTypes.StringType)
            .add("col4", DataTypes.StringType)
            .add("col5", DataTypes.NullType)
            .add("col6", DataTypes.NullType);
    final Row row =
        new GenericRowWithSchema(
            new Object[] {null, SYSTEM, "v1", "404684003", null, null}, schema);

    final IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> CodingSchema.decode(row));
    Stream.of("system", "code", "userSelected", "col1", "col6")
        .forEach(name -> assertTrue(error.getMessage().contains(name), error.getMessage()));
  }

  @Nonnull
  private static Object[] values(@Nonnull final Row row) {
    final Object[] values = new Object[row.length()];
    for (int i = 0; i < row.length(); i++) {
      values[i] = row.get(i);
    }
    return values;
  }
}
