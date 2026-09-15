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
import static au.csiro.pathling.schema.SchemaFixtures.at;
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
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

/**
 * Tests that schema derivation takes element types and cardinality from the FHIR definitions and
 * never from the shape of the data (FR-008, FR-012).
 *
 * <p>The structures standing in for an inferred read schema are deliberately at odds with the
 * definitions: a repeating element carrying a single value, a singular element carrying several,
 * and a numeric type where the layout stores text. The derived schema must ignore all of it.
 */
class SchemaBuilderTest {

  @Nonnull
  private static StructType densePatient() {
    return builder().dense("Patient");
  }

  @Test
  void takesPrimitiveTypesFromTheDefinitions() {
    final StructType patient = densePatient();
    assertEquals(DataTypes.StringType, at(patient, "id"));
    assertEquals(DataTypes.BooleanType, at(patient, "active"));
    assertEquals(DataTypes.StringType, at(patient, "birthDate"));
    assertEquals(DataTypes.StringType, at(patient, "gender"));
    assertEquals(DataTypes.IntegerType, at(patient, "multipleBirthInteger"));
    assertEquals(DataTypes.BooleanType, at(patient, "multipleBirthBoolean"));
  }

  @Test
  void storesADecimalAsTextSoThatItsLexicalFormSurvives() {
    final StructType observation = builder().dense("Observation");
    assertEquals(DataTypes.StringType, at(observation, "valueQuantity", "value"));
  }

  @Test
  void representsARepeatingElementAsAnArrayWhateverTheDataCarried() {
    // The inferred schema of a document carrying one name as an object, and one given name as a
    // bare string, rather than as the arrays the definitions call for.
    final StructType observed =
        struct(
            field("resourceType", DataTypes.StringType),
            field("name", struct(field("given", DataTypes.StringType))));
    final StructType derived = builder().pruned("Patient", observed);

    assertEquals(array(structAt(derived, "name")), at(derived, "name"));
    assertEquals(array(DataTypes.StringType), at(derived, "name", "given"));
  }

  @Test
  void representsASingularElementAsAScalarWhateverTheDataCarried() {
    // The inferred schema of a document that repeated elements the definitions declare singular.
    final StructType observed =
        struct(
            field("resourceType", DataTypes.StringType),
            field("birthDate", array(DataTypes.StringType)),
            field("maritalStatus", array(struct(field("text", DataTypes.StringType)))));
    final StructType derived = builder().pruned("Patient", observed);

    assertEquals(DataTypes.StringType, at(derived, "birthDate"));
    assertTrue(at(derived, "maritalStatus") instanceof StructType);
  }

  @Test
  void takesTypesFromTheDefinitionsWhereTheDataDisagrees() {
    // The inferred schema of a document whose decimal was read as a double and whose date was read
    // as a timestamp. Neither may reach the derived schema.
    final StructType observed =
        struct(
            field("resourceType", DataTypes.StringType),
            field("birthDate", DataTypes.TimestampType),
            field("multipleBirthInteger", DataTypes.DoubleType));
    final StructType derived = builder().pruned("Patient", observed);

    assertEquals(DataTypes.StringType, at(derived, "birthDate"));
    assertEquals(DataTypes.IntegerType, at(derived, "multipleBirthInteger"));
  }

  @Test
  void derivesAComplexElementAsAStructureOfItsElements() {
    final StructType patient = densePatient();
    assertEquals(
        List.of(
            "id", "_id", "use", "_use", "text", "_text", "family", "_family", "given", "_given",
            "prefix", "_prefix", "suffix", "_suffix", "period"),
        names(structAt(patient, "name")));
    assertEquals(array(DataTypes.StringType), at(patient, "name", "given"));
    assertEquals(DataTypes.StringType, at(patient, "name", "family"));
    assertEquals(DataTypes.StringType, at(patient, "name", "period", "start"));
  }

  @Test
  void givesAPrimitiveMetadataGroupTheCardinalityOfTheElementItAccompanies() {
    final StructType patient = densePatient();
    // The group beside a repeating primitive repeats with it, because one id or extension belongs
    // to each value.
    assertTrue(at(patient, "name", "_given") instanceof ArrayType);
    assertTrue(at(patient, "name", "_family") instanceof StructType);
  }

  @Test
  void doesNotRepresentContainedResources() {
    assertFalse(names(densePatient()).contains("contained"));
  }

  @Test
  void expandsAChoiceIntoOneFieldPerType() {
    final List<String> fields = names(densePatient());
    assertTrue(fields.contains("deceasedBoolean"));
    assertTrue(fields.contains("deceasedDateTime"));
    assertFalse(fields.contains("deceased"));
  }
}
