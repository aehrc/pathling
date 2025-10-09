/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2025 Commonwealth Scientific 
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
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

package au.csiro.pathling.encoders;

import static au.csiro.pathling.test.SchemaAsserts.assertFieldNotPresent;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.encoders.datatypes.DataTypeMappings;
import au.csiro.pathling.encoders.datatypes.R4DataTypeMappings;
import au.csiro.pathling.sql.types.FlexiDecimal;
import ca.uhn.fhir.context.FhirContext;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.Device;
import org.hl7.fhir.r4.model.MedicationRequest;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Questionnaire;
import org.hl7.fhir.r4.model.QuestionnaireResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.jdk.javaapi.CollectionConverters;

public class SchemaConverterTest {

  public static final Set<String> OPEN_TYPES = Set.of(
      "boolean",
      "canonical",
      "code",
      "date",
      "dateTime",
      "decimal",
      "instant",
      "integer",
      "oid",
      "positiveInt",
      "string",
      "time",
      "unsignedInt",
      "uri",
      "url",
      "Coding",
      "Identifier"
  );

  private static final FhirContext FHIR_CONTEXT = FhirContext.forR4();
  private static final DataTypeMappings DATA_TYPE_MAPPINGS = new R4DataTypeMappings();

  private SchemaConverter converterL0;
  private SchemaConverter converterL1;
  private SchemaConverter converterL2;

  private StructType conditionSchema;
  private StructType observationSchema;
  private StructType medRequestSchema;
  private StructType questionnaireSchema;
  private StructType questionnaireResponseSchema;
  private StructType deviceSchema;
  private StructType observationSchemaL2;

  /**
   * Traverses a DataType recursively passing all encountered StructTypes to the provided consumer.
   *
   * @param type the DataType to traverse.
   * @param consumer the consumer that receives all StructTypes.
   */
  private void traverseSchema(final DataType type, final Consumer<StructType> consumer) {
    if (type instanceof final StructType structType) {
      consumer.accept(structType);
      Arrays.stream(structType.fields())
          .filter(f -> !f.name().startsWith("_")) // filter out synthetic fields
          .forEach(f -> traverseSchema(f.dataType(), consumer));
    } else if (type instanceof ArrayType) {
      traverseSchema(((ArrayType) type).elementType(), consumer);
    } else if (type instanceof MapType) {
      traverseSchema(((MapType) type).keyType(), consumer);
      traverseSchema(((MapType) type).valueType(), consumer);
    }
  }

  private SchemaConverter createSchemaConverter(final int maxNestingLevel) {
    return new SchemaConverter(FHIR_CONTEXT, DATA_TYPE_MAPPINGS,
        EncoderConfig.apply(maxNestingLevel, CollectionConverters.asScala(OPEN_TYPES).toSet(), true));
  }

  /**
   * Returns the type of a nested field.
   */
  private static DataType getField(final DataType dataType, final boolean isNullable,
      final String... names) {

    final StructType schema = dataType instanceof ArrayType
                              ? (StructType) ((ArrayType) dataType).elementType()
                              : (StructType) dataType;

    final StructField field = Arrays.stream(schema.fields())
        .filter(sf -> sf.name().equalsIgnoreCase(names[0]))
        .findFirst()
        .orElseThrow();

    final DataType child = field.dataType();

    // Recurse through children if there are more names.
    if (names.length == 1) {

      // Check the nullability.
      assertEquals(isNullable,
          field.nullable(),
          "Unexpected nullability of field " + field.name());

      return child;
    } else {
      return getField(child, isNullable, Arrays.copyOfRange(names, 1, names.length));
    }
  }

  private static DataType unArray(final DataType maybeArrayType) {
    return maybeArrayType instanceof ArrayType
           ?
           ((ArrayType) maybeArrayType).elementType()
           : maybeArrayType;
  }

  @BeforeEach
  void setUp() {
    converterL0 = createSchemaConverter(0);
    converterL1 = createSchemaConverter(1);
    converterL2 = createSchemaConverter(2);

    conditionSchema = converterL0.resourceSchema(Condition.class);
    observationSchema = converterL0.resourceSchema(Observation.class);
    medRequestSchema = converterL0.resourceSchema(MedicationRequest.class);
    questionnaireSchema = converterL0.resourceSchema(Questionnaire.class);
    questionnaireResponseSchema = converterL0.resourceSchema(QuestionnaireResponse.class);
    deviceSchema = converterL0.resourceSchema(Device.class);
    observationSchemaL2 = converterL2.resourceSchema(Observation.class);
  }

  @Test
  void resourceHasId() {
    assertInstanceOf(StringType.class, getField(conditionSchema, true, "id"));
  }

  @Test
  void boundCodeToStruct() {
    assertInstanceOf(StructType.class, getField(conditionSchema, true, "verificationStatus"));
  }

  @Test
  void codingToStruct() {

    final DataType codingType = getField(conditionSchema, true, "severity", "coding");

    assertInstanceOf(StringType.class, getField(codingType, true, "system"));
    assertInstanceOf(StringType.class, getField(codingType, true, "version"));
    assertInstanceOf(StringType.class, getField(codingType, true, "code"));
    assertInstanceOf(StringType.class, getField(codingType, true, "display"));
    assertInstanceOf(BooleanType.class, getField(codingType, true, "userSelected"));
  }

  @Test
  void codeableConceptToStruct() {

    final DataType codeableType = getField(conditionSchema, true, "severity");

    assertInstanceOf(StructType.class, codeableType);
    assertInstanceOf(ArrayType.class, getField(codeableType, true, "coding"));
    assertInstanceOf(StringType.class, getField(codeableType, true, "text"));
  }

  @Test
  void idToString() {
    assertInstanceOf(StringType.class, getField(conditionSchema, true, "id"));
  }

  @Test
  void narrativeToStruct() {

    assertInstanceOf(StringType.class, getField(conditionSchema, true, "text", "status"));
    assertInstanceOf(StringType.class, getField(conditionSchema, true, "text", "div"));
  }

  @Test
  void expandChoiceFields() {
    assertInstanceOf(StructType.class, getField(conditionSchema, true, "onsetPeriod"));
    assertInstanceOf(StructType.class, getField(conditionSchema, true, "onsetRange"));
    assertInstanceOf(StringType.class, getField(conditionSchema, true, "onsetDateTime"));
    assertInstanceOf(StringType.class, getField(conditionSchema, true, "onsetString"));
    assertInstanceOf(StructType.class, getField(conditionSchema, true, "onsetAge"));
  }

  @Test
  void orderChoiceFields() {
    final List<String> expectedFields = Arrays
        .asList("valueBoolean", "valueCodeableConcept", "valueDateTime",
            "valueInteger", "valuePeriod", "valueQuantity", "valueRange",
            "valueRatio", "valueSampledData", "valueString", "valueTime");

    final List<String> actualFields = Stream.of(observationSchema.fieldNames())
        .filter(fn -> fn.startsWith("value"))
        .toList();

    assertEquals(expectedFields, actualFields);
  }

  @Test
  void decimalWithinChoiceField() {
    assertInstanceOf(DecimalType.class, getField(questionnaireSchema, true, "item", "enableWhen",
        "answerDecimal"));
    assertInstanceOf(IntegerType.class, getField(questionnaireSchema, true, "item", "enableWhen",
        "answerDecimal_scale"));
    assertInstanceOf(DecimalType.class,
        getField(questionnaireResponseSchema, true, "item", "answer",
            "valueDecimal"));
    assertInstanceOf(IntegerType.class,
        getField(questionnaireResponseSchema, true, "item", "answer",
            "valueDecimal_scale"));
  }

  @Test
  void instantToTimestamp() {
    assertInstanceOf(TimestampType.class, getField(observationSchema, true, "issued"));
  }

  @Test
  void timeToString() {
    assertTrue((getField(observationSchema, true, "valueTime") instanceof StringType));
  }

  @Test
  void bigDecimalToDecimal() {
    assertInstanceOf(DecimalType.class,
        getField(observationSchema, true, "valueQuantity", "value"));
  }

  @Test
  void reference() {
    assertInstanceOf(StringType.class, getField(observationSchema, true, "subject", "id"));
    assertInstanceOf(StringType.class, getField(observationSchema, true, "subject", "reference"));
    assertInstanceOf(StringType.class, getField(observationSchema, true, "subject", "display"));
    assertInstanceOf(StringType.class, getField(observationSchema, true, "subject", "type"));
    assertInstanceOf(StructType.class, getField(observationSchema, true, "subject", "identifier"));
    assertInstanceOf(StringType.class,
        getField(observationSchema, true, "subject", "identifier", "value"));

  }

  @Test
  void identifier() {
    assertInstanceOf(StringType.class,
        unArray(getField(observationSchema, true, "identifier", "value")));
    // `assigner` field should be present in the root level `Identifier` schema.
    assertInstanceOf(StructType.class,
        unArray(getField(observationSchema, true, "identifier", "assigner")));
    assertInstanceOf(StringType.class,
        unArray(getField(observationSchema, true, "identifier", "assigner", "reference")));

  }

  @Test
  void identifierInReference() {
    // 
    // Identifier (assigner) in root Reference
    // 
    assertFieldNotPresent("assigner", getField(observationSchema, true, "subject", "identifier"));
    // The `assigner` field should not be present in Identifier schema of the Reference `identifier` field.
    assertFieldNotPresent("assigner",
        getField(observationSchemaL2, true, "subject", "identifier"));

    // 
    //  Identifier (assigner) in a Reference nested in an Identifier
    //
    // the `identifier` field should not be present because for normal nesting rules for 0-level nesting
    assertFieldNotPresent("identifier",
        unArray(getField(observationSchema, true, "identifier", "assigner")));
    // the `identifier` field should be present because for normal nesting rules for 2-level nesting
    assertInstanceOf(StructType.class,
        unArray(getField(observationSchemaL2, true, "identifier", "assigner", "identifier")));
    // but it should not have the assigner field
    assertFieldNotPresent("assigner",
        unArray(getField(observationSchemaL2, true, "identifier", "assigner", "identifier")));
  }


  @Test
  void preferredNameOnly() {

    // Only the primary name that includes the
    // choice type should be included.
    assertTrue(medRequestSchema.getFieldIndex(
        "medicationReference").nonEmpty());

    // Additional names for the field should not be included
    assertTrue(medRequestSchema.getFieldIndex(
        "medicationMedication").isEmpty());
    assertTrue(medRequestSchema.getFieldIndex(
        "medicationResource").isEmpty());
  }

  @Test
  void testDirectlyNestedType() {
    // level 0  - only the backbone element from the resource
    // Questionnaire/item
    assertNotNull(converterL0);
    assertNotNull(Questionnaire.class);
    final StructType questionnaireSchemaL0 = converterL0
        .resourceSchema(Questionnaire.class);

    assertFieldNotPresent("item", unArray(getField(questionnaireSchemaL0, true, "item")));

    // level 1
    // Questionnaire/item/item
    final StructType questionnaireSchemaL1 = converterL1
        .resourceSchema(Questionnaire.class);

    assertEquals(DataTypes.StringType,
        getField(questionnaireSchemaL1, true, "item", "item", "linkId"));
    assertFieldNotPresent("item", unArray(getField(questionnaireSchemaL1, true, "item", "item")));

    // level 2
    // Questionnaire/item/item/item
    final StructType questionnaireSchemaL2 = converterL2
        .resourceSchema(Questionnaire.class);

    assertEquals(DataTypes.StringType,
        getField(questionnaireSchemaL2, true, "item", "item", "item", "linkId"));
    assertFieldNotPresent("item",
        unArray(getField(questionnaireSchemaL2, true, "item", "item", "item")));
  }


  @Test
  void testIndirectlyNestedType() {
    // level 0  - only the backbone element from the resource
    // QuestionnaireResponse/item/answer
    final StructType questionnaireResponseSchemaL0 = converterL0
        .resourceSchema(QuestionnaireResponse.class);
    assertEquals(DataTypes.StringType,
        getField(questionnaireResponseSchemaL0, true, "item", "answer", "id"));
    assertFieldNotPresent("item",
        unArray(getField(questionnaireResponseSchemaL0, true, "item", "answer")));
    // level 1
    // QuestionnaireResponse/item/answer/item/answer
    final StructType questionnaireResponseSchemaL1 = converterL1
        .resourceSchema(QuestionnaireResponse.class);

    assertEquals(DataTypes.StringType,
        getField(questionnaireResponseSchemaL1, true, "item", "answer", "item", "linkId"));
    assertEquals(DataTypes.StringType,
        getField(questionnaireResponseSchemaL1, true, "item", "answer", "item", "answer", "id"));
    assertFieldNotPresent("item", unArray(
        getField(questionnaireResponseSchemaL1, true, "item", "answer", "item", "answer")));

    // level 2
    // QuestionnaireResponse/item/answer/item/answer/item/answer/item/answer
    final StructType questionnaireResponseSchemaL2 = converterL2
        .resourceSchema(QuestionnaireResponse.class);

    assertEquals(DataTypes.StringType,
        getField(questionnaireResponseSchemaL2, true,
            "item", "answer", "item", "answer", "item", "linkId"));
    assertEquals(DataTypes.StringType,
        getField(questionnaireResponseSchemaL2, true,
            "item", "answer", "item", "answer", "item", "answer", "id"));
    assertFieldNotPresent("item", unArray(getField(questionnaireResponseSchemaL2, true,
        "item", "answer", "item", "answer", "item", "answer")));
  }

  @Test
  void testExtensions() {
    final StructType extensionSchema = converterL2
        .resourceSchema(Condition.class);

    // We need to test that:
    // - That there is a global '_extension' of map type field
    // - There is not 'extension' field in any of the structure types
    // - That each struct type has a '_fid' field of INTEGER type

    final MapType extensionsContainerType = (MapType) getField(extensionSchema, true,
        "_extension");
    assertEquals(DataTypes.IntegerType, extensionsContainerType.keyType());
    assertInstanceOf(ArrayType.class, extensionsContainerType.valueType());

    traverseSchema(extensionSchema, t -> {
      assertEquals(DataTypes.IntegerType, t.fields()[t.fieldIndex("_fid")].dataType());
      assertFieldNotPresent("extension", t);
    });
  }

  @Test
  void testRestrictsOpenTypesCorrectly() {

    final Set<String> limitedOpenTypes = Set.of(
        "boolean",
        "integer",
        "Coding",
        "ElementDefinition" // this is not a valid R4 open type, so it should not be returned
    );

    final SchemaConverter schemaConverter = new SchemaConverter(FHIR_CONTEXT, DATA_TYPE_MAPPINGS,
        EncoderConfig.apply(0, CollectionConverters.asScala(limitedOpenTypes).toSet(), true));

    final StructType extensionParent = schemaConverter.resourceSchema(Condition.class);
    final MapType extensionsContainerType = (MapType) getField(extensionParent, true,
        "_extension");
    final StructType extensionStruct = (StructType) ((ArrayType) extensionsContainerType.valueType())
        .elementType();
    final Set<String> actualOpenTypeFieldNames = Stream.of(extensionStruct.fieldNames())
        .filter(fn -> fn.startsWith("value")).collect(Collectors.toUnmodifiableSet());
    assertEquals(Set.of("valueBoolean", "valueInteger", "valueCoding"), actualOpenTypeFieldNames);
  }

  @Test
  void testQuantity() {
    final DataType quantityType = getField(observationSchema, true, "valueQuantity");
    assertQuantityType(quantityType);
  }

  @Test
  void testSimpleQuantity() {
    final DataType quantityType = getField(medRequestSchema, true, "dispenseRequest", "quantity");
    assertQuantityType(quantityType);
  }

  @Test
  void testQuantityArray() {
    final DataType quantityType = getField(deviceSchema, true, "property", "valueQuantity");
    assertQuantityType(quantityType);
  }

  private void assertQuantityType(final DataType quantityType) {
    assertInstanceOf(DecimalType.class, getField(quantityType, true, "value"));
    assertInstanceOf(IntegerType.class, getField(quantityType, true, "value_scale"));
    assertInstanceOf(StringType.class, getField(quantityType, true, "comparator"));
    assertInstanceOf(StringType.class, getField(quantityType, true, "unit"));
    assertInstanceOf(StringType.class, getField(quantityType, true, "system"));
    assertInstanceOf(StringType.class, getField(quantityType, true, "code"));
    assertEquals(FlexiDecimal.DATA_TYPE, getField(quantityType, true, "_value_canonicalized"));
    assertInstanceOf(StringType.class, getField(quantityType, true, "_code_canonicalized"));
  }
}
