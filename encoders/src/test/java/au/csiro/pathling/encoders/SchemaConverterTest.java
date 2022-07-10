/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright © 2018-2022, Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230. Licensed
 * under the CSIRO Open Source Software Licence Agreement.
 *
 */

package au.csiro.pathling.encoders;

import static au.csiro.pathling.test.SchemaAsserts.assertFieldNotPresent;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.encoders.datatypes.DataTypeMappings;
import au.csiro.pathling.encoders.datatypes.R4DataTypeMappings;
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
import scala.collection.JavaConverters;

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

  private SchemaConverter converter_L0;
  private SchemaConverter converter_L1;
  private SchemaConverter converter_L2;

  private StructType conditionSchema;
  private StructType observationSchema;
  private StructType medRequestSchema;
  private StructType questionnaireSchema;
  private StructType questionnaireResponseSchema;

  private StructType deviceSchema;


  /**
   * Traverses a DataType recursively passing all encountered StructTypes to the provided consumer.
   *
   * @param type the DataType to traverse.
   * @param consumer the consumer that receives all StructTypes.
   */
  private void traverseSchema(final DataType type, final Consumer<StructType> consumer) {
    if (type instanceof StructType) {
      final StructType structType = (StructType) type;
      consumer.accept(structType);
      Arrays.stream(structType.fields()).forEach(f -> traverseSchema(f.dataType(), consumer));
    } else if (type instanceof ArrayType) {
      traverseSchema(((ArrayType) type).elementType(), consumer);
    } else if (type instanceof MapType) {
      traverseSchema(((MapType) type).keyType(), consumer);
      traverseSchema(((MapType) type).valueType(), consumer);
    }
  }

  private SchemaConverter createSchemaConverter(final int maxNestingLevel) {
    return new SchemaConverter(FHIR_CONTEXT, DATA_TYPE_MAPPINGS,
        EncoderConfig.apply(maxNestingLevel, JavaConverters.asScalaSet(OPEN_TYPES).toSet(), true));
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
  public void setUp() {
    converter_L0 = createSchemaConverter(0);
    converter_L1 = createSchemaConverter(1);
    converter_L2 = createSchemaConverter(2);

    conditionSchema = converter_L0.resourceSchema(Condition.class);
    observationSchema = converter_L0.resourceSchema(Observation.class);
    medRequestSchema = converter_L0.resourceSchema(MedicationRequest.class);
    questionnaireSchema = converter_L0.resourceSchema(Questionnaire.class);
    questionnaireResponseSchema = converter_L0.resourceSchema(QuestionnaireResponse.class);
    deviceSchema = converter_L0.resourceSchema(Device.class);
  }

  @Test
  public void resourceHasId() {
    assertTrue(getField(conditionSchema, true, "id") instanceof StringType);
  }

  @Test
  public void boundCodeToStruct() {
    assertTrue(getField(conditionSchema, true, "verificationStatus") instanceof StructType);
  }

  @Test
  public void codingToStruct() {

    final DataType codingType = getField(conditionSchema, true, "severity", "coding");

    assertTrue(getField(codingType, true, "system") instanceof StringType);
    assertTrue(getField(codingType, true, "version") instanceof StringType);
    assertTrue(getField(codingType, true, "code") instanceof StringType);
    assertTrue(getField(codingType, true, "display") instanceof StringType);
    assertTrue(getField(codingType, true, "userSelected") instanceof BooleanType);
  }

  @Test
  public void codeableConceptToStruct() {

    final DataType codeableType = getField(conditionSchema, true, "severity");

    assertTrue(codeableType instanceof StructType);
    assertTrue(getField(codeableType, true, "coding") instanceof ArrayType);
    assertTrue(getField(codeableType, true, "text") instanceof StringType);
  }

  @Test
  public void idToString() {
    assertTrue(getField(conditionSchema, true, "id") instanceof StringType);
  }

  @Test
  public void narrativeToStruct() {

    assertTrue(getField(conditionSchema, true, "text", "status") instanceof StringType);
    assertTrue(getField(conditionSchema, true, "text", "div") instanceof StringType);
  }

  @Test
  public void expandChoiceFields() {
    assertTrue(getField(conditionSchema, true, "onsetPeriod") instanceof StructType);
    assertTrue(getField(conditionSchema, true, "onsetRange") instanceof StructType);
    assertTrue(getField(conditionSchema, true, "onsetDateTime") instanceof StringType);
    assertTrue(getField(conditionSchema, true, "onsetString") instanceof StringType);
    assertTrue(getField(conditionSchema, true, "onsetAge") instanceof StructType);
  }

  @Test
  public void orderChoiceFields() {
    final List<String> expectedFields = Arrays
        .asList("valueBoolean", "valueCodeableConcept", "valueDateTime",
            "valueInteger", "valuePeriod", "valueQuantity", "valueRange",
            "valueRatio", "valueSampledData", "valueString", "valueTime");

    final List<String> actualFields = Stream.of(observationSchema.fieldNames())
        .filter(fn -> fn.startsWith("value"))
        .collect(Collectors.toList());

    assertEquals(expectedFields, actualFields);
  }

  @Test
  public void decimalWithinChoiceField() {
    assertTrue(getField(questionnaireSchema, true, "item", "enableWhen",
        "answerDecimal") instanceof DecimalType);
    assertTrue(getField(questionnaireSchema, true, "item", "enableWhen",
        "answerDecimal_scale") instanceof IntegerType);
    assertTrue(getField(questionnaireResponseSchema, true, "item", "answer",
        "valueDecimal") instanceof DecimalType);
    assertTrue(getField(questionnaireResponseSchema, true, "item", "answer",
        "valueDecimal_scale") instanceof IntegerType);
  }

  @Test
  public void instantToTimestamp() {
    assertTrue(getField(observationSchema, true, "issued") instanceof TimestampType);
  }

  @Test
  public void timeToString() {
    assertTrue((getField(observationSchema, true, "valueTime") instanceof StringType));
  }

  @Test
  public void bigDecimalToDecimal() {
    assertTrue(
        getField(observationSchema, true, "valueQuantity", "value") instanceof DecimalType);
  }

  @Test
  public void reference() {
    assertTrue(
        getField(observationSchema, true, "subject", "reference") instanceof StringType);
    assertTrue(getField(observationSchema, true, "subject", "display") instanceof StringType);
  }

  @Test
  public void preferredNameOnly() {

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
  public void testDirectlyNestedType() {
    // level 0  - only the backbone element from the resource
    // Questionnaire/item
    assertNotNull(converter_L0);
    assertNotNull(Questionnaire.class);
    final StructType questionnaireSchema_L0 = converter_L0
        .resourceSchema(Questionnaire.class);

    assertFieldNotPresent("item", unArray(getField(questionnaireSchema_L0, true, "item")));

    // level 1
    // Questionnaire/item/item
    final StructType questionnaireSchema_L1 = converter_L1
        .resourceSchema(Questionnaire.class);

    assertEquals(DataTypes.StringType,
        getField(questionnaireSchema_L1, true, "item", "item", "linkId"));
    assertFieldNotPresent("item", unArray(getField(questionnaireSchema_L1, true, "item", "item")));

    // level 2
    // Questionnaire/item/item/item
    final StructType questionnaireSchema_L2 = converter_L2
        .resourceSchema(Questionnaire.class);

    assertEquals(DataTypes.StringType,
        getField(questionnaireSchema_L2, true, "item", "item", "item", "linkId"));
    assertFieldNotPresent("item",
        unArray(getField(questionnaireSchema_L2, true, "item", "item", "item")));
  }


  @Test
  public void testIndirectlyNestedType() {
    // level 0  - only the backbone element from the resource
    // QuestionnaireResponse/item/answer
    final StructType questionnaireResponseSchema_L0 = converter_L0
        .resourceSchema(QuestionnaireResponse.class);
    assertEquals(DataTypes.StringType,
        getField(questionnaireResponseSchema_L0, true, "item", "answer", "id"));
    assertFieldNotPresent("item",
        unArray(getField(questionnaireResponseSchema_L0, true, "item", "answer")));
    // level 1
    // QuestionnaireResponse/item/answer/item/answer
    final StructType questionnaireResponseSchema_L1 = converter_L1
        .resourceSchema(QuestionnaireResponse.class);

    assertEquals(DataTypes.StringType,
        getField(questionnaireResponseSchema_L1, true, "item", "answer", "item", "linkId"));
    assertEquals(DataTypes.StringType,
        getField(questionnaireResponseSchema_L1, true, "item", "answer", "item", "answer", "id"));
    assertFieldNotPresent("item", unArray(
        getField(questionnaireResponseSchema_L1, true, "item", "answer", "item", "answer")));

    // level 2
    // QuestionnaireResponse/item/answer/item/answer/item/answer/item/answer
    final StructType questionnaireResponseSchema_L2 = converter_L2
        .resourceSchema(QuestionnaireResponse.class);

    assertEquals(DataTypes.StringType,
        getField(questionnaireResponseSchema_L2, true,
            "item", "answer", "item", "answer", "item", "linkId"));
    assertEquals(DataTypes.StringType,
        getField(questionnaireResponseSchema_L2, true,
            "item", "answer", "item", "answer", "item", "answer", "id"));
    assertFieldNotPresent("item", unArray(getField(questionnaireResponseSchema_L2, true,
        "item", "answer", "item", "answer", "item", "answer")));
  }

  @Test
  public void testExtensions() {
    final StructType extensionSchema = converter_L2
        .resourceSchema(Condition.class);

    // We need to test that:
    // - That there is a global '_extension' of map type field
    // - There is not 'extension' field in any of the structure types
    // - That each struct type has a '_fid' field of INTEGER type

    final MapType extensionsContainerType = (MapType) getField(extensionSchema, true,
        "_extension");
    assertEquals(DataTypes.IntegerType, extensionsContainerType.keyType());
    assertTrue(extensionsContainerType.valueType() instanceof ArrayType);

    traverseSchema(extensionSchema, t -> {
      assertEquals(DataTypes.IntegerType, t.fields()[t.fieldIndex("_fid")].dataType());
      assertFieldNotPresent("extension", t);
    });
  }

  @Test
  public void testRestrictsOpenTypesCorrectly() {

    final Set<String> limitedOpenTypes = Set.of(
        "boolean",
        "integer",
        "Coding",
        "ElementDefinition" // this is not a valid R4 open type, so it should not be returned
    );

    final SchemaConverter schemaConverter = new SchemaConverter(FHIR_CONTEXT, DATA_TYPE_MAPPINGS,
        EncoderConfig.apply(0, JavaConverters.asScalaSet(limitedOpenTypes).toSet(), true));

    final StructType conditionSchema = schemaConverter.resourceSchema(Condition.class);
    final MapType extensionsContainerType = (MapType) getField(conditionSchema, true,
        "_extension");
    final StructType extensionStruct = (StructType) ((ArrayType) extensionsContainerType.valueType())
        .elementType();
    final Set<String> actualOpenTypeFieldNames = Stream.of(extensionStruct.fieldNames())
        .filter(fn -> fn.startsWith("value")).collect(Collectors.toUnmodifiableSet());
    assertEquals(Set.of("valueBoolean", "valueInteger", "valueCoding"), actualOpenTypeFieldNames);
  }

  @Test
  public void testQuantity() {
    final DataType quantityType = getField(observationSchema, true, "valueQuantity");
    assertQuantityType(quantityType);
  }

  @Test
  public void testSimpleQuantity() {
    final DataType quantityType = getField(medRequestSchema, true, "dispenseRequest", "quantity");
    assertQuantityType(quantityType);
  }

  @Test
  public void testQuantityArray() {
    final DataType quantityType = getField(deviceSchema, true, "property", "valueQuantity");
    assertQuantityType(quantityType);
  }

  private void assertQuantityType(final DataType quantityType) {
    assertTrue(getField(quantityType, true, "value") instanceof DecimalType);
    assertTrue(getField(quantityType, true, "value_scale") instanceof IntegerType);
    assertTrue(getField(quantityType, true, "comparator") instanceof StringType);
    assertTrue(getField(quantityType, true, "unit") instanceof StringType);
    assertTrue(getField(quantityType, true, "system") instanceof StringType);
    assertTrue(getField(quantityType, true, "code") instanceof StringType);
    assertTrue(getField(quantityType, true, "_value_canonicalized") instanceof DecimalType);
    assertTrue(getField(quantityType, true, "_code_canonicalized") instanceof StringType);
  }
}
