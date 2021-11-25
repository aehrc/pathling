/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright © 2018-2021, Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230. Licensed
 * under the CSIRO Open Source Software Licence Agreement.
 */

package au.csiro.pathling.encoders;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import au.csiro.pathling.encoders.datatypes.DataTypeMappings;
import au.csiro.pathling.encoders.datatypes.R4DataTypeMappings;
import ca.uhn.fhir.context.FhirContext;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.*;
import org.hl7.fhir.r4.model.*;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public abstract class AbstractSchemaConverterTest {


  protected static final FhirContext FHIR_CONTEXT = FhirContext.forR4();
  protected static final DataTypeMappings DATA_TYPE_MAPPINGS = new R4DataTypeMappings();

  private SchemaConverter converter_L0;
  private SchemaConverter converter_L1;
  private SchemaConverter converter_L2;
  private StructType conditionSchema;
  private StructType observationSchema;
  private StructType medRequestSchema;
  private StructType questionnaireSchema;
  private StructType questionnaireResponseSchema;

  protected abstract SchemaConverter createSchemaConverter(int maxNestingLevel);

  @Before
  public void setUp() {
    converter_L0 = createSchemaConverter(0);
    converter_L1 = createSchemaConverter(1);
    converter_L2 = createSchemaConverter(2);

    conditionSchema = converter_L0.resourceSchema(Condition.class);
    observationSchema = converter_L0.resourceSchema(Observation.class);
    medRequestSchema = converter_L0.resourceSchema(MedicationRequest.class);
    questionnaireSchema = converter_L0.resourceSchema(Questionnaire.class);
    questionnaireResponseSchema = converter_L0.resourceSchema(QuestionnaireResponse.class);
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
      assertEquals("Unexpected nullability of field " + field.name(),
          isNullable,
          field.nullable());

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

  private static void assertFieldNotPresent(final String fieldName,
      final DataType maybeStructType) {
    assertTrue("Must be struct type.", maybeStructType instanceof StructType);
    assertTrue("Field: '" + fieldName + "' not present in struct type.",
        ((StructType) maybeStructType).getFieldIndex(
            fieldName).isEmpty());
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

  // TODO: Complete when "#375 - Collections are not supported for custom encoders is fixed"
  @Test
  @Ignore
  public void testNestedTypeInChoice() {

    //  Parameters
    //  ElementDefinition: parameter.valueElementDefinition-> parameter.valueElementDefinition.fixedElementDefinition
    //  ElementDefinition: parameter.valueElementDefinition-> parameter.valueElementDefinition.example.valueElementDefinition (indirect)
  }
}
