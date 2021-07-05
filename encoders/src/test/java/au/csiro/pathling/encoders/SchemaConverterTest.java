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

import au.csiro.pathling.encoders.datatypes.R4DataTypeMappings;
import ca.uhn.fhir.context.FhirContext;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.types.*;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.MedicationRequest;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Questionnaire;
import org.junit.Test;

public class SchemaConverterTest {

  private static final SchemaConverter converter = new SchemaConverter(FhirContext.forR4(),
      new R4DataTypeMappings());

  private static final StructType conditionSchema = converter.resourceSchema(Condition.class);

  private static final StructType observationSchema = converter.resourceSchema(Observation.class);

  private static final StructType medRequestSchema = converter
      .resourceSchema(MedicationRequest.class);

  private static final StructType questionnaireSchema = converter
      .resourceSchema(Questionnaire.class);

  /**
   * Returns the type of a nested field.
   */
  private DataType getField(final DataType dataType, final boolean isNullable,
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
    final DataType field = getField(questionnaireSchema, true, "item", "answer");
    assertTrue(field instanceof DecimalType);
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
}
