/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright © Australian e-Health Research Centre,
 * CSIRO. All rights reserved.
 */

package au.csiro.pathling.bunsen.r4;

import au.csiro.pathling.bunsen.SchemaConverter;
import ca.uhn.fhir.context.FhirContext;
import java.util.Arrays;
import org.apache.spark.sql.types.*;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.MedicationRequest;
import org.hl7.fhir.r4.model.Observation;
import org.junit.Assert;
import org.junit.Test;

public class SchemaConverterTest {

  static SchemaConverter converter = new SchemaConverter(FhirContext.forR4(),
      new R4DataTypeMappings());

  static StructType conditionSchema = converter.resourceSchema(Condition.class);

  static StructType observationSchema = converter.resourceSchema(Observation.class);

  static StructType medRequestSchema = converter.resourceSchema(MedicationRequest.class);

  /**
   * Returns the type of a nested field.
   */
  DataType getField(DataType dataType, boolean isNullable, String... names) {

    StructType schema = dataType instanceof ArrayType
        ? (StructType) ((ArrayType) dataType).elementType()
        : (StructType) dataType;

    StructField field = Arrays.stream(schema.fields())
        .filter(sf -> sf.name().equalsIgnoreCase(names[0]))
        .findFirst()
        .get();

    DataType child = field.dataType();

    // Recurse through children if there are more names.
    if (names.length == 1) {

      // Check the nullability.
      Assert.assertEquals("Unexpected nullability of field " + field.name(),
          isNullable,
          field.nullable());

      return child;
    } else {
      return getField(child, isNullable, Arrays.copyOfRange(names, 1, names.length));
    }
  }

  @Test
  public void resourceHasId() {

    Assert.assertTrue(getField(conditionSchema, true, "id") instanceof StringType);
  }


  @Test
  public void boundCodeToStruct() {
    Assert.assertTrue(getField(conditionSchema, true, "verificationStatus") instanceof StructType);
  }

  @Test
  public void codingToStruct() {

    DataType codingType = getField(conditionSchema, true, "severity", "coding");

    Assert.assertTrue(getField(codingType, true, "system") instanceof StringType);
    Assert.assertTrue(getField(codingType, true, "version") instanceof StringType);
    Assert.assertTrue(getField(codingType, true, "code") instanceof StringType);
    Assert.assertTrue(getField(codingType, true, "display") instanceof StringType);
    Assert.assertTrue(getField(codingType, true, "userSelected") instanceof BooleanType);
  }

  @Test
  public void codeableConceptToStruct() {

    DataType codeableType = getField(conditionSchema, true, "severity");

    Assert.assertTrue(codeableType instanceof StructType);
    Assert.assertTrue(getField(codeableType, true, "coding") instanceof ArrayType);
    Assert.assertTrue(getField(codeableType, true, "text") instanceof StringType);
  }

  @Test
  public void idToString() {
    Assert.assertTrue(getField(conditionSchema, true, "id") instanceof StringType);
  }

  @Test
  public void narrativeToStruct() {

    Assert.assertTrue(getField(conditionSchema, true, "text", "status") instanceof StringType);
    Assert.assertTrue(getField(conditionSchema, true, "text", "div") instanceof StringType);
  }

  @Test
  public void expandChoiceFields() {
    Assert.assertTrue(getField(conditionSchema, true, "onsetPeriod") instanceof StructType);
    Assert.assertTrue(getField(conditionSchema, true, "onsetRange") instanceof StructType);
    Assert.assertTrue(getField(conditionSchema, true, "onsetDateTime") instanceof StringType);
    Assert.assertTrue(getField(conditionSchema, true, "onsetString") instanceof StringType);
    Assert.assertTrue(getField(conditionSchema, true, "onsetAge") instanceof StructType);
  }

  @Test
  public void instantToTimestamp() {
    Assert.assertTrue(getField(observationSchema, true, "issued") instanceof TimestampType);
  }

  @Test
  public void timeToString() {
    Assert.assertTrue((getField(observationSchema, true, "valueTime") instanceof StringType));
  }

  @Test
  public void bigDecimalToDecimal() {
    Assert.assertTrue(
        getField(observationSchema, true, "valueQuantity", "value") instanceof DecimalType);
  }

  @Test
  public void reference() {
    Assert.assertTrue(
        getField(observationSchema, true, "subject", "reference") instanceof StringType);
    Assert
        .assertTrue(getField(observationSchema, true, "subject", "display") instanceof StringType);
  }

  @Test
  public void preferredNameOnly() {

    // Only the primary name that includes the
    // choice type should be included.
    Assert.assertTrue(medRequestSchema.getFieldIndex(
        "medicationReference").nonEmpty());

    // Additional names for the field should not be included
    Assert.assertTrue(medRequestSchema.getFieldIndex(
        "medicationMedication").isEmpty());
    Assert.assertTrue(medRequestSchema.getFieldIndex(
        "medicationResource").isEmpty());
  }
}
