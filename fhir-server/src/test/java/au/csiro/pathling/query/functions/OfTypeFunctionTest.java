/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.functions;

import static au.csiro.pathling.test.Assertions.assertThat;

import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.test.DatasetBuilder;
import au.csiro.pathling.test.PolymorphicExpressionBuilder;
import au.csiro.pathling.test.ResourceExpressionBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * @author John Grimes
 */
@Category(au.csiro.pathling.UnitTest.class)
public class OfTypeFunctionTest {

  @Test
  public void polymorphicReferenceResolution() {
    // Build an expression which represents the input to the function.
    ParsedExpression input = new PolymorphicExpressionBuilder()
        .withColumn("123abcd_id", DataTypes.StringType)
        .withColumn("123abcd_type", DataTypes.StringType)
        .withColumn("123abcd", DataTypes.StringType)
        .withRow("Encounter/xyz1", "Patient", "Patient/abc1")
        .withRow("Encounter/xyz2", "Patient", "Patient/abc2")
        .withRow("Encounter/xyz3", "Patient", "Patient/abc2")
        .withRow("Encounter/xyz4", "Patient", "Patient/abc3")
        .withRow("Encounter/xyz5", "Group", "Group/def1")
        .build();
    input.setSingular(true);

    // Build an expression that looks like one that would be passed as the argument.
    ParsedExpression argument = new ResourceExpressionBuilder(ResourceType.PATIENT,
        FHIRDefinedType.PATIENT)
        .withColumn("789wxyz_id", DataTypes.StringType)
        .withStructColumn("id", DataTypes.StringType)
        .withStructColumn("gender", DataTypes.StringType)
        .withStructColumn("active", DataTypes.BooleanType)
        .withRow("Patient/abc1", RowFactory.create("Patient/abc1", "female", true))
        .withRow("Patient/abc2", RowFactory.create("Patient/abc2", "female", false))
        .withRow("Patient/abc3", RowFactory.create("Patient/abc3", "male", true))
        .buildWithStructValue("789wxyz");
    argument.setSingular(true);

    FunctionInput ofTypeInput = new FunctionInput();
    ofTypeInput.setInput(input);
    ofTypeInput.getArguments().add(argument);
    ofTypeInput.setExpression("ofType(Patient)");

    // Invoke the function.
    OfTypeFunction ofTypeFunction = new OfTypeFunction();
    ParsedExpression result = ofTypeFunction.invoke(ofTypeInput);

    // Check the result.
    assertThat(result).hasFhirPath("ofType(Patient)");
    assertThat(result).isSingular();
    assertThat(result).isResourceOfType(ResourceType.PATIENT, FHIRDefinedType.PATIENT);
    assertThat(result).isSelection();

    // Check the result dataset.
    Dataset<Row> expectedDataset = new DatasetBuilder()
        .withColumn("123abcd_id", DataTypes.StringType)
        .withStructColumn("id", DataTypes.StringType)
        .withStructColumn("gender", DataTypes.StringType)
        .withStructColumn("active", DataTypes.BooleanType)
        .withRow("Encounter/xyz1", RowFactory.create("Patient/abc1", "female", true))
        .withRow("Encounter/xyz2", RowFactory.create("Patient/abc2", "female", false))
        .withRow("Encounter/xyz3", RowFactory.create("Patient/abc2", "female", false))
        .withRow("Encounter/xyz4", RowFactory.create("Patient/abc3", "male", true))
        .withRow("Encounter/xyz5", null)
        .buildWithStructValue("123abcd");
    assertThat(result)
        .selectResult()
        .hasRows(expectedDataset);
  }
}