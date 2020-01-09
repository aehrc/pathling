/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.functions;

import static au.csiro.pathling.test.Assertions.assertThat;
import static org.apache.spark.sql.functions.when;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import au.csiro.pathling.query.parsing.ParsedExpression;
import au.csiro.pathling.query.parsing.ParsedExpression.FhirPathType;
import au.csiro.pathling.test.DatasetBuilder;
import au.csiro.pathling.test.PrimitiveExpressionBuilder;
import au.csiro.pathling.test.ResourceExpressionBuilder;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * @author John Grimes
 */
@Category(au.csiro.pathling.UnitTest.class)
public class WhereFunctionTest {

  @Test
  public void whereOnResource() {
    StructType encounterStruct = new StructType(new StructField[]{
        DataTypes.createStructField("id", DataTypes.StringType, true),
        DataTypes.createStructField("status", DataTypes.StringType, true)
    });

    // Build an expression which represents the input to the function.
    ParsedExpression inputExpression = new ResourceExpressionBuilder(ResourceType.ENCOUNTER,
        FHIRDefinedType.ENCOUNTER)
        .withColumn("123abcd_id", DataTypes.StringType)
        .withStructTypeColumns(encounterStruct)
        .withRow("Patient/abc1", RowFactory.create("Encounter/abc1", "in-progress"))
        .withRow("Patient/abc1", RowFactory.create("Encounter/abc2", "finished"))
        .withRow("Patient/abc2", RowFactory.create("Encounter/abc3", "in-progress"))
        .withRow("Patient/abc3", RowFactory.create("Encounter/abc4", "in-progress"))
        .withRow("Patient/abc3", RowFactory.create("Encounter/abc5", "finished"))
        .buildWithStructValue("123abcd");
    inputExpression.setSingular(false);

    // Build an expression which represents the argument to the function. We assume that the value
    // column from the input dataset is also present within the argument dataset.
    Column status = inputExpression.getValueColumn().getField("status");
    Column whereResult = status.equalTo("in-progress");
    ParsedExpression argumentExpression = new PrimitiveExpressionBuilder(FHIRDefinedType.BOOLEAN,
        FhirPathType.BOOLEAN)
        .withDataset(inputExpression.getDataset().withColumn("whereResult", whereResult))
        .build();
    argumentExpression.setSingular(true);
    argumentExpression.setValueColumn(whereResult);

    // Prepare the input to the function.
    FunctionInput whereInput = new FunctionInput();
    whereInput.setInput(inputExpression);
    whereInput.getArguments().add(argumentExpression);
    whereInput.setExpression("where($this.status = 'in-progress')");

    // Execute the function.
    WhereFunction whereFunction = new WhereFunction();
    ParsedExpression result = whereFunction.invoke(whereInput);

    // Check the result dataset.
    Dataset<Row> expectedDataset = new DatasetBuilder()
        .withColumn("456mnop_id", DataTypes.StringType)
        .withStructColumn("id", DataTypes.StringType)
        .withStructColumn("status", DataTypes.StringType)
        .withRow("Patient/abc1", RowFactory.create("Encounter/abc1", "in-progress"))
        .withRow("Patient/abc2", RowFactory.create("Encounter/abc3", "in-progress"))
        .withRow("Patient/abc3", RowFactory.create("Encounter/abc4", "in-progress"))
        .buildWithStructValue("456mnop");
    assertThat(result)
        .selectResult()
        .hasRows(expectedDataset);
  }

  @Test
  public void whereOnElement() {
    // Build an expression which represents the input to the function.
    ParsedExpression inputExpression = new PrimitiveExpressionBuilder(FHIRDefinedType.STRING,
        FhirPathType.STRING)
        .withColumn("123abcd_id", DataTypes.StringType)
        .withColumn("123abcd", DataTypes.StringType)
        .withRow("Patient/abc1", "en")
        .withRow("Patient/abc1", "es")
        .withRow("Patient/abc2", "de")
        .withRow("Patient/abc3", "en")
        .withRow("Patient/abc3", "en")
        .withRow("Patient/abc3", "zh")
        .build();
    inputExpression.setSingular(false);

    // Build an expression which represents the argument to the function.
    Column inputCol = inputExpression.getValueColumn();
    Column whereResult = inputCol.equalTo("en");
    ParsedExpression argumentExpression = new PrimitiveExpressionBuilder(FHIRDefinedType.BOOLEAN,
        FhirPathType.BOOLEAN)
        .withDataset(inputExpression.getDataset().withColumn("whereResult", whereResult))
        .build();
    argumentExpression.setSingular(true);
    argumentExpression.setValueColumn(whereResult);

    // Prepare the input to the function.
    FunctionInput whereInput = new FunctionInput();
    whereInput.setInput(inputExpression);
    whereInput.getArguments().add(argumentExpression);
    whereInput.setExpression("where($this = 'en')");

    // Execute the function.
    WhereFunction whereFunction = new WhereFunction();
    ParsedExpression result = whereFunction.invoke(whereInput);

    // Check the result dataset.
    Dataset<Row> expectedDataset = new DatasetBuilder()
        .withColumn("456mnop_id", DataTypes.StringType)
        .withColumn("456mnop", DataTypes.StringType)
        .withRow("Patient/abc1", "en")
        .withRow("Patient/abc3", "en")
        .withRow("Patient/abc3", "en")
        .build();
    assertThat(result)
        .selectResult()
        .hasRows(expectedDataset);
  }

  @Test
  public void withLiteralArgument() {
    // Build an expression which represents the input to the function.
    ParsedExpression inputExpression = new PrimitiveExpressionBuilder(FHIRDefinedType.STRING,
        FhirPathType.STRING)
        .withColumn("123abcd_id", DataTypes.StringType)
        .withColumn("123abcd", DataTypes.StringType)
        .withRow("Patient/abc1", "en")
        .withRow("Patient/abc1", "es")
        .withRow("Patient/abc2", "de")
        .withRow("Patient/abc3", "en")
        .withRow("Patient/abc3", "en")
        .withRow("Patient/abc3", "zh")
        .build();
    inputExpression.setSingular(false);

    // Build an expression which represents the argument to the function.
    ParsedExpression argumentExpression = PrimitiveExpressionBuilder.literalBoolean(true);

    // Prepare the input to the function.
    FunctionInput whereInput = new FunctionInput();
    whereInput.setInput(inputExpression);
    whereInput.getArguments().add(argumentExpression);
    whereInput.setExpression("where(true)");

    // Execute the function.
    WhereFunction whereFunction = new WhereFunction();
    ParsedExpression result = whereFunction.invoke(whereInput);

    // Check the result dataset.
    Dataset<Row> expectedDataset = new DatasetBuilder()
        .withColumn("456mnop_id", DataTypes.StringType)
        .withColumn("456mnop", DataTypes.StringType)
        .withRow("Patient/abc1", "en")
        .withRow("Patient/abc1", "es")
        .withRow("Patient/abc2", "de")
        .withRow("Patient/abc3", "en")
        .withRow("Patient/abc3", "en")
        .withRow("Patient/abc3", "zh")
        .build();
    assertThat(result)
        .selectResult()
        .hasRows(expectedDataset);
  }

  @Test
  public void nullValuesAreExcluded() {
    // Build an expression which represents the input to the function.
    ParsedExpression inputExpression = new PrimitiveExpressionBuilder(FHIRDefinedType.STRING,
        FhirPathType.STRING)
        .withColumn("123abcd_id", DataTypes.StringType)
        .withColumn("123abcd", DataTypes.StringType)
        .withRow("Patient/abc1", "en")
        .withRow("Patient/abc1", "es")
        .withRow("Patient/abc2", "de")
        .withRow("Patient/abc3", "en")
        .withRow("Patient/abc3", "en")
        .withRow("Patient/abc3", "zh")
        .build();
    inputExpression.setSingular(false);

    // Build an expression which represents the argument to the function.
    Column inputCol = inputExpression.getValueColumn();
    Column whereResult = when(inputCol.equalTo("en"), null).otherwise(true);
    ParsedExpression argumentExpression = new PrimitiveExpressionBuilder(FHIRDefinedType.BOOLEAN,
        FhirPathType.BOOLEAN)
        .withDataset(inputExpression.getDataset().withColumn("whereResult", whereResult))
        .build();
    argumentExpression.setSingular(true);
    argumentExpression.setValueColumn(whereResult);

    // Prepare the input to the function.
    FunctionInput whereInput = new FunctionInput();
    whereInput.setInput(inputExpression);
    whereInput.getArguments().add(argumentExpression);
    whereInput.setExpression("where($this = 'en')");

    // Execute the function.
    WhereFunction whereFunction = new WhereFunction();
    ParsedExpression result = whereFunction.invoke(whereInput);

    // Check the result dataset.
    Dataset<Row> expectedDataset = new DatasetBuilder()
        .withColumn("456mnop_id", DataTypes.StringType)
        .withColumn("456mnop", DataTypes.StringType)
        .withRow("Patient/abc1", "es")
        .withRow("Patient/abc2", "de")
        .withRow("Patient/abc3", "zh")
        .build();
    assertThat(result)
        .selectResult()
        .hasRows(expectedDataset);
  }

  @Test
  public void throwsErrorIfMoreThanOneArgument() {
    ParsedExpression input = new ResourceExpressionBuilder(ResourceType.PATIENT,
        FHIRDefinedType.PATIENT)
        .build();
    ParsedExpression argument1 = new PrimitiveExpressionBuilder(FHIRDefinedType.BOOLEAN,
        FhirPathType.BOOLEAN).build(),
        argument2 = new PrimitiveExpressionBuilder(FHIRDefinedType.BOOLEAN,
            FhirPathType.BOOLEAN).build();

    FunctionInput whereInput = new FunctionInput();
    whereInput.setInput(input);
    whereInput.getArguments().add(argument1);
    whereInput.getArguments().add(argument2);
    whereInput.setExpression("where($this.gender = 'female', $this.gender != 'male')");

    WhereFunction whereFunction = new WhereFunction();
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> whereFunction.invoke(whereInput))
        .withMessage(
            "where function accepts one argument: where($this.gender = 'female', $this.gender != 'male')");
  }

  @Test
  public void throwsErrorIfArgumentNotBoolean() {
    ParsedExpression input = new ResourceExpressionBuilder(ResourceType.PATIENT,
        FHIRDefinedType.PATIENT)
        .build();
    ParsedExpression argument = new PrimitiveExpressionBuilder(FHIRDefinedType.CODE,
        FhirPathType.STRING).build();
    argument.setFhirPath("$this.gender");
    argument.setSingular(true);

    FunctionInput whereInput = new FunctionInput();
    whereInput.setInput(input);
    whereInput.getArguments().add(argument);
    whereInput.setExpression("where($this.gender)");

    WhereFunction whereFunction = new WhereFunction();
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> whereFunction.invoke(whereInput))
        .withMessage(
            "Argument to where function must be a singular Boolean: $this.gender");
  }

  @Test
  public void throwsErrorIfArgumentNotSingular() {
    ParsedExpression input = new ResourceExpressionBuilder(ResourceType.PATIENT,
        FHIRDefinedType.PATIENT).build();
    ParsedExpression argument = new PrimitiveExpressionBuilder(FHIRDefinedType.BOOLEAN,
        FhirPathType.BOOLEAN).build();
    argument.setFhirPath("$this.communication.preferred");

    FunctionInput whereInput = new FunctionInput();
    whereInput.setInput(input);
    whereInput.getArguments().add(argument);
    whereInput.setExpression("where($this.communication.preferred)");

    WhereFunction whereFunction = new WhereFunction();
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> whereFunction.invoke(whereInput))
        .withMessage(
            "Argument to where function must be a singular Boolean: $this.communication.preferred");
  }

}