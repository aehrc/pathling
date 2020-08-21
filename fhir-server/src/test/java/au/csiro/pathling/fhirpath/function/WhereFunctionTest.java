/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.literal.BooleanLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import au.csiro.pathling.test.builders.ResourcePathBuilder;
import java.util.Arrays;
import java.util.Collections;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * @author John Grimes
 */
@Tag("UnitTest")
@Disabled
public class WhereFunctionTest {

  @Test
  public void whereOnResource() {
    final StructType encounterStruct = new StructType(new StructField[]{
        DataTypes.createStructField("id", DataTypes.StringType, true),
        DataTypes.createStructField("status", DataTypes.StringType, true)
    });

    final Dataset<Row> dataset = new DatasetBuilder()
        .withIdColumn()
        .withStructTypeColumns(encounterStruct)
        .withRow("Patient/abc1", RowFactory.create("Encounter/abc1", "in-progress"))
        .withRow("Patient/abc1", RowFactory.create("Encounter/abc2", "finished"))
        .withRow("Patient/abc2", RowFactory.create("Encounter/abc3", "in-progress"))
        .withRow("Patient/abc3", RowFactory.create("Encounter/abc4", "in-progress"))
        .withRow("Patient/abc3", RowFactory.create("Encounter/abc5", "finished"))
        .buildWithStructValue();
    final ResourceReader resourceReader = mock(ResourceReader.class);
    when(resourceReader.read(ResourceType.PATIENT)).thenReturn(dataset);

    final ResourcePath inputExpression = new ResourcePathBuilder()
        .resourceType(ResourceType.ENCOUNTER)
        .singular(false)
        .resourceReader(resourceReader)
        .build();

    // Build an expression which represents the argument to the function. We assume that the value
    // column from the input dataset is also present within the argument dataset.
    final Column status = inputExpression.getValueColumn().getField("status");
    final Column whereResult = status.equalTo("in-progress");
    final ElementPath argumentExpression = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.BOOLEAN)
        .dataset(inputExpression.getDataset().withColumn("value", whereResult))
        .singular(true)
        .build();

    // Prepare the input to the function.
    final ParserContext parserContext = new ParserContextBuilder().build();
    final NamedFunctionInput whereInput = new NamedFunctionInput(parserContext,
        inputExpression,
        Collections.singletonList(argumentExpression));

    // Execute the function.
    final NamedFunction whereFunction = NamedFunction.getInstance("where");
    final FhirPath result = whereFunction.invoke(whereInput);

    // Check the result dataset.
    final Dataset<Row> expectedDataset = new DatasetBuilder()
        .withIdColumn()
        .withStructTypeColumns(encounterStruct)
        .withRow("Patient/abc1", RowFactory.create("Encounter/abc1", "in-progress"))
        .withRow("Patient/abc1", null)
        .withRow("Patient/abc2", RowFactory.create("Encounter/abc3", "in-progress"))
        .withRow("Patient/abc3", RowFactory.create("Encounter/abc4", "in-progress"))
        .withRow("Patient/abc3", null)
        .buildWithStructValue();
    assertThat(result)
        .selectResult()
        .hasRows(expectedDataset);
  }

  @Test
  public void whereOnElement() {
    // Build an expression which represents the input to the function.
    final Dataset<Row> dataset = new DatasetBuilder()
        .withIdColumn()
        .withValueColumn(DataTypes.StringType)
        .withRow("Patient/abc1", "en")
        .withRow("Patient/abc1", "es")
        .withRow("Patient/abc2", "de")
        .withRow("Patient/abc3", "en")
        .withRow("Patient/abc3", "en")
        .withRow("Patient/abc3", "zh")
        .build();
    final ElementPath inputExpression = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.STRING)
        .dataset(dataset)
        .singular(false)
        .build();

    // Build an expression which represents the argument to the function.
    final Column inputCol = inputExpression.getValueColumn();
    final Column whereResult = inputCol.equalTo("en");
    final ElementPath argumentExpression = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.BOOLEAN)
        .dataset(inputExpression.getDataset().withColumn("value", whereResult))
        .singular(true)
        .valueColumn(whereResult)
        .build();

    // Prepare the input to the function.
    final ParserContext parserContext = new ParserContextBuilder().build();
    final NamedFunctionInput whereInput = new NamedFunctionInput(parserContext,
        inputExpression,
        Collections.singletonList(argumentExpression));

    // Execute the function.
    final NamedFunction whereFunction = NamedFunction.getInstance("where");
    final FhirPath result = whereFunction.invoke(whereInput);

    // Check the result dataset.
    final Dataset<Row> expectedDataset = new DatasetBuilder()
        .withIdColumn()
        .withValueColumn(DataTypes.StringType)
        .withRow("Patient/abc1", "en")
        .withRow("Patient/abc1", null)
        .withRow("Patient/abc2", null)
        .withRow("Patient/abc3", "en")
        .withRow("Patient/abc3", "en")
        .withRow("Patient/abc3", null)
        .build();
    assertThat(result)
        .selectResult()
        .hasRows(expectedDataset);
  }

  @Test
  public void withLiteralArgument() {
    // Build an expression which represents the input to the function.
    final Dataset<Row> dataset = new DatasetBuilder()
        .withIdColumn()
        .withValueColumn(DataTypes.StringType)
        .withRow("Patient/abc1", "en")
        .withRow("Patient/abc1", "es")
        .withRow("Patient/abc2", "de")
        .withRow("Patient/abc3", "en")
        .withRow("Patient/abc3", "en")
        .withRow("Patient/abc3", "zh")
        .build();
    final ElementPath inputExpression = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.STRING)
        .dataset(dataset)
        .singular(false)
        .build();

    // Build an expression which represents the argument to the function.
    final BooleanLiteralPath argumentExpression = BooleanLiteralPath
        .fromString("true", inputExpression);

    // Prepare the input to the function.
    final ParserContext parserContext = new ParserContextBuilder().build();
    final NamedFunctionInput whereInput = new NamedFunctionInput(parserContext,
        inputExpression,
        Collections.singletonList(argumentExpression));

    // Execute the function.
    final NamedFunction whereFunction = NamedFunction.getInstance("where");
    final FhirPath result = whereFunction.invoke(whereInput);

    // Check the result dataset.
    final Dataset<Row> expectedDataset = new DatasetBuilder()
        .withIdColumn()
        .withValueColumn(DataTypes.StringType)
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
  public void nullValuesAreNull() {
    // Build an expression which represents the input to the function.
    final Dataset<Row> dataset = new DatasetBuilder()
        .withIdColumn()
        .withValueColumn(DataTypes.StringType)
        .withRow("Patient/abc1", "en")
        .withRow("Patient/abc1", "es")
        .withRow("Patient/abc2", "de")
        .withRow("Patient/abc3", "en")
        .withRow("Patient/abc3", "en")
        .withRow("Patient/abc3", "zh")
        .build();
    final ElementPath inputExpression = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.STRING)
        .dataset(dataset)
        .singular(false)
        .build();

    // Build an expression which represents the argument to the function.
    final Column inputCol = inputExpression.getValueColumn();
    final Column whereResult = functions.when(inputCol.equalTo("en"), null).otherwise(true);
    final ElementPath argumentExpression = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.BOOLEAN)
        .dataset(inputExpression.getDataset().withColumn("value", whereResult))
        .singular(true)
        .valueColumn(whereResult)
        .build();

    // Prepare the input to the function.
    final ParserContext parserContext = new ParserContextBuilder().build();
    final NamedFunctionInput whereInput = new NamedFunctionInput(parserContext,
        inputExpression,
        Collections.singletonList(argumentExpression));

    // Execute the function.
    final NamedFunction whereFunction = NamedFunction.getInstance("where");
    final FhirPath result = whereFunction.invoke(whereInput);

    // Check the result dataset.
    final Dataset<Row> expectedDataset = new DatasetBuilder()
        .withIdColumn()
        .withValueColumn(DataTypes.StringType)
        .withRow("Patient/abc1", null)
        .withRow("Patient/abc1", "es")
        .withRow("Patient/abc2", "de")
        .withRow("Patient/abc3", null)
        .withRow("Patient/abc3", null)
        .withRow("Patient/abc3", "zh")
        .build();
    assertThat(result)
        .selectResult()
        .hasRows(expectedDataset);
  }

  @Test
  public void throwsErrorIfMoreThanOneArgument() {
    final ResourcePath input = new ResourcePathBuilder().build();
    final ElementPath argument1 = new ElementPathBuilder()
        .expression("$this.gender = 'female'")
        .fhirType(FHIRDefinedType.BOOLEAN)
        .build();
    final ElementPath argument2 = new ElementPathBuilder()
        .expression("$this.gender != 'male'")
        .fhirType(FHIRDefinedType.BOOLEAN)
        .build();

    final ParserContext parserContext = new ParserContextBuilder().build();
    final NamedFunctionInput whereInput = new NamedFunctionInput(parserContext, input,
        Arrays.asList(argument1, argument2));

    final NamedFunction whereFunction = NamedFunction.getInstance("where");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> whereFunction.invoke(whereInput));
    assertEquals(
        "where function accepts one argument: where($this.gender = 'female', $this.gender != 'male')",
        error.getMessage());
  }

  @Test
  public void throwsErrorIfArgumentNotBoolean() {
    final ResourcePath input = new ResourcePathBuilder().build();
    final ElementPath argument = new ElementPathBuilder()
        .expression("$this.gender")
        .fhirType(FHIRDefinedType.STRING)
        .build();

    final ParserContext parserContext = new ParserContextBuilder().build();
    final NamedFunctionInput whereInput = new NamedFunctionInput(parserContext, input,
        Collections.singletonList(argument));

    final NamedFunction whereFunction = NamedFunction.getInstance("where");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> whereFunction.invoke(whereInput));
    assertEquals(
        "Argument to where function must be a singular Boolean: $this.gender",
        error.getMessage());
  }

  @Test
  public void throwsErrorIfArgumentNotSingular() {
    final ResourcePath input = new ResourcePathBuilder().build();
    final ElementPath argument = new ElementPathBuilder()
        .expression("$this.communication.preferred")
        .fhirType(FHIRDefinedType.BOOLEAN)
        .singular(false)
        .build();

    final ParserContext parserContext = new ParserContextBuilder().build();
    final NamedFunctionInput whereInput = new NamedFunctionInput(parserContext, input,
        Collections.singletonList(argument));

    final NamedFunction whereFunction = NamedFunction.getInstance("where");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> whereFunction.invoke(whereInput));
    assertEquals(
        "Argument to where function must be a singular Boolean: $this.communication.preferred",
        error.getMessage());
  }

}