/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.utilities.Preconditions.check;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourceDefinition;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.literal.BooleanLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import au.csiro.pathling.test.builders.ResourcePathBuilder;
import au.csiro.pathling.test.helpers.FhirHelpers;
import au.csiro.pathling.test.helpers.SparkHelpers;
import au.csiro.pathling.test.helpers.SparkHelpers.IdAndValueColumns;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Encounter;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * @author John Grimes
 */
@Tag("UnitTest")
public class WhereFunctionTest {

  // This test simulates the execution of the where function on the path
  // `Patient.reverseResolve(Encounter.subject).where($this.status = 'in-progress')`.
  @Test
  public void whereOnResource() {
    final StructType encounterStruct = new StructType(new StructField[]{
        DataTypes.createStructField("id", DataTypes.StringType, true),
        DataTypes.createStructField("status", DataTypes.StringType, true)
    });

    final Dataset<Row> inputDataset = new DatasetBuilder()
        .withIdColumn()
        .withStructTypeColumns(encounterStruct)
        .withRow("Patient/abc1", RowFactory.create("Encounter/abc1", "in-progress"))
        .withRow("Patient/abc1", RowFactory.create("Encounter/abc2", "finished"))
        .withRow("Patient/abc2", RowFactory.create("Encounter/abc3", "in-progress"))
        .withRow("Patient/abc3", RowFactory.create("Encounter/abc4", "in-progress"))
        .withRow("Patient/abc3", RowFactory.create("Encounter/abc5", "finished"))
        .buildWithStructValue();
    final IdAndValueColumns inputIdAndValue = SparkHelpers.getIdAndValueColumns(inputDataset);
    final ResourceDefinition resourceDefinition = new ResourceDefinition(ResourceType.ENCOUNTER,
        FhirHelpers.getFhirContext().getResourceDefinition(Encounter.class));

    final ResourcePath inputPath = new ResourcePath("reverseResolve(Encounter.subject)",
        inputDataset, Optional.of(inputIdAndValue.getId()), inputIdAndValue.getValue(), false,
        resourceDefinition);

    // Build an expression which represents the argument to the function. We assume that the value
    // column from the input dataset is also present within the argument dataset.
    final Dataset<Row> argumentDataset = inputPath.getDataset()
        .withColumn("value", inputPath.getValueColumn().getField("status").equalTo("in-progress"));
    check(inputPath.getIdColumn().isPresent());
    final ElementPath argumentPath = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.BOOLEAN)
        .dataset(argumentDataset)
        .idColumn(inputPath.getIdColumn().get())
        .valueColumn(argumentDataset.col("value"))
        .singular(true)
        .build();

    // Prepare the input to the function.
    final ParserContext parserContext = new ParserContextBuilder().build();
    final NamedFunctionInput whereInput = new NamedFunctionInput(parserContext,
        inputPath, Collections.singletonList(argumentPath));

    // Execute the function.
    final NamedFunction whereFunction = NamedFunction.getInstance("where");
    final FhirPath result = whereFunction.invoke(whereInput);

    // Check the result dataset.
    final Dataset<Row> expectedDataset = new DatasetBuilder()
        .withIdColumn()
        .withStructTypeColumns(encounterStruct)
        .withRow("Patient/abc1", null)
        .withRow("Patient/abc1", RowFactory.create("Encounter/abc1", "in-progress"))
        .withRow("Patient/abc2", RowFactory.create("Encounter/abc3", "in-progress"))
        .withRow("Patient/abc3", null)
        .withRow("Patient/abc3", RowFactory.create("Encounter/abc4", "in-progress"))
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
    final ElementPath inputPath = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.STRING)
        .dataset(dataset)
        .idAndValueColumns()
        .singular(false)
        .build();

    // Build an expression which represents the argument to the function.
    final Dataset<Row> argumentDataset = inputPath.getDataset()
        .withColumn("value", inputPath.getValueColumn().equalTo("en"));
    check(inputPath.getIdColumn().isPresent());
    final ElementPath argumentExpression = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.BOOLEAN)
        .dataset(argumentDataset)
        .idColumn(inputPath.getIdColumn().get())
        .valueColumn(argumentDataset.col("value"))
        .singular(true)
        .build();

    // Prepare the input to the function.
    final ParserContext parserContext = new ParserContextBuilder().build();
    final NamedFunctionInput whereInput = new NamedFunctionInput(parserContext,
        inputPath,
        Collections.singletonList(argumentExpression));

    // Execute the function.
    final NamedFunction whereFunction = NamedFunction.getInstance("where");
    final FhirPath result = whereFunction.invoke(whereInput);

    // Check the result dataset.
    final Dataset<Row> expectedDataset = new DatasetBuilder()
        .withIdColumn()
        .withValueColumn(DataTypes.StringType)
        .withRow("Patient/abc1", null)
        .withRow("Patient/abc1", "en")
        .withRow("Patient/abc2", null)
        .withRow("Patient/abc3", null)
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
    final Dataset<Row> inputDataset = new DatasetBuilder()
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
        .dataset(inputDataset)
        .idAndValueColumns()
        .singular(false)
        .build();

    // Build an expression which represents the argument to the function.
    final BooleanLiteralPath argumentPath = BooleanLiteralPath
        .fromString("true", inputExpression);

    // Prepare the input to the function.
    final ParserContext parserContext = new ParserContextBuilder().build();
    final NamedFunctionInput whereInput = new NamedFunctionInput(parserContext, inputExpression,
        Collections.singletonList(argumentPath));

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
    final ElementPath inputPath = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.STRING)
        .dataset(dataset)
        .idAndValueColumns()
        .singular(false)
        .build();

    // Build an expression which represents the argument to the function.
    final Dataset<Row> argumentDataset = inputPath.getDataset()
        .withColumn("value",
            functions.when(inputPath.getValueColumn().equalTo("en"), null).otherwise(true));
    check(inputPath.getIdColumn().isPresent());
    final ElementPath argumentPath = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.BOOLEAN)
        .dataset(argumentDataset)
        .idColumn(inputPath.getIdColumn().get())
        .valueColumn(argumentDataset.col("value"))
        .singular(true)
        .build();

    // Prepare the input to the function.
    final ParserContext parserContext = new ParserContextBuilder().build();
    final NamedFunctionInput whereInput = new NamedFunctionInput(parserContext,
        inputPath, Collections.singletonList(argumentPath));

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
    assertEquals("where function accepts one argument", error.getMessage());
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