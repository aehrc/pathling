/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.QueryHelpers.createColumn;
import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.builders.DatasetBuilder.makeEid;
import static au.csiro.pathling.utilities.Strings.randomAlias;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.QueryHelpers.DatasetWithColumn;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.literal.BooleanLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import au.csiro.pathling.test.builders.ResourcePathBuilder;
import java.util.Arrays;
import java.util.Collections;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
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
    final String statusColumn = randomAlias();
    final Dataset<Row> inputDataset = new DatasetBuilder()
        .withIdColumn()
        .withEidColumn()
        .withIdColumn()
        .withColumn(statusColumn, DataTypes.StringType)
        .withRow("Patient/1", makeEid(1), "Encounter/1", "in-progress")
        .withRow("Patient/1", makeEid(0), "Encounter/2", "finished")
        .withRow("Patient/2", makeEid(0), "Encounter/3", "in-progress")
        .withRow("Patient/3", makeEid(1), "Encounter/4", "in-progress")
        .withRow("Patient/3", makeEid(0), "Encounter/5", "finished")
        .withRow("Patient/4", makeEid(1), "Encounter/6", "finished")
        .withRow("Patient/4", makeEid(0), "Encounter/7", "finished")
        .withRow("Patient/5", makeEid(1), "Encounter/8", "in-progress")
        .withRow("Patient/5", makeEid(0), "Encounter/9", "in-progress")
        .withRow("Patient/6", null, null, null)
        .build();
    final ResourcePath inputPath = new ResourcePathBuilder()
        .expression("reverseResolve(Encounter.subject)")
        .dataset(inputDataset)
        .idEidAndValueColumns()
        .buildCustom();

    // Build an expression which represents the argument to the function. We assume that the value
    // column from the input dataset is also present within the argument dataset.

    // @TODO: EID - Refactor common code
    final DatasetWithColumn inputDatasetWithThis = createColumn(
        inputPath.getDataset(), inputPath.makeThisColumn());

    final Dataset<Row> argumentDataset = inputDatasetWithThis.getDataset()
        .withColumn("value",
            inputDatasetWithThis.getDataset().col(statusColumn).equalTo("in-progress"));

    final ElementPath argumentPath = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.BOOLEAN)
        .dataset(argumentDataset)
        .idColumn(inputPath.getIdColumn())
        .valueColumn(argumentDataset.col("value"))
        .thisColumn(inputDatasetWithThis.getColumn())
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
        .withEidColumn()
        .withIdColumn()
        .withRow("Patient/1", makeEid(0), null)
        .withRow("Patient/1", makeEid(1), "Patient/1")
        .withRow("Patient/2", makeEid(0), "Patient/2")
        .withRow("Patient/3", makeEid(0), null)
        .withRow("Patient/3", makeEid(1), "Patient/3")
        .withRow("Patient/4", makeEid(0), null)
        .withRow("Patient/4", makeEid(1), null)
        .withRow("Patient/5", makeEid(0), "Patient/5")
        .withRow("Patient/5", makeEid(1), "Patient/5")
        .withRow("Patient/6", null, null)
        .build();

    assertThat(result)
        .selectOrderedResultWithEid()
        .hasRows(expectedDataset);
  }

  @Test
  public void whereOnElement() {
    // Build an expression which represents the input to the function.
    final Dataset<Row> dataset = new DatasetBuilder()
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.StringType)
        .withRow("Patient/1", makeEid(1), "en")
        .withRow("Patient/1", makeEid(0), "es")
        .withRow("Patient/2", makeEid(0), "de")
        .withRow("Patient/3", makeEid(2), "en")
        .withRow("Patient/3", makeEid(1), "en")
        .withRow("Patient/3", makeEid(0), "zh")
        .withRow("Patient/4", makeEid(1), "fr")
        .withRow("Patient/4", makeEid(0), "fr")
        .withRow("Patient/5", null, null)
        .build();
    final ElementPath inputPath = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.STRING)
        .dataset(dataset)
        .idAndEidAndValueColumns()
        .singular(false)
        .build();

    final DatasetWithColumn inputDatasetWithThis = createColumn(
        inputPath.getDataset(), inputPath.makeThisColumn());

    // Build an expression which represents the argument to the function.
    final Dataset<Row> argumentDataset = inputDatasetWithThis.getDataset()
        .withColumn("value", inputPath.getValueColumn().equalTo("en"));

    final ElementPath argumentExpression = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.BOOLEAN)
        .dataset(argumentDataset)
        .idColumn(inputPath.getIdColumn())
        .valueColumn(argumentDataset.col("value"))
        .thisColumn(inputDatasetWithThis.getColumn())
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
        .withEidColumn()
        .withColumn(DataTypes.StringType)
        .withRow("Patient/1", makeEid(0), null)
        .withRow("Patient/1", makeEid(1), "en")
        .withRow("Patient/2", makeEid(0), null)
        .withRow("Patient/3", makeEid(0), null)
        .withRow("Patient/3", makeEid(1), "en")
        .withRow("Patient/3", makeEid(2), "en")
        .withRow("Patient/4", makeEid(0), null)
        .withRow("Patient/4", makeEid(1), null)
        .withRow("Patient/5", null, null)
        .build();
    assertThat(result)
        .selectOrderedResultWithEid()
        .hasRows(expectedDataset);
  }

  @Test
  public void nullValuesAreNull() {
    // Build an expression which represents the input to the function.
    final Dataset<Row> dataset = new DatasetBuilder()
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.StringType)
        .withRow("Patient/1", makeEid(0), "en")
        .withRow("Patient/1", makeEid(1), "es")
        .withRow("Patient/2", makeEid(0), "de")
        .withRow("Patient/3", makeEid(0), "en")
        .withRow("Patient/3", makeEid(1), "en")
        .withRow("Patient/3", makeEid(2), "zh")
        .withRow("Patient/4", makeEid(0), "ar")
        .build();
    final ElementPath inputPath = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.STRING)
        .dataset(dataset)
        .idAndEidAndValueColumns()
        .singular(false)
        .build();

    final DatasetWithColumn inputDatasetWithThis = createColumn(
        inputPath.getDataset(), inputPath.makeThisColumn());
    // Build an expression which represents the argument to the function.
    final Dataset<Row> argumentDataset = inputDatasetWithThis.getDataset()
        .withColumn("value",
            functions.when(inputPath.getValueColumn().equalTo("en"), null).otherwise(true));
    final ElementPath argumentPath = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.BOOLEAN)
        .dataset(argumentDataset)
        .idColumn(inputPath.getIdColumn())
        .valueColumn(argumentDataset.col("value"))
        .thisColumn(inputDatasetWithThis.getColumn())
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
        .withEidColumn()
        .withColumn(DataTypes.StringType)
        .withRow("Patient/1", makeEid(0), null)
        .withRow("Patient/1", makeEid(1), "es")
        .withRow("Patient/2", makeEid(0), "de")
        .withRow("Patient/3", makeEid(0), null)
        .withRow("Patient/3", makeEid(1), null)
        .withRow("Patient/3", makeEid(2), "zh")
        .withRow("Patient/4", makeEid(0), "ar")
        .build();
    assertThat(result)
        .selectOrderedResultWithEid()
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

  @Test
  public void throwsErrorIfArgumentIsLiteral() {
    final ResourcePath input = new ResourcePathBuilder().build();
    final BooleanLiteralPath argument = BooleanLiteralPath
        .fromString("true", input);

    final ParserContext parserContext = new ParserContextBuilder().build();
    final NamedFunctionInput whereInput = new NamedFunctionInput(parserContext, input,
        Collections.singletonList(argument));

    final NamedFunction whereFunction = NamedFunction.getInstance("where");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> whereFunction.invoke(whereInput));
    assertEquals(
        "Argument to where function cannot be a literal: true",
        error.getMessage());
  }

}