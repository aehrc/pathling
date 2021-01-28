/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.builders.DatasetBuilder.makeEid;
import static au.csiro.pathling.utilities.Strings.randomAlias;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.literal.BooleanLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import au.csiro.pathling.test.builders.ResourcePathBuilder;
import ca.uhn.fhir.context.FhirContext;
import java.util.Arrays;
import java.util.Collections;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * @author John Grimes
 */
@SpringBootTest
@TestInstance(Lifecycle.PER_METHOD)
@Tag("UnitTest")
public class WhereFunctionTest {

  @Autowired
  private SparkSession spark;

  @Autowired
  private FhirContext fhirContext;

  // This test simulates the execution of the where function on the path
  // `Patient.reverseResolve(Encounter.subject).where($this.status = 'in-progress')`.
  @Test
  public void whereOnResource() {
    final String statusColumn = randomAlias();
    final Dataset<Row> inputDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withIdColumn()
        .withColumn(statusColumn, DataTypes.StringType)
        .withRow("patient-1", makeEid(1), "encounter-1", "in-progress")
        .withRow("patient-1", makeEid(0), "encounter-2", "finished")
        .withRow("patient-2", makeEid(0), "encounter-3", "in-progress")
        .withRow("patient-3", makeEid(1), "encounter-4", "in-progress")
        .withRow("patient-3", makeEid(0), "encounter-5", "finished")
        .withRow("patient-4", makeEid(1), "encounter-6", "finished")
        .withRow("patient-4", makeEid(0), "encounter-7", "finished")
        .withRow("patient-5", makeEid(1), "encounter-8", "in-progress")
        .withRow("patient-5", makeEid(0), "encounter-9", "in-progress")
        .withRow("patient-6", null, null, null)
        .build();
    final ResourcePath inputPath = new ResourcePathBuilder(spark)
        .expression("reverseResolve(Encounter.subject)")
        .dataset(inputDataset)
        .idEidAndValueColumns()
        .buildCustom();

    // Build an expression which represents the argument to the function. We assume that the value
    // column from the input dataset is also present within the argument dataset.

    final NonLiteralPath thisPath = inputPath.toThisPath();

    final Dataset<Row> argumentDataset = thisPath.getDataset()
        .withColumn("value",
            thisPath.getDataset().col(statusColumn).equalTo("in-progress"));

    assertTrue(thisPath.getThisColumn().isPresent());
    final ElementPath argumentPath = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.BOOLEAN)
        .dataset(argumentDataset)
        .idColumn(inputPath.getIdColumn())
        .valueColumn(argumentDataset.col("value"))
        .thisColumn(thisPath.getThisColumn().get())
        .singular(true)
        .build();

    // Prepare the input to the function.
    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();
    final NamedFunctionInput whereInput = new NamedFunctionInput(parserContext,
        inputPath, Collections.singletonList(argumentPath));

    // Execute the function.
    final NamedFunction whereFunction = NamedFunction.getInstance("where");
    final FhirPath result = whereFunction.invoke(whereInput);

    // Check the result dataset.
    final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withIdColumn()
        .withRow("patient-1", makeEid(0), null)
        .withRow("patient-1", makeEid(1), "patient-1")
        .withRow("patient-2", makeEid(0), "patient-2")
        .withRow("patient-3", makeEid(0), null)
        .withRow("patient-3", makeEid(1), "patient-3")
        .withRow("patient-4", makeEid(0), null)
        .withRow("patient-4", makeEid(1), null)
        .withRow("patient-5", makeEid(0), "patient-5")
        .withRow("patient-5", makeEid(1), "patient-5")
        .withRow("patient-6", null, null)
        .build();

    assertThat(result)
        .selectOrderedResultWithEid()
        .hasRows(expectedDataset);
  }

  @Test
  public void whereOnElement() {
    // Build an expression which represents the input to the function.
    final Dataset<Row> dataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.StringType)
        .withRow("patient-1", makeEid(1), "en")
        .withRow("patient-1", makeEid(0), "es")
        .withRow("patient-2", makeEid(0), "de")
        .withRow("patient-3", makeEid(2), "en")
        .withRow("patient-3", makeEid(1), "en")
        .withRow("patient-3", makeEid(0), "zh")
        .withRow("patient-4", makeEid(1), "fr")
        .withRow("patient-4", makeEid(0), "fr")
        .withRow("patient-5", null, null)
        .build();
    final ElementPath inputPath = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.STRING)
        .dataset(dataset)
        .idAndEidAndValueColumns()
        .singular(false)
        .build();

    final NonLiteralPath thisPath = inputPath.toThisPath();

    // Build an expression which represents the argument to the function.
    final Dataset<Row> argumentDataset = thisPath.getDataset()
        .withColumn("value", inputPath.getValueColumn().equalTo("en"));

    assertTrue(thisPath.getThisColumn().isPresent());
    final ElementPath argumentExpression = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.BOOLEAN)
        .dataset(argumentDataset)
        .idColumn(inputPath.getIdColumn())
        .valueColumn(argumentDataset.col("value"))
        .thisColumn(thisPath.getThisColumn().get())
        .singular(true)
        .build();

    // Prepare the input to the function.
    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();
    final NamedFunctionInput whereInput = new NamedFunctionInput(parserContext,
        inputPath,
        Collections.singletonList(argumentExpression));

    // Execute the function.
    final NamedFunction whereFunction = NamedFunction.getInstance("where");
    final FhirPath result = whereFunction.invoke(whereInput);

    // Check the result dataset.
    final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.StringType)
        .withRow("patient-1", makeEid(0), null)
        .withRow("patient-1", makeEid(1), "en")
        .withRow("patient-2", makeEid(0), null)
        .withRow("patient-3", makeEid(0), null)
        .withRow("patient-3", makeEid(1), "en")
        .withRow("patient-3", makeEid(2), "en")
        .withRow("patient-4", makeEid(0), null)
        .withRow("patient-4", makeEid(1), null)
        .withRow("patient-5", null, null)
        .build();
    assertThat(result)
        .selectOrderedResultWithEid()
        .hasRows(expectedDataset);
  }

  @Test
  public void nullValuesAreNull() {
    // Build an expression which represents the input to the function.
    final Dataset<Row> dataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.StringType)
        .withRow("patient-1", makeEid(0), "en")
        .withRow("patient-1", makeEid(1), "es")
        .withRow("patient-2", makeEid(0), "de")
        .withRow("patient-3", makeEid(0), "en")
        .withRow("patient-3", makeEid(1), "en")
        .withRow("patient-3", makeEid(2), "zh")
        .withRow("patient-4", makeEid(0), "ar")
        .build();
    final ElementPath inputPath = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.STRING)
        .dataset(dataset)
        .idAndEidAndValueColumns()
        .singular(false)
        .build();

    // Build an expression which represents the argument to the function.
    final NonLiteralPath thisPath = inputPath.toThisPath();

    final Dataset<Row> argumentDataset = thisPath.getDataset()
        .withColumn("value",
            functions.when(inputPath.getValueColumn().equalTo("en"), null).otherwise(true));
    assertTrue(thisPath.getThisColumn().isPresent());
    final ElementPath argumentPath = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.BOOLEAN)
        .dataset(argumentDataset)
        .idColumn(inputPath.getIdColumn())
        .valueColumn(argumentDataset.col("value"))
        .thisColumn(thisPath.getThisColumn().get())
        .singular(true)
        .build();

    // Prepare the input to the function.
    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();
    final NamedFunctionInput whereInput = new NamedFunctionInput(parserContext,
        inputPath, Collections.singletonList(argumentPath));

    // Execute the function.
    final NamedFunction whereFunction = NamedFunction.getInstance("where");
    final FhirPath result = whereFunction.invoke(whereInput);

    // Check the result dataset.
    final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.StringType)
        .withRow("patient-1", makeEid(0), null)
        .withRow("patient-1", makeEid(1), "es")
        .withRow("patient-2", makeEid(0), "de")
        .withRow("patient-3", makeEid(0), null)
        .withRow("patient-3", makeEid(1), null)
        .withRow("patient-3", makeEid(2), "zh")
        .withRow("patient-4", makeEid(0), "ar")
        .build();
    assertThat(result)
        .selectOrderedResultWithEid()
        .hasRows(expectedDataset);
  }

  @Test
  public void throwsErrorIfMoreThanOneArgument() {
    final ResourcePath input = new ResourcePathBuilder(spark).build();
    final ElementPath argument1 = new ElementPathBuilder(spark)
        .expression("$this.gender = 'female'")
        .fhirType(FHIRDefinedType.BOOLEAN)
        .build();
    final ElementPath argument2 = new ElementPathBuilder(spark)
        .expression("$this.gender != 'male'")
        .fhirType(FHIRDefinedType.BOOLEAN)
        .build();

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();
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
    final ResourcePath input = new ResourcePathBuilder(spark).build();
    final ElementPath argument = new ElementPathBuilder(spark)
        .expression("$this.gender")
        .fhirType(FHIRDefinedType.STRING)
        .build();

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();
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
    final ResourcePath input = new ResourcePathBuilder(spark).build();
    final ElementPath argument = new ElementPathBuilder(spark)
        .expression("$this.communication.preferred")
        .fhirType(FHIRDefinedType.BOOLEAN)
        .singular(false)
        .build();

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();
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
    final ResourcePath input = new ResourcePathBuilder(spark).build();
    final BooleanLiteralPath argument = BooleanLiteralPath
        .fromString("true", input);

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();
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