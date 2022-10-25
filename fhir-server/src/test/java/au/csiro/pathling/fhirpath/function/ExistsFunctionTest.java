/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.builders.DatasetBuilder.makeEid;
import static au.csiro.pathling.test.helpers.SparkHelpers.codeableConceptStructType;
import static au.csiro.pathling.test.helpers.SparkHelpers.rowFromCodeableConcept;
import static au.csiro.pathling.utilities.Strings.randomAlias;
import static org.apache.spark.sql.functions.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.BooleanPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import au.csiro.pathling.test.builders.ResourcePathBuilder;
import au.csiro.pathling.test.helpers.TestHelpers;
import ca.uhn.fhir.context.FhirContext;
import java.util.Collections;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
@Tag("UnitTest")
public class ExistsFunctionTest {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirContext fhirContext;

  @Test
  void returnsOppositeResultsToEmpty() {
    final Coding coding1 = new Coding(TestHelpers.SNOMED_URL, "840546002", "Exposure to COVID-19");
    final CodeableConcept concept1 = new CodeableConcept(coding1);
    final Coding coding2 = new Coding(TestHelpers.SNOMED_URL, "248427009", "Fever symptoms");
    final CodeableConcept concept2 = new CodeableConcept(coding2);

    final Dataset<Row> dataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withColumn(codeableConceptStructType())
        .withRow("observation-1", null)
        .withRow("observation-2", null)
        .withRow("observation-2", null)
        .withRow("observation-3", rowFromCodeableConcept(concept1))
        .withRow("observation-4", rowFromCodeableConcept(concept1))
        .withRow("observation-4", null)
        .withRow("observation-5", rowFromCodeableConcept(concept1))
        .withRow("observation-5", rowFromCodeableConcept(concept2))
        .build();
    final ElementPath input = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.CODEABLECONCEPT)
        .dataset(dataset)
        .idAndValueColumns()
        .expression("code")
        .build();
    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .groupingColumns(Collections.singletonList(input.getIdColumn()))
        .build();

    // Set up the function input.
    final NamedFunctionInput emptyInput = new NamedFunctionInput(parserContext, input,
        Collections.emptyList());

    // Invoke the empty function.
    final NamedFunction emptyFunction = NamedFunction.getInstance("empty");
    final FhirPath emptyResult = emptyFunction.invoke(emptyInput);

    // Create an expected dataset from the result of the empty function, with an inverted value.
    final Dataset<Row> emptyResultDataset = emptyResult.getDataset();
    final Column invertedValue = not(emptyResult.getValueColumn());
    final Dataset<Row> expectedDataset = emptyResultDataset.select(emptyResult.getIdColumn(),
        invertedValue);

    // Invoke the exists function.
    final NamedFunction existsFunction = NamedFunction.getInstance("exists");
    final FhirPath existsResult = existsFunction.invoke(emptyInput);

    // Check the result.
    assertThat(existsResult)
        .hasExpression("code.exists()")
        .isSingular()
        .isElementPath(BooleanPath.class)
        .selectOrderedResult()
        .hasRows(expectedDataset);
  }

  @Test
  void returnsSameResultsAsWhereAndExists() {
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

    // Prepare the input to the where function.
    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();
    final NamedFunctionInput whereInput = new NamedFunctionInput(parserContext,
        inputPath, Collections.singletonList(argumentPath));

    // Execute the where function.
    final NamedFunction whereFunction = NamedFunction.getInstance("where");
    final FhirPath whereResult = whereFunction.invoke(whereInput);

    // Prepare the input to the exists function.
    final NamedFunctionInput existsInput = new NamedFunctionInput(parserContext,
        (NonLiteralPath) whereResult, Collections.emptyList());

    // Execute the exists function.
    final NamedFunction existsFunction = NamedFunction.getInstance("exists");
    final FhirPath whereExistsResult = existsFunction.invoke(existsInput);

    // Prepare the input to the exists function (with argument).
    final NamedFunctionInput existsWithArgumentInput = new NamedFunctionInput(parserContext,
        inputPath, Collections.singletonList(argumentPath));
    final FhirPath existsWithArgumentResult = existsFunction.invoke(existsWithArgumentInput);

    // Check the results.
    assertThat(existsWithArgumentResult.getDataset()
        .select(existsWithArgumentResult.getIdColumn(), existsWithArgumentResult.getValueColumn()))
        .hasRows(whereExistsResult.getDataset()
            .select(whereExistsResult.getIdColumn(), whereExistsResult.getValueColumn()));
  }

  @Test
  void throwsErrorIfArgumentNotBoolean() {
    final ResourcePath input = new ResourcePathBuilder(spark).build();
    final ElementPath argument = new ElementPathBuilder(spark)
        .expression("$this.gender")
        .fhirType(FHIRDefinedType.STRING)
        .build();

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();
    final NamedFunctionInput existsInput = new NamedFunctionInput(parserContext, input,
        Collections.singletonList(argument));

    final NamedFunction existsFunction = NamedFunction.getInstance("exists");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> existsFunction.invoke(existsInput));
    assertEquals(
        "Argument to exists function must be a singular Boolean: $this.gender",
        error.getMessage());
  }

  @Test
  void throwsErrorIfArgumentNotSingular() {
    final ResourcePath input = new ResourcePathBuilder(spark).build();
    final ElementPath argument = new ElementPathBuilder(spark)
        .expression("$this.communication.preferred")
        .fhirType(FHIRDefinedType.BOOLEAN)
        .singular(false)
        .build();

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();
    final NamedFunctionInput existsInput = new NamedFunctionInput(parserContext, input,
        Collections.singletonList(argument));

    final NamedFunction existsFunction = NamedFunction.getInstance("exists");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> existsFunction.invoke(existsInput));
    assertEquals(
        "Argument to exists function must be a singular Boolean: "
            + "$this.communication.preferred",
        error.getMessage());
  }

}
