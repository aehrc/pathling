/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.builders.DatasetBuilder.makeEid;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.BooleanPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.literal.StringLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import ca.uhn.fhir.context.FhirContext;
import java.util.Collections;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * @author John Grimes
 */
@SpringBootTest
@Tag("UnitTest")
class NotFunctionTest {

  @Autowired
  private SparkSession spark;

  @Autowired
  private FhirContext fhirContext;

  @Test
  void returnsCorrectResults() {
    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();
    final Dataset<Row> dataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.BooleanType)
        .withRow("observation-1", makeEid(0), true)
        .withRow("observation-2", makeEid(0), false)
        .withRow("observation-3", makeEid(0), null)
        .withRow("observation-4", makeEid(0), true)
        .withRow("observation-4", makeEid(1), false)
        .withRow("observation-5", makeEid(0), null)
        .withRow("observation-5", makeEid(1), null)
        .build();
    final ElementPath input = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.BOOLEAN)
        .dataset(dataset)
        .idAndEidAndValueColumns()
        .expression("valueBoolean")
        .singular(false)
        .build();

    final NamedFunctionInput notInput = new NamedFunctionInput(parserContext, input,
        Collections.emptyList());
    final NamedFunction notFunction = NamedFunction.getInstance("not");
    final FhirPath result = notFunction.invoke(notInput);

    final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withColumn(DataTypes.BooleanType)
        .withRow("observation-1", false)
        .withRow("observation-2", true)
        .withRow("observation-3", null)
        .withRow("observation-4", false)
        .withRow("observation-4", true)
        .withRow("observation-5", null)
        .withRow("observation-5", null)
        .build();
    assertThat(result)
        .hasExpression("valueBoolean.not()")
        .isNotSingular()
        .isElementPath(BooleanPath.class)
        .selectOrderedResult()
        .hasRows(expectedDataset);
  }

  @Test
  public void inputMustNotContainArguments() {
    final ElementPath input = new ElementPathBuilder(spark).build();
    final StringLiteralPath argument = StringLiteralPath
        .fromString("'some argument'", input);

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();
    final NamedFunctionInput notInput = new NamedFunctionInput(parserContext, input,
        Collections.singletonList(argument));

    final NamedFunction notFunction = NamedFunction.getInstance("not");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> notFunction.invoke(notInput));
    assertEquals(
        "Arguments can not be passed to not function",
        error.getMessage());
  }

  @Test
  public void throwsErrorIfInputNotBoolean() {
    final ElementPath input = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.INTEGER)
        .expression("valueInteger")
        .build();

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();
    final NamedFunctionInput notInput = new NamedFunctionInput(parserContext, input,
        Collections.emptyList());

    final NamedFunction notFunction = NamedFunction.getInstance("not");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> notFunction.invoke(notInput));
    assertEquals(
        "Input to not function must be Boolean: valueInteger",
        error.getMessage());
  }

}