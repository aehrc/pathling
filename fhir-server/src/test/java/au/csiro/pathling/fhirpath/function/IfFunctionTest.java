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
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.element.StringPath;
import au.csiro.pathling.fhirpath.literal.StringLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.test.builders.*;
import ca.uhn.fhir.context.FhirContext;
import java.util.Arrays;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * @author John Grimes
 */
@SpringBootTest
@Tag("UnitTest")
class IfFunctionTest {

  @Autowired
  private SparkSession spark;

  @Autowired
  private FhirContext fhirContext;

  @Test
  void returnsCorrectResultsForTwoLiterals() {
    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();
    final Dataset<Row> inputContextDataset = new ResourceDatasetBuilder(spark)
        .withIdColumn()
        .withRow("observation-1")
        .withRow("observation-2")
        .withRow("observation-3")
        .withRow("observation-4")
        .withRow("observation-5")
        .build();
    final ResourcePath inputContext = new ResourcePathBuilder(spark)
        .expression("Observation")
        .resourceType(ResourceType.OBSERVATION)
        .dataset(inputContextDataset)
        .singular(true)
        .build();
    final NonLiteralPath thisPath = inputContext.toThisPath();
    final Dataset<Row> conditionDataset = new DatasetBuilder(spark)
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
    assertTrue(thisPath.getThisColumn().isPresent());
    final Dataset<Row> conditionDatasetWithThis = conditionDataset.join(thisPath.getDataset(),
        thisPath.getIdColumn().equalTo(conditionDataset.col(conditionDataset.columns()[0])),
        "left_outer");
    final ElementPath condition = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.BOOLEAN)
        .dataset(conditionDatasetWithThis)
        .idAndEidAndValueColumns()
        .expression("valueBoolean")
        .singular(false)
        .thisColumn(thisPath.getThisColumn().get())
        .build();
    final StringLiteralPath ifTrue = StringLiteralPath.fromString("foo", inputContext);
    final StringLiteralPath otherwise = StringLiteralPath.fromString("bar", inputContext);

    final NamedFunctionInput ifInput = new NamedFunctionInput(parserContext, inputContext,
        Arrays.asList(condition, ifTrue, otherwise));
    final FhirPath result = NamedFunction.getInstance("iif").invoke(ifInput);

    final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withColumn(DataTypes.StringType)
        .withRow("observation-1", "foo")
        .withRow("observation-2", "bar")
        .withRow("observation-3", "bar")
        .withRow("observation-4", "foo")
        .withRow("observation-4", "bar")
        .withRow("observation-5", "bar")
        .withRow("observation-5", "bar")
        .build();
    assertThat(result)
        .hasExpression("Observation.iif(valueBoolean, 'foo', 'bar')")
        .isNotSingular()
        .isElementPath(StringPath.class)
        .selectResult()
        .hasRowsUnordered(expectedDataset);
  }

  @Test
  void throwsErrorIfConditionNotBoolean() {
    final ElementPath condition = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.INTEGER)
        .expression("valueInteger")
        .build();
    final StringLiteralPath ifTrue = StringLiteralPath.fromString("foo", condition);
    final StringLiteralPath otherwise = StringLiteralPath.fromString("bar", condition);

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();
    final NamedFunctionInput ifInput = new NamedFunctionInput(parserContext, condition,
        Arrays.asList(condition, ifTrue, otherwise));

    final NamedFunction notFunction = NamedFunction.getInstance("iif");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> notFunction.invoke(ifInput));
    assertEquals(
        "Condition argument to iif must be Boolean: valueInteger",
        error.getMessage());
  }

  @Test
  void throwsErrorIfNoThisInCondition() {
    final ElementPath condition = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.BOOLEAN)
        .expression("valueBoolean")
        .build();
    final StringLiteralPath ifTrue = StringLiteralPath.fromString("foo", condition);
    final StringLiteralPath otherwise = StringLiteralPath.fromString("bar", condition);

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();
    final NamedFunctionInput ifInput = new NamedFunctionInput(parserContext, condition,
        Arrays.asList(condition, ifTrue, otherwise));

    final NamedFunction notFunction = NamedFunction.getInstance("iif");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> notFunction.invoke(ifInput));
    assertEquals(
        "Condition argument to iif function must be navigable from collection item (use $this): valueBoolean",
        error.getMessage());
  }
 
}