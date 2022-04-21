/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.QueryHelpers.join;
import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.builders.DatasetBuilder.makeEid;
import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.QueryHelpers.JoinType;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.ResourceDefinition;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.UntypedResourcePath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.element.IntegerPath;
import au.csiro.pathling.fhirpath.element.StringPath;
import au.csiro.pathling.fhirpath.literal.IntegerLiteralPath;
import au.csiro.pathling.fhirpath.literal.StringLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.Database;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import au.csiro.pathling.test.builders.ResourceDatasetBuilder;
import au.csiro.pathling.test.builders.ResourcePathBuilder;
import ca.uhn.fhir.context.FhirContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * @author John Grimes
 */
@SpringBootTest
@Tag("UnitTest")
class IifFunctionTest {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirContext fhirContext;

  ParserContext parserContext;
  Database database;
  Dataset<Row> dataset;

  @BeforeEach
  void setUp() {
    dataset = new DatasetBuilder(spark)
        .withIdColumn("id")
        .withColumn(DataTypes.StringType)
        .build();
    final ResourcePath inputContext = new ResourcePathBuilder(spark)
        .dataset(dataset)
        .build();
    parserContext = new ParserContextBuilder(spark, fhirContext)
        .inputContext(inputContext)
        .build();
    database = mock(Database.class);
  }

  @Test
  void returnsCorrectResultsForTwoLiterals() {
    final Dataset<Row> inputContextDataset = new ResourceDatasetBuilder(spark)
        .withIdColumn()
        .withRow("observation-1")
        .withRow("observation-2")
        .withRow("observation-3")
        .withRow("observation-4")
        .withRow("observation-5")
        .build();
    when(database.read(ResourceType.OBSERVATION)).thenReturn(inputContextDataset);
    final ResourcePath inputContext = new ResourcePathBuilder(spark)
        .expression("Observation")
        .resourceType(ResourceType.OBSERVATION)
        .database(database)
        .singular(true)
        .build();
    final Dataset<Row> inputDataset = new DatasetBuilder(spark)
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
    final ElementPath inputPath = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.BOOLEAN)
        .dataset(inputDataset)
        .idAndEidAndValueColumns()
        .expression("valueBoolean")
        .singular(false)
        .build();
    parserContext = new ParserContextBuilder(spark, fhirContext)
        .groupingColumns(Collections.singletonList(inputPath.getIdColumn()))
        .build();
    final NonLiteralPath condition = inputPath.toThisPath();
    final StringLiteralPath ifTrue = StringLiteralPath.fromString("foo", inputContext);
    final StringLiteralPath otherwise = StringLiteralPath.fromString("bar", inputContext);

    final NamedFunctionInput iifInput = new NamedFunctionInput(parserContext, inputPath,
        Arrays.asList(condition, ifTrue, otherwise));
    final FhirPath result = NamedFunction.getInstance("iif").invoke(iifInput);

    final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.StringType)
        .withRow("observation-1", makeEid(0), "foo")
        .withRow("observation-2", makeEid(0), "bar")
        .withRow("observation-3", makeEid(0), "bar")
        .withRow("observation-4", makeEid(0), "foo")
        .withRow("observation-4", makeEid(1), "bar")
        .withRow("observation-5", makeEid(0), "bar")
        .withRow("observation-5", makeEid(1), "bar")
        .build();
    assertThat(result)
        .hasExpression("valueBoolean.iif($this, 'foo', 'bar')")
        .isNotSingular()
        .isElementPath(StringPath.class)
        .selectOrderedResultWithEid()
        .hasRowsUnordered(expectedDataset);
  }

  @Test
  void returnsCorrectResultsForTwoNonLiterals() {
    final Dataset<Row> inputDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.BooleanType)
        .withRow("observation-1", makeEid(0), false)
        .withRow("observation-2", makeEid(0), true)
        .withRow("observation-3", makeEid(0), null)
        .withRow("observation-4", makeEid(0), true)
        .withRow("observation-4", makeEid(1), false)
        .withRow("observation-5", makeEid(0), null)
        .withRow("observation-5", makeEid(1), null)
        .build();
    final ElementPath inputPath = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.BOOLEAN)
        .dataset(inputDataset)
        .idAndEidAndValueColumns()
        .expression("valueBoolean")
        .singular(false)
        .build();
    parserContext = new ParserContextBuilder(spark, fhirContext)
        .groupingColumns(Collections.singletonList(inputPath.getIdColumn()))
        .build();
    final NonLiteralPath condition = inputPath.toThisPath();

    final Dataset<Row> ifTrueDataset = new DatasetBuilder(spark)
        .withContext(inputPath)
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.IntegerType)
        .withRow("observation-1", makeEid(0), 1)
        .withRow("observation-2", makeEid(0), 2)
        .withRow("observation-3", makeEid(0), 3)
        .withRow("observation-4", makeEid(0), 4)
        .withRow("observation-5", makeEid(0), 5)
        .build();
    final ElementPath ifTrue = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.INTEGER)
        .dataset(ifTrueDataset)
        .idAndEidAndValueColumns()
        .expression("someInteger")
        .singular(true)
        .build();

    final Dataset<Row> otherwiseDataset = new DatasetBuilder(spark)
        .withContext(inputPath)
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.IntegerType)
        .withRow("observation-1", makeEid(0), 11)
        .withRow("observation-1", makeEid(0), 16)
        .withRow("observation-2", makeEid(0), 12)
        .withRow("observation-3", makeEid(0), 13)
        .withRow("observation-4", makeEid(0), 14)
        .withRow("observation-5", makeEid(0), 15)
        .build();
    final ElementPath otherwise = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.INTEGER)
        .dataset(otherwiseDataset)
        .idAndEidAndValueColumns()
        .expression("anotherInteger")
        .singular(true)
        .build();

    final NamedFunctionInput iifInput = new NamedFunctionInput(parserContext, inputPath,
        Arrays.asList(condition, ifTrue, otherwise));
    final FhirPath result = NamedFunction.getInstance("iif").invoke(iifInput);

    final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.IntegerType)
        .withRow("observation-1", makeEid(0), 11)
        .withRow("observation-1", makeEid(0), 16)
        .withRow("observation-2", makeEid(0), 2)
        .withRow("observation-3", makeEid(0), 13)
        .withRow("observation-4", makeEid(0), 4)
        .withRow("observation-4", makeEid(1), 14)
        .withRow("observation-5", makeEid(0), 15)
        .withRow("observation-5", makeEid(1), 15)
        .build();
    assertThat(result)
        .hasExpression("valueBoolean.iif($this, someInteger, anotherInteger)")
        .isNotSingular()
        .isElementPath(IntegerPath.class)
        .selectOrderedResultWithEid()
        .hasRowsUnordered(expectedDataset);
  }

  @Test
  void returnsCorrectResultsForLiteralAndNonLiteral() {
    final Dataset<Row> contextDataset = new ResourceDatasetBuilder(spark)
        .withIdColumn("id")
        .withRow("observation-1")
        .withRow("observation-2")
        .withRow("observation-3")
        .withRow("observation-4")
        .withRow("observation-5")
        .build();
    when(database.read(ResourceType.OBSERVATION)).thenReturn(contextDataset);
    final ResourcePath inputContext = new ResourcePathBuilder(spark)
        .expression("Observation")
        .resourceType(ResourceType.OBSERVATION)
        .database(database)
        .singular(true)
        .build();
    final Dataset<Row> inputDataset = new DatasetBuilder(spark)
        .withContext(inputContext)
        .withIdColumn("inputId")
        .withEidColumn()
        .withColumn(DataTypes.BooleanType)
        .withRow("observation-1", makeEid(0), false)
        .withRow("observation-2", makeEid(0), true)
        .withRow("observation-3", makeEid(0), null)
        .withRow("observation-4", makeEid(0), true)
        .withRow("observation-4", makeEid(1), false)
        .withRow("observation-5", makeEid(0), null)
        .withRow("observation-5", makeEid(1), null)
        .build();
    final ElementPath inputPath = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.BOOLEAN)
        .dataset(inputDataset)
        .idAndEidAndValueColumns()
        .expression("valueBoolean")
        .singular(false)
        .build();
    parserContext = new ParserContextBuilder(spark, fhirContext)
        .groupingColumns(Collections.singletonList(inputPath.getIdColumn()))
        .build();
    final NonLiteralPath condition = inputPath.toThisPath();

    final Dataset<Row> ifTrueDataset = new DatasetBuilder(spark)
        .withContext(inputContext)
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.IntegerType)
        .withRow("observation-1", makeEid(0), 1)
        .withRow("observation-2", makeEid(0), 2)
        .withRow("observation-3", makeEid(0), 3)
        .withRow("observation-4", makeEid(0), 4)
        .withRow("observation-5", makeEid(0), 5)
        .build();
    final ElementPath ifTrue = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.INTEGER)
        .dataset(ifTrueDataset)
        .idAndEidAndValueColumns()
        .expression("someInteger")
        .singular(true)
        .build();

    final IntegerLiteralPath otherwise = IntegerLiteralPath.fromString("99", inputContext);

    final NamedFunctionInput iifInput1 = new NamedFunctionInput(parserContext, inputPath,
        Arrays.asList(condition, ifTrue, otherwise));
    final FhirPath result1 = NamedFunction.getInstance("iif").invoke(iifInput1);

    final Dataset<Row> expectedDataset1 = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.IntegerType)
        .withRow("observation-1", makeEid(0), 99)
        .withRow("observation-2", makeEid(0), 2)
        .withRow("observation-3", makeEid(0), 99)
        .withRow("observation-4", makeEid(0), 4)
        .withRow("observation-4", makeEid(1), 99)
        .withRow("observation-5", makeEid(0), 99)
        .withRow("observation-5", makeEid(1), 99)
        .build();
    assertThat(result1)
        .hasExpression("valueBoolean.iif($this, someInteger, 99)")
        .isNotSingular()
        .isElementPath(IntegerPath.class)
        .selectOrderedResultWithEid()
        .hasRows(expectedDataset1);

    final NamedFunctionInput iifInput2 = new NamedFunctionInput(parserContext, inputPath,
        Arrays.asList(condition, otherwise, ifTrue));
    final FhirPath result2 = NamedFunction.getInstance("iif").invoke(iifInput2);

    final Dataset<Row> expectedDataset2 = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.IntegerType)
        .withRow("observation-1", makeEid(0), 1)
        .withRow("observation-2", makeEid(0), 99)
        .withRow("observation-3", makeEid(0), 3)
        .withRow("observation-4", makeEid(0), 99)
        .withRow("observation-4", makeEid(1), 4)
        .withRow("observation-5", makeEid(0), 5)
        .withRow("observation-5", makeEid(1), 5)
        .build();
    assertThat(result2)
        .hasExpression("valueBoolean.iif($this, 99, someInteger)")
        .isNotSingular()
        .isElementPath(IntegerPath.class)
        .selectOrderedResultWithEid()
        .hasRows(expectedDataset2);
  }

  @Test
  void throwsErrorIfConditionNotBoolean() {
    final ElementPath condition = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.INTEGER)
        .expression("valueInteger")
        .singular(true)
        .build();
    final StringLiteralPath ifTrue = StringLiteralPath.fromString("foo", condition);
    final StringLiteralPath otherwise = StringLiteralPath.fromString("bar", condition);

    final NamedFunctionInput iifInput = new NamedFunctionInput(parserContext, condition,
        Arrays.asList(condition, ifTrue, otherwise));

    final NamedFunction notFunction = NamedFunction.getInstance("iif");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> notFunction.invoke(iifInput));
    assertEquals(
        "Condition argument to iif must be Boolean: valueInteger",
        error.getMessage());
  }

  @Test
  void throwsErrorIfConditionNotSingular() {
    final ElementPath condition = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.BOOLEAN)
        .expression("valueBoolean")
        .singular(false)
        .build();
    final StringLiteralPath ifTrue = StringLiteralPath.fromString("foo", condition);
    final StringLiteralPath otherwise = StringLiteralPath.fromString("bar", condition);

    final NamedFunctionInput iifInput = new NamedFunctionInput(parserContext, condition,
        Arrays.asList(condition, ifTrue, otherwise));

    final NamedFunction notFunction = NamedFunction.getInstance("iif");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> notFunction.invoke(iifInput));
    assertEquals(
        "Condition argument to iif must be singular: valueBoolean",
        error.getMessage());
  }

  @Test
  void throwsErrorIfNoThisInCondition() {
    final ElementPath condition = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.BOOLEAN)
        .expression("valueBoolean")
        .singular(true)
        .build();
    final StringLiteralPath ifTrue = StringLiteralPath.fromString("foo", condition);
    final StringLiteralPath otherwise = StringLiteralPath.fromString("bar", condition);

    final NamedFunctionInput iifInput = new NamedFunctionInput(parserContext, condition,
        Arrays.asList(condition, ifTrue, otherwise));

    final NamedFunction notFunction = NamedFunction.getInstance("iif");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> notFunction.invoke(iifInput));
    assertEquals(
        "Condition argument to iif function must be navigable from collection item (use $this): valueBoolean",
        error.getMessage());
  }

  @Test
  void throwsErrorIfResultArgumentsDifferentTypes() {
    final NonLiteralPath condition = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.BOOLEAN)
        .expression("valueBoolean")
        .singular(true)
        .build()
        .toThisPath();
    final StringLiteralPath ifTrue = StringLiteralPath.fromString("foo", condition);
    final IntegerLiteralPath otherwise = IntegerLiteralPath.fromString("99", condition);

    final NamedFunctionInput iifInput = new NamedFunctionInput(parserContext, condition,
        Arrays.asList(condition, ifTrue, otherwise));

    final NamedFunction notFunction = NamedFunction.getInstance("iif");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> notFunction.invoke(iifInput));
    assertEquals(
        "Paths cannot be merged into a collection together: 'foo', 99",
        error.getMessage());
  }

  @Test
  void throwsErrorIfResultArgumentsAreBackboneElements() {
    final NonLiteralPath condition = new ElementPathBuilder(spark)
        .dataset(parserContext.getInputContext().getDataset())
        .idAndValueColumns()
        .fhirType(FHIRDefinedType.BOOLEAN)
        .expression("valueBoolean")
        .singular(true)
        .build()
        .toThisPath();
    final ElementPath ifTrue = new ElementPathBuilder(spark)
        .dataset(parserContext.getInputContext().getDataset())
        .idAndValueColumns()
        .fhirType(FHIRDefinedType.BACKBONEELEMENT)
        .expression("someBackboneElement")
        .build();
    final ElementPath otherwise = new ElementPathBuilder(spark)
        .dataset(parserContext.getInputContext().getDataset())
        .idAndValueColumns()
        .fhirType(FHIRDefinedType.BACKBONEELEMENT)
        .expression("anotherBackboneElement")
        .build();

    final NamedFunctionInput iifInput = new NamedFunctionInput(parserContext, condition,
        Arrays.asList(condition, ifTrue, otherwise));

    final NamedFunction notFunction = NamedFunction.getInstance("iif");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> notFunction.invoke(iifInput));
    assertEquals(
        "Paths cannot be merged into a collection together: someBackboneElement, "
            + "anotherBackboneElement", error.getMessage());
  }

  @Test
  void throwsErrorWithIncompatibleResourceResults() {
    final ResourcePath inputContext = (ResourcePath) parserContext.getInputContext();
    final NonLiteralPath condition = new ElementPathBuilder(spark)
        .dataset(inputContext.getDataset())
        .idAndValueColumns()
        .fhirType(FHIRDefinedType.BOOLEAN)
        .expression("valueBoolean")
        .singular(true)
        .build()
        .toThisPath();
    final Dataset<Row> resourceDataset = new DatasetBuilder(spark)
        .withContext(inputContext)
        .withIdColumn("id")
        .withColumn("value", DataTypes.StringType)
        .build();
    final ResourcePath otherwise = new ResourcePath("anotherResource", resourceDataset,
        inputContext.getIdColumn(), inputContext.getEidColumn(), inputContext.getValueColumn(),
        inputContext.isSingular(), inputContext.getThisColumn(),
        new ResourceDefinition(ResourceType.CONDITION,
            fhirContext.getResourceDefinition("Condition")), Collections.emptyMap());

    final NamedFunctionInput iifInput = new NamedFunctionInput(parserContext, condition,
        Arrays.asList(condition, inputContext, otherwise));

    final NamedFunction notFunction = NamedFunction.getInstance("iif");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> notFunction.invoke(iifInput));
    assertEquals(
        "Paths cannot be merged into a collection together: Patient, anotherResource",
        error.getMessage());
  }

  @Test
  void throwsErrorWithResourceAndLiteralResults() {
    final ResourcePath inputContext = (ResourcePath) parserContext.getInputContext();
    final NonLiteralPath condition = new ElementPathBuilder(spark)
        .dataset(inputContext.getDataset())
        .idAndValueColumns()
        .fhirType(FHIRDefinedType.BOOLEAN)
        .expression("valueBoolean")
        .singular(true)
        .build()
        .toThisPath();
    final StringLiteralPath otherwise = StringLiteralPath.fromString("foo", condition);

    final NamedFunctionInput iifInput = new NamedFunctionInput(parserContext, condition,
        Arrays.asList(condition, inputContext, otherwise));

    final NamedFunction notFunction = NamedFunction.getInstance("iif");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> notFunction.invoke(iifInput));
    assertEquals(
        "Paths cannot be merged into a collection together: Patient, 'foo'",
        error.getMessage());
  }

  @Test
  void throwsErrorWithUntypedResourceAndLiteralResults() {
    final NonLiteralPath inputContext = new ElementPathBuilder(spark)
        .dataset(parserContext.getInputContext().getDataset())
        .idAndValueColumns()
        .fhirType(FHIRDefinedType.BOOLEAN)
        .expression("valueBoolean")
        .singular(true)
        .build()
        .toThisPath();

    final Dataset<Row> dataset = join(parserContext.getInputContext().getDataset(),
        parserContext.getInputContext().getIdColumn(), new DatasetBuilder(spark)
            .withIdColumn("id")
            .withColumn("value", DataTypes.StringType)
            .withColumn("type", DataTypes.StringType)
            .build(), col("id"), JoinType.LEFT_OUTER);

    final UntypedResourcePath ifTrue = new UntypedResourcePath("someUntypedResource", dataset,
        col("id"), Optional.empty(), col("value"), true, Optional.empty(), col("value"));
    final StringLiteralPath otherwise = StringLiteralPath.fromString("foo", inputContext);

    parserContext = new ParserContextBuilder(spark, fhirContext)
        .inputContext(inputContext)
        .build();

    final NamedFunctionInput iifInput = new NamedFunctionInput(parserContext, inputContext,
        Arrays.asList(inputContext, ifTrue, otherwise));

    final NamedFunction notFunction = NamedFunction.getInstance("iif");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> notFunction.invoke(iifInput));
    assertEquals(
        "Paths cannot be merged into a collection together: someUntypedResource, 'foo'",
        error.getMessage());
  }

  @Test
  void throwsErrorWithElementAndResourceResults() {
    final FhirPath inputContext = parserContext.getInputContext();
    final NonLiteralPath condition = new ElementPathBuilder(spark)
        .dataset(inputContext.getDataset())
        .idAndValueColumns()
        .fhirType(FHIRDefinedType.BOOLEAN)
        .expression("valueBoolean")
        .singular(true)
        .build()
        .toThisPath();
    final ElementPath ifTrue = new ElementPathBuilder(spark)
        .dataset(inputContext.getDataset())
        .idAndValueColumns()
        .fhirType(FHIRDefinedType.STRING)
        .expression("someString")
        .build();

    final NamedFunctionInput iifInput = new NamedFunctionInput(parserContext, condition,
        Arrays.asList(condition, ifTrue, inputContext));

    final NamedFunction notFunction = NamedFunction.getInstance("iif");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> notFunction.invoke(iifInput));
    assertEquals(
        "Paths cannot be merged into a collection together: someString, Patient",
        error.getMessage());
  }

}
