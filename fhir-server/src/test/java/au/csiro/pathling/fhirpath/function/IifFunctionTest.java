/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.builders.DatasetBuilder.makeEid;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.UntypedResourcePath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.element.IntegerPath;
import au.csiro.pathling.fhirpath.element.StringPath;
import au.csiro.pathling.fhirpath.literal.IntegerLiteralPath;
import au.csiro.pathling.fhirpath.literal.StringLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import au.csiro.pathling.test.builders.ResourceDatasetBuilder;
import au.csiro.pathling.test.builders.ResourcePathBuilder;
import au.csiro.pathling.test.builders.UntypedResourcePathBuilder;
import ca.uhn.fhir.context.FhirContext;
import java.util.Arrays;
import java.util.Collections;
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
  private SparkSession spark;

  @Autowired
  private FhirContext fhirContext;

  private static final String ID_ALIAS = "_abc123";

  private ParserContext parserContext;
  private ResourceReader resourceReader;

  @BeforeEach
  void setUp() {
    parserContext = new ParserContextBuilder(spark, fhirContext).build();
    resourceReader = mock(ResourceReader.class);
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
    when(resourceReader.read(ResourceType.OBSERVATION)).thenReturn(inputContextDataset);
    final ResourcePath inputContext = new ResourcePathBuilder(spark)
        .expression("Observation")
        .resourceType(ResourceType.OBSERVATION)
        .resourceReader(resourceReader)
        .singular(true)
        .build();
    final Dataset<Row> inputDataset = new DatasetBuilder(spark)
        .withIdColumn(ID_ALIAS)
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
        .withIdColumn(ID_ALIAS)
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
        .withIdColumn(ID_ALIAS)
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
        .withIdColumn(ID_ALIAS)
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
        .withIdColumn(ID_ALIAS)
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
        .withIdColumn(ID_ALIAS)
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
    final Dataset<Row> inputContextDataset = new ResourceDatasetBuilder(spark)
        .withIdColumn()
        .withRow("observation-1")
        .withRow("observation-2")
        .withRow("observation-3")
        .withRow("observation-4")
        .withRow("observation-5")
        .build();
    when(resourceReader.read(ResourceType.OBSERVATION)).thenReturn(inputContextDataset);
    final ResourcePath inputContext = new ResourcePathBuilder(spark)
        .expression("Observation")
        .resourceType(ResourceType.OBSERVATION)
        .resourceReader(resourceReader)
        .singular(true)
        .build();
    final Dataset<Row> inputDataset = new DatasetBuilder(spark)
        .withIdColumn(ID_ALIAS)
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
        .withIdColumn(ID_ALIAS)
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
        .withIdColumn(ID_ALIAS)
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
        .withIdColumn(ID_ALIAS)
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
        .fhirType(FHIRDefinedType.BOOLEAN)
        .expression("valueBoolean")
        .singular(true)
        .build()
        .toThisPath();
    final ElementPath ifTrue = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.BACKBONEELEMENT)
        .expression("someBackboneElement")
        .build();
    final ElementPath otherwise = new ElementPathBuilder(spark)
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
    final NonLiteralPath condition = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.BOOLEAN)
        .expression("valueBoolean")
        .singular(true)
        .build()
        .toThisPath();
    final ResourcePath ifTrue = new ResourcePathBuilder(spark)
        .expression("someResource")
        .resourceType(ResourceType.PATIENT)
        .build();
    final ResourcePath otherwise = new ResourcePathBuilder(spark)
        .expression("anotherResource")
        .resourceType(ResourceType.CONDITION)
        .build();

    final NamedFunctionInput iifInput = new NamedFunctionInput(parserContext, condition,
        Arrays.asList(condition, ifTrue, otherwise));

    final NamedFunction notFunction = NamedFunction.getInstance("iif");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> notFunction.invoke(iifInput));
    assertEquals(
        "Paths cannot be merged into a collection together: someResource, anotherResource",
        error.getMessage());
  }

  @Test
  void throwsErrorWithResourceAndLiteralResults() {
    final NonLiteralPath condition = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.BOOLEAN)
        .expression("valueBoolean")
        .singular(true)
        .build()
        .toThisPath();
    final ResourcePath ifTrue = new ResourcePathBuilder(spark)
        .expression("someResource")
        .resourceType(ResourceType.PATIENT)
        .build();
    final StringLiteralPath otherwise = StringLiteralPath.fromString("foo", condition);

    final NamedFunctionInput iifInput = new NamedFunctionInput(parserContext, condition,
        Arrays.asList(condition, ifTrue, otherwise));

    final NamedFunction notFunction = NamedFunction.getInstance("iif");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> notFunction.invoke(iifInput));
    assertEquals(
        "Paths cannot be merged into a collection together: someResource, 'foo'",
        error.getMessage());
  }

  @Test
  void throwsErrorWithUntypedResourceAndLiteralResults() {
    final NonLiteralPath condition = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.BOOLEAN)
        .expression("valueBoolean")
        .singular(true)
        .build()
        .toThisPath();
    final UntypedResourcePath ifTrue = new UntypedResourcePathBuilder(spark)
        .expression("someUntypedResource")
        .build();
    final StringLiteralPath otherwise = StringLiteralPath.fromString("foo", condition);

    final NamedFunctionInput iifInput = new NamedFunctionInput(parserContext, condition,
        Arrays.asList(condition, ifTrue, otherwise));

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
    final NonLiteralPath condition = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.BOOLEAN)
        .expression("valueBoolean")
        .singular(true)
        .build()
        .toThisPath();
    final ElementPath ifTrue = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.STRING)
        .expression("someString")
        .build();
    final ResourcePath otherwise = new ResourcePathBuilder(spark)
        .expression("someResource")
        .resourceType(ResourceType.CONDITION)
        .build();

    final NamedFunctionInput iifInput = new NamedFunctionInput(parserContext, condition,
        Arrays.asList(condition, ifTrue, otherwise));

    final NamedFunction notFunction = NamedFunction.getInstance("iif");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> notFunction.invoke(iifInput));
    assertEquals(
        "Paths cannot be merged into a collection together: someString, someResource",
        error.getMessage());
  }

}
