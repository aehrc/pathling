/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.fhirpath.function;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.builders.DatasetBuilder.makeEid;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.collection.UntypedResourcePath;
import au.csiro.pathling.fhirpath.collection.IntegerCollection;
import au.csiro.pathling.fhirpath.collection.PrimitivePath;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.literal.IntegerLiteralPath;
import au.csiro.pathling.fhirpath.literal.StringLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.test.SpringBootUnitTest;
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
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;

/**
 * @author John Grimes
 */
@SpringBootUnitTest
class IifFunctionTest {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirContext fhirContext;

  @MockBean
  DataSource dataSource;


  static final String ID_ALIAS = "_abc123";

  ParserContext parserContext;

  @BeforeEach
  void setUp() {
    parserContext = new ParserContextBuilder(spark, fhirContext).build();
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
    when(dataSource.read(ResourceType.OBSERVATION)).thenReturn(inputContextDataset);
    final ResourceCollection inputContext = new ResourcePathBuilder(spark)
        .expression("Observation")
        .resourceType(ResourceType.OBSERVATION)
        .database(dataSource)
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
    final PrimitivePath inputPath = new ElementPathBuilder(spark)
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
    final StringLiteralPath ifTrue = StringCollection.fromLiteral("foo", inputContext);
    final StringLiteralPath otherwise = StringCollection.fromLiteral("bar", inputContext);

    final NamedFunctionInput iifInput = new NamedFunctionInput(parserContext, inputPath,
        Arrays.asList(condition, ifTrue, otherwise));
    final Collection result = NamedFunction.getInstance("iif").invoke(iifInput);

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
        .isElementPath(StringCollection.class)
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
    final PrimitivePath inputPath = new ElementPathBuilder(spark)
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
    final PrimitivePath ifTrue = new ElementPathBuilder(spark)
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
    final PrimitivePath otherwise = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.INTEGER)
        .dataset(otherwiseDataset)
        .idAndEidAndValueColumns()
        .expression("anotherInteger")
        .singular(true)
        .build();

    final NamedFunctionInput iifInput = new NamedFunctionInput(parserContext, inputPath,
        Arrays.asList(condition, ifTrue, otherwise));
    final Collection result = NamedFunction.getInstance("iif").invoke(iifInput);

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
        .isElementPath(IntegerCollection.class)
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
    when(dataSource.read(ResourceType.OBSERVATION)).thenReturn(inputContextDataset);
    final ResourceCollection inputContext = new ResourcePathBuilder(spark)
        .expression("Observation")
        .resourceType(ResourceType.OBSERVATION)
        .database(dataSource)
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
    final PrimitivePath inputPath = new ElementPathBuilder(spark)
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
    final PrimitivePath ifTrue = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.INTEGER)
        .dataset(ifTrueDataset)
        .idAndEidAndValueColumns()
        .expression("someInteger")
        .singular(true)
        .build();

    final IntegerLiteralPath otherwise = IntegerLiteralPath.fromString("99", inputContext);

    final NamedFunctionInput iifInput1 = new NamedFunctionInput(parserContext, inputPath,
        Arrays.asList(condition, ifTrue, otherwise));
    final Collection result1 = NamedFunction.getInstance("iif").invoke(iifInput1);

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
        .isElementPath(IntegerCollection.class)
        .selectOrderedResultWithEid()
        .hasRows(expectedDataset1);

    final NamedFunctionInput iifInput2 = new NamedFunctionInput(parserContext, inputPath,
        Arrays.asList(condition, otherwise, ifTrue));
    final Collection result2 = NamedFunction.getInstance("iif").invoke(iifInput2);

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
        .isElementPath(IntegerCollection.class)
        .selectOrderedResultWithEid()
        .hasRows(expectedDataset2);
  }

  @Test
  void throwsErrorIfConditionNotBoolean() {
    final PrimitivePath condition = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.INTEGER)
        .expression("valueInteger")
        .singular(true)
        .build();
    final StringLiteralPath ifTrue = StringCollection.fromLiteral("foo", condition);
    final StringLiteralPath otherwise = StringCollection.fromLiteral("bar", condition);

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
    final PrimitivePath condition = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.BOOLEAN)
        .expression("valueBoolean")
        .singular(false)
        .build();
    final StringLiteralPath ifTrue = StringCollection.fromLiteral("foo", condition);
    final StringLiteralPath otherwise = StringCollection.fromLiteral("bar", condition);

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
    final PrimitivePath condition = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.BOOLEAN)
        .expression("valueBoolean")
        .singular(true)
        .build();
    final StringLiteralPath ifTrue = StringCollection.fromLiteral("foo", condition);
    final StringLiteralPath otherwise = StringCollection.fromLiteral("bar", condition);

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
    final StringLiteralPath ifTrue = StringCollection.fromLiteral("foo", condition);
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
    final PrimitivePath ifTrue = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.BACKBONEELEMENT)
        .expression("someBackboneElement")
        .build();
    final PrimitivePath otherwise = new ElementPathBuilder(spark)
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
    final ResourceCollection ifTrue = new ResourcePathBuilder(spark)
        .expression("someResource")
        .resourceType(ResourceType.PATIENT)
        .build();
    final ResourceCollection otherwise = new ResourcePathBuilder(spark)
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
    final ResourceCollection ifTrue = new ResourcePathBuilder(spark)
        .expression("someResource")
        .resourceType(ResourceType.PATIENT)
        .build();
    final StringLiteralPath otherwise = StringCollection.fromLiteral("foo", condition);

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
    final StringLiteralPath otherwise = StringCollection.fromLiteral("foo", condition);

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
    final PrimitivePath ifTrue = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.STRING)
        .expression("someString")
        .build();
    final ResourceCollection otherwise = new ResourcePathBuilder(spark)
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
