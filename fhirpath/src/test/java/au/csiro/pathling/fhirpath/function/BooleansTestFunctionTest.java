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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.BooleanPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.literal.StringLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.Collections;
import java.util.stream.Stream;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * These functions perform tests across a collection of Boolean input values.
 *
 * @author John Grimes
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#anytrue">anyTrue</a>
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#anyfalse">anyFalse</a>
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#alltrue">allTrue</a>
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#allfalse">allFalse</a>
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class BooleansTestFunctionTest {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirContext fhirContext;

  @Value
  static class TestParameters {

    @Nonnull
    String functionName;

    @Nonnull
    Dataset<Row> expectedResult;

    @Override
    public String toString() {
      return functionName;
    }

  }

  Stream<TestParameters> parameters() {
    return Stream.of(
        new TestParameters("anyTrue", new DatasetBuilder(spark)
            .withIdColumn()
            .withColumn(DataTypes.BooleanType)
            .withRow("observation-1", true)
            .withRow("observation-2", true)
            .withRow("observation-3", false)
            .withRow("observation-4", true)
            .withRow("observation-5", false)
            .withRow("observation-6", false)
            .withRow("observation-7", false)
            .build()),
        new TestParameters("anyFalse", new DatasetBuilder(spark)
            .withIdColumn()
            .withColumn(DataTypes.BooleanType)
            .withRow("observation-1", true)
            .withRow("observation-2", false)
            .withRow("observation-3", true)
            .withRow("observation-4", false)
            .withRow("observation-5", true)
            .withRow("observation-6", false)
            .withRow("observation-7", false)
            .build()),
        new TestParameters("allTrue", new DatasetBuilder(spark)
            .withIdColumn()
            .withColumn(DataTypes.BooleanType)
            .withRow("observation-1", false)
            .withRow("observation-2", true)
            .withRow("observation-3", false)
            .withRow("observation-4", true)
            .withRow("observation-5", false)
            .withRow("observation-6", true)
            .withRow("observation-7", true)
            .build()),
        new TestParameters("allFalse", new DatasetBuilder(spark)
            .withIdColumn()
            .withColumn(DataTypes.BooleanType)
            .withRow("observation-1", false)
            .withRow("observation-2", false)
            .withRow("observation-3", true)
            .withRow("observation-4", false)
            .withRow("observation-5", true)
            .withRow("observation-6", true)
            .withRow("observation-7", true)
            .build())
    );
  }

  @ParameterizedTest
  @MethodSource("parameters")
  void returnsCorrectResults(@Nonnull final TestParameters parameters) {
    final Dataset<Row> dataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withColumn(DataTypes.BooleanType)
        .withRow("observation-1", true)
        .withRow("observation-1", false)
        .withRow("observation-2", true)
        .withRow("observation-2", true)
        .withRow("observation-3", false)
        .withRow("observation-3", false)
        .withRow("observation-4", true)
        .withRow("observation-4", null)
        .withRow("observation-5", false)
        .withRow("observation-5", null)
        .withRow("observation-6", null)
        .withRow("observation-6", null)
        .withRow("observation-7", null)
        .build();
    final ElementPath input = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.BOOLEAN)
        .dataset(dataset)
        .idAndValueColumns()
        .expression("valueBoolean")
        .build();
    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .groupingColumns(Collections.singletonList(input.getIdColumn()))
        .build();

    final NamedFunctionInput functionInput = new NamedFunctionInput(parserContext, input,
        Collections.emptyList());
    final NamedFunction function = NamedFunction.getInstance(parameters.getFunctionName());
    final FhirPath result = function.invoke(functionInput);

    assertThat(result)
        .hasExpression("valueBoolean." + parameters.getFunctionName() + "()")
        .isSingular()
        .isElementPath(BooleanPath.class)
        .selectOrderedResult()
        .hasRows(parameters.getExpectedResult());
  }

  @ParameterizedTest
  @MethodSource("parameters")
  void inputMustNotContainArguments(@Nonnull final TestParameters parameters) {
    final ElementPath input = new ElementPathBuilder(spark).build();
    final StringLiteralPath argument = StringLiteralPath
        .fromString("'some argument'", input);

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();
    final NamedFunctionInput functionInput = new NamedFunctionInput(parserContext, input,
        Collections.singletonList(argument));
    final NamedFunction function = NamedFunction.getInstance(parameters.getFunctionName());
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> function.invoke(functionInput));
    assertEquals(
        "Arguments can not be passed to " + parameters.getFunctionName() + " function",
        error.getMessage());
  }

  @ParameterizedTest
  @MethodSource("parameters")
  void inputMustBeBoolean(@Nonnull final TestParameters parameters) {
    final ElementPath input = new ElementPathBuilder(spark)
        .expression("valueString")
        .fhirType(FHIRDefinedType.STRING)
        .build();

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();
    final NamedFunctionInput functionInput = new NamedFunctionInput(parserContext, input,
        Collections.emptyList());
    final NamedFunction function = NamedFunction.getInstance(parameters.getFunctionName());
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> function.invoke(functionInput));
    assertEquals(
        "Input to " + parameters.getFunctionName() + " function must be Boolean",
        error.getMessage());
  }

}
