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

package au.csiro.pathling.fhirpath.function.terminology;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.builders.DatasetBuilder.makeEid;
import static au.csiro.pathling.test.helpers.SparkHelpers.codingStructType;
import static au.csiro.pathling.test.helpers.SparkHelpers.rowFromCoding;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.CodingPath;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.function.NamedFunction;
import au.csiro.pathling.fhirpath.function.NamedFunctionInput;
import au.csiro.pathling.fhirpath.literal.IntegerLiteralPath;
import au.csiro.pathling.fhirpath.literal.StringLiteral;
import au.csiro.pathling.fhirpath.literal.StringLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.AbstractTerminologyTestBase;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import au.csiro.pathling.test.helpers.FhirHelpers;
import au.csiro.pathling.test.helpers.TerminologyServiceHelpers;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author Piotr Szul
 */
@SpringBootUnitTest
class PropertyFunctionTest extends AbstractTerminologyTestBase {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirContext fhirContext;

  @Autowired
  TerminologyServiceFactory terminologyServiceFactory;

  @Autowired
  TerminologyService terminologyService;

  @BeforeEach
  void setUp() {
    reset(terminologyService);
  }

  private void checkPropertyOfCoding(final String propertyCode,
      final Optional<String> maybePropertyType,
      final Optional<String> maybeLanguage,
      final List<Type> propertyAFhirValues, final List<Type> propertyBFhirValues,
      final FHIRDefinedType expectedResultType,
      final Dataset<Row> expectedResult) {

    // check the that if type is provided if language is provided
    assertTrue(maybeLanguage.isEmpty() || maybePropertyType.isPresent());

    TerminologyServiceHelpers.setupLookup(terminologyService)
        .withProperty(CODING_A, "propertyA", maybeLanguage.orElse(null), propertyAFhirValues)
        .withProperty(CODING_B, "propertyB", maybeLanguage.orElse(null), propertyBFhirValues);

    final Optional<ElementDefinition> optionalDefinition = FhirHelpers
        .getChildOfResource(fhirContext, "Encounter", "class");
    assertTrue(optionalDefinition.isPresent());
    final ElementDefinition definition = optionalDefinition.get();

    final Dataset<Row> inputDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withStructTypeColumns(codingStructType())
        .withRow("encounter-1", makeEid(0), rowFromCoding(CODING_A))
        .withRow("encounter-1", makeEid(1), rowFromCoding(INVALID_CODING_0))
        .withRow("encounter-2", makeEid(0), rowFromCoding(CODING_B))
        .withRow("encounter-3", null, null)
        .buildWithStructValue();

    final CodingPath inputExpression = (CodingPath) new ElementPathBuilder(spark)
        .dataset(inputDataset)
        .idAndEidAndValueColumns()
        .expression("Encounter.class")
        .singular(false)
        .definition(definition)
        .buildDefined();

    // Prepare the inputs to the function.
    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .idColumn(inputExpression.getIdColumn())
        .terminologyClientFactory(terminologyServiceFactory)
        .build();

    final List<String> parameterLiterals = Stream.of(Optional.of(propertyCode),
            maybePropertyType,
            maybeLanguage).flatMap(Optional::stream)
        .map(StringLiteral::toLiteral).toList();

    final List<FhirPath> arguments = parameterLiterals.stream()
        .map(lit -> StringLiteralPath.fromString(lit, inputExpression))
        .collect(Collectors.toUnmodifiableList());

    final NamedFunctionInput propertyInput = new NamedFunctionInput(parserContext, inputExpression,
        arguments);

    // Invoke the function.
    final FhirPath result = NamedFunction.getInstance("property").invoke(propertyInput);

    // Check the result.
    assertThat(result).hasExpression(
            String.format("Encounter.class.property(%s)",
                String.join(", ", parameterLiterals)))
        .isElementPath(ElementPath.class)
        .hasFhirType(expectedResultType)
        .isNotSingular()
        .selectOrderedResultWithEid()
        .hasRows(expectedResult);

    // Check that the ElementPath for CODING type has the correct Coding definition.
    if (FHIRDefinedType.CODING.equals(expectedResultType)) {
      assertThat(result)
          .isElementPath(ElementPath.class)
          .hasDefinition(definition);
    }
  }

  @SuppressWarnings("unused")
  @ParameterizedTest
  @MethodSource("propertyParameters")
  public void propertyAOfCoding(final String propertyType, final DataType resultDataType,
      final List<Type> propertyAFhirValues, final List<Type> propertyBFhirValues,
      final List<Object> propertyASqlValues, final List<Object> propertyBSqlValues) {

    final Dataset<Row> expectedResult = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withColumn(resultDataType)
        .withRow("encounter-1", makeEid(0, 0), propertyASqlValues.get(0))
        .withRow("encounter-1", makeEid(1, 0), null)
        .withRow("encounter-2", makeEid(0, 0), null)
        .withRow("encounter-3", null, null)
        .build();
    checkPropertyOfCoding("propertyA",
        Optional.of(propertyType),
        Optional.empty(),
        propertyAFhirValues, propertyBFhirValues,
        FHIRDefinedType.fromCode(propertyType),
        expectedResult);
  }

  @SuppressWarnings("unused")
  @ParameterizedTest
  @MethodSource("propertyParameters")
  public void propertyBOfCoding(final String propertyType, final DataType resultDataType,
      final List<Type> propertyAFhirValues, final List<Type> propertyBFhirValues,
      final List<Object> propertyASqlValues, final List<Object> propertyBSqlValues) {

    final Dataset<Row> expectedResult = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withColumn(resultDataType)
        .withRow("encounter-1", makeEid(0, 0), null)
        .withRow("encounter-1", makeEid(1, 0), null)
        .withRow("encounter-2", makeEid(0, 0), propertyBSqlValues.get(0))
        .withRow("encounter-2", makeEid(0, 1), propertyBSqlValues.get(1))
        .withRow("encounter-3", null, null)
        .build();

    checkPropertyOfCoding("propertyB",
        Optional.of(propertyType),
        Optional.empty(),
        propertyAFhirValues,
        propertyBFhirValues,
        FHIRDefinedType.fromCode(propertyType),
        expectedResult);
  }

  @Test
  public void propertyAOfCodingWithDefaultType() {
    final Dataset<Row> expectedResult = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.StringType)
        .withRow("encounter-1", makeEid(0, 0), "value_1")
        .withRow("encounter-1", makeEid(0, 1), "value_2")
        .withRow("encounter-1", makeEid(1, 0), null)
        .withRow("encounter-2", makeEid(0, 0), null)
        .withRow("encounter-3", null, null)
        .build();

    checkPropertyOfCoding("propertyA",
        Optional.empty(),
        Optional.empty(),
        List.of(new StringType("value_1"), new StringType("value_2")),
        Collections.emptyList(),
        FHIRDefinedType.STRING,
        expectedResult);
  }


  @SuppressWarnings("unused")
  @ParameterizedTest
  @MethodSource("propertyParameters")
  public void propertyBOfCodingLanguage(final String propertyType, final DataType resultDataType,
      final List<Type> propertyAFhirValues, final List<Type> propertyBFhirValues,
      final List<Object> propertyASqlValues, final List<Object> propertyBSqlValues) {

    final Dataset<Row> expectedResult = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withColumn(resultDataType)
        .withRow("encounter-1", makeEid(0, 0), null)
        .withRow("encounter-1", makeEid(1, 0), null)
        .withRow("encounter-2", makeEid(0, 0), propertyBSqlValues.get(0))
        .withRow("encounter-2", makeEid(0, 1), propertyBSqlValues.get(1))
        .withRow("encounter-3", null, null)
        .build();

    checkPropertyOfCoding("propertyB",
        Optional.of(propertyType),
        Optional.of("de"),
        propertyAFhirValues,
        propertyBFhirValues,
        FHIRDefinedType.fromCode(propertyType),
        expectedResult);
  }

  @Test
  void throwsErrorIfInputTypeIsUnsupported() {
    final FhirPath mockContext = new ElementPathBuilder(spark).build();
    final ElementPath input = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.STRING)
        .expression("name.given")
        .build();
    final FhirPath argument = StringLiteralPath.fromString("some-property", mockContext);

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .terminologyClientFactory(mock(TerminologyServiceFactory.class))
        .build();

    final NamedFunctionInput propertyInput = new NamedFunctionInput(parserContext, input,
        Collections.singletonList(argument));

    final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
        () -> new PropertyFunction().invoke(propertyInput));
    assertEquals("Input to property function must be Coding but is: name.given",
        error.getMessage());
  }

  void assertThrowsErrorForArguments(@Nonnull final String expectedError,
      @Nonnull final Function<ElementPath, List<FhirPath>> argsFactory) {

    final Optional<ElementDefinition> optionalDefinition = FhirHelpers
        .getChildOfResource(fhirContext, "Encounter", "class");
    assertTrue(optionalDefinition.isPresent());
    final ElementDefinition definition = optionalDefinition.get();

    final ElementPath input = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.CODING)
        .definition(definition)
        .buildDefined();

    final ParserContext context = new ParserContextBuilder(spark, fhirContext)
        .terminologyClientFactory(mock(TerminologyServiceFactory.class))
        .build();

    final NamedFunctionInput propertyInput = new NamedFunctionInput(context, input,
        argsFactory.apply(input));

    final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
        () -> new PropertyFunction().invoke(propertyInput));
    assertEquals(expectedError,
        error.getMessage());

  }

  @Test
  void throwsErrorIfNoArguments() {
    assertThrowsErrorForArguments(
        "property function accepts one required and one optional arguments",
        input -> Collections.emptyList());
  }

  @Test
  void throwsErrorIfFirstArgumentIsNotString() {
    assertThrowsErrorForArguments("Function `property` expects `String literal` as argument 1",
        input -> Collections.singletonList(
            IntegerLiteralPath.fromString("4", input)));
  }

  @Test
  void throwsErrorIfSecondArgumentIsNotBoolean() {
    assertThrowsErrorForArguments("Function `property` expects `String literal` as argument 2",
        input -> Arrays.asList(
            StringLiteralPath.fromString("'foo'", input),
            IntegerLiteralPath.fromString("5", input)));
  }

  @Test
  void throwsErrorIfThirdArgumentIsNotBoolean() {
    assertThrowsErrorForArguments("Function `property` expects `String literal` as argument 3",
        input -> Arrays.asList(
            StringLiteralPath.fromString("'foo'", input),
            StringLiteralPath.fromString("'foo'", input),
            IntegerLiteralPath.fromString("5", input)));
  }

  @Test
  void throwsErrorIfTooManyArguments() {
    assertThrowsErrorForArguments(
        "property function accepts one required and one optional arguments",
        input -> Arrays.asList(
            StringLiteralPath.fromString("'foo'", input),
            StringLiteralPath.fromString("'false'", input),
            StringLiteralPath.fromString("'false'", input),
            StringLiteralPath.fromString("'false'", input)
        ));
  }

  @Test
  void throwsErrorIfCannotParsePropertyType() {
    assertThrowsErrorForArguments(
        "Unknown FHIRDefinedType code 'not-an-fhir-type'",
        input -> Arrays.asList(
            StringLiteralPath.fromString("'foo'", input),
            StringLiteralPath.fromString("'not-an-fhir-type'", input)
        ));
  }

  @Test
  void throwsErrorIfTerminologyServiceNotConfigured() {
    final ElementPath input = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.CODING)
        .build();
    final FhirPath argument = StringLiteralPath.fromString("some string", input);

    final ParserContext context = new ParserContextBuilder(spark, fhirContext)
        .build();

    final NamedFunctionInput translateInput = new NamedFunctionInput(context, input,
        Collections.singletonList(argument));

    final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
        () -> new TranslateFunction().invoke(translateInput));
    assertEquals(
        "Attempt to call terminology function translate when terminology service has not been configured",
        error.getMessage());
  }
}
