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

import static au.csiro.pathling.test.AbstractTerminologyTestBase.INVALID_CODING_0;
import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.builders.DatasetBuilder.makeEid;
import static au.csiro.pathling.test.helpers.SparkHelpers.codingStructType;
import static au.csiro.pathling.test.helpers.SparkHelpers.rowFromCoding;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.CD_SNOMED_VER_63816008;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.LC_55915_3;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.CodingPath;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.literal.IntegerLiteralPath;
import au.csiro.pathling.fhirpath.literal.StringLiteralPath;
import au.csiro.pathling.fhirpath.literal.TimeLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import au.csiro.pathling.test.helpers.FhirHelpers;
import au.csiro.pathling.test.helpers.TerminologyServiceHelpers;
import ca.uhn.fhir.context.FhirContext;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * @author Piotr Szul
 */
@SpringBootTest
@Tag("UnitTest")
class DisplayFunctionTest {

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
    SharedMocks.resetAll();
  }

  @Test
  public void displayCoding() {

    final Optional<ElementDefinition> optionalDefinition = FhirHelpers
        .getChildOfResource(fhirContext, "Encounter", "class");
    assertTrue(optionalDefinition.isPresent());
    final ElementDefinition definition = optionalDefinition.get();

    final Dataset<Row> inputDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withStructTypeColumns(codingStructType())
        .withRow("encounter-1", makeEid(0), rowFromCoding(LC_55915_3))
        .withRow("encounter-1", makeEid(1), rowFromCoding(INVALID_CODING_0))
        .withRow("encounter-2", makeEid(0), rowFromCoding(CD_SNOMED_VER_63816008))
        .withRow("encounter-3", null, null)
        .buildWithStructValue();

    final CodingPath inputExpression = (CodingPath) new ElementPathBuilder(spark)
        .dataset(inputDataset)
        .idAndEidAndValueColumns()
        .expression("Encounter.class")
        .singular(false)
        .definition(definition)
        .buildDefined();

    // Setup mocks
    TerminologyServiceHelpers.setupLookup(terminologyService)
        .withDisplay(LC_55915_3)
        .withDisplay(CD_SNOMED_VER_63816008);

    // Prepare the inputs to the function.
    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .idColumn(inputExpression.getIdColumn())
        .terminologyClientFactory(terminologyServiceFactory)
        .build();

    final NamedFunctionInput displayInput = new NamedFunctionInput(parserContext, inputExpression,
        Collections.emptyList());

    // Invoke the function.
    final FhirPath result = new DisplayFunction().invoke(displayInput);

    final Dataset<Row> expectedResult = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.StringType)
        .withRow("encounter-1", makeEid(0), LC_55915_3.getDisplay())
        .withRow("encounter-1", makeEid(1), null)
        .withRow("encounter-2", makeEid(0), CD_SNOMED_VER_63816008.getDisplay())
        .withRow("encounter-3", null, null)
        .build();

    // Check the result.
    assertThat(result)
        .hasExpression("Encounter.class.display()")
        .isElementPath(ElementPath.class)
        .hasFhirType(FHIRDefinedType.STRING)
        .isNotSingular()
        .selectOrderedResultWithEid()
        .hasRows(expectedResult);
  }



  @Test
  public void displayCodingLanguage() {

    final Optional<ElementDefinition> optionalDefinition = FhirHelpers
        .getChildOfResource(fhirContext, "Encounter", "class");
    assertTrue(optionalDefinition.isPresent());
    final ElementDefinition definition = optionalDefinition.get();

    final Dataset<Row> inputDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withStructTypeColumns(codingStructType())
        .withRow("encounter-1", makeEid(0), rowFromCoding(LC_55915_3))
        .withRow("encounter-1", makeEid(1), rowFromCoding(INVALID_CODING_0))
        .withRow("encounter-2", makeEid(0), rowFromCoding(CD_SNOMED_VER_63816008))
        .withRow("encounter-3", null, null)
        .buildWithStructValue();

    final CodingPath inputExpression = (CodingPath) new ElementPathBuilder(spark)
        .dataset(inputDataset)
        .idAndEidAndValueColumns()
        .expression("Encounter.class")
        .singular(false)
        .definition(definition)
        .buildDefined();

    // Setup mocks
    TerminologyServiceHelpers.setupLookup(terminologyService)
        .withDisplayLanguage(LC_55915_3, LC_55915_3.getDisplay(), "de")
        .withDisplayLanguage(CD_SNOMED_VER_63816008, CD_SNOMED_VER_63816008.getDisplay(), "de");

    // Prepare the inputs to the function.
    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .idColumn(inputExpression.getIdColumn())
        .terminologyClientFactory(terminologyServiceFactory)
        .build();

    final StringLiteralPath argumentExpression = StringLiteralPath
        .fromString("'de'", inputExpression);

    final NamedFunctionInput displayInput = new NamedFunctionInput(parserContext, inputExpression,
        Collections.singletonList(argumentExpression));

    // Invoke the function.
    final FhirPath result = new DisplayFunction().invoke(displayInput);

    final Dataset<Row> expectedResult = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.StringType)
        .withRow("encounter-1", makeEid(0), LC_55915_3.getDisplay())
        .withRow("encounter-1", makeEid(1), null)
        .withRow("encounter-2", makeEid(0), CD_SNOMED_VER_63816008.getDisplay())
        .withRow("encounter-3", null, null)
        .build();

    // Check the result.
    assertThat(result)
        .hasExpression("Encounter.class.display('de')")
        .isElementPath(ElementPath.class)
        .hasFhirType(FHIRDefinedType.STRING)
        .isNotSingular()
        .selectOrderedResultWithEid()
        .hasRows(expectedResult);
  }

  @Test
  void throwsErrorIfTerminologyServiceNotConfigured() {
    final ElementPath input = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.CODING)
        .build();

    final ParserContext context = new ParserContextBuilder(spark, fhirContext)
        .build();

    final NamedFunctionInput displayInput = new NamedFunctionInput(context, input,
        Collections.emptyList());

    final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
        () -> new DisplayFunction().invoke(displayInput));
    assertEquals(
        "Attempt to call terminology function display when terminology service has not been configured",
        error.getMessage());
  }

  @Test
  void inputMustNotContainTwoArguments() {
    final ElementPath input = new ElementPathBuilder(spark).build();
    final StringLiteralPath argument1 = StringLiteralPath
        .fromString("'some argument'", input);
    final StringLiteralPath argument2 = StringLiteralPath
        .fromString("'some argument'", input);
    List<FhirPath> arguments = new ArrayList<FhirPath>();
    arguments.add(argument1);
    arguments.add(argument2);

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();
    final NamedFunctionInput displayInput = new NamedFunctionInput(parserContext, input,
        arguments);

    final NamedFunction displayFunction = NamedFunction.getInstance("display");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> displayFunction.invoke(displayInput));
    assertEquals(
        "display function accepts one optional language argument",
        error.getMessage());
  }

  @Test
  void inputMustNotContainNonStringArgument() {
    final ElementPath input = new ElementPathBuilder(spark).build();
    final IntegerLiteralPath argument = IntegerLiteralPath
        .fromString("9493948", input);

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();
    final NamedFunctionInput displayInput = new NamedFunctionInput(parserContext, input,
        Collections.singletonList(argument));

    final NamedFunction displayFunction = NamedFunction.getInstance("display");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> displayFunction.invoke(displayInput));
    assertEquals(
        "display function can accept only one optional argument to display, it must be string type",
        error.getMessage());
  }

  @Test
  void throwsErrorIfInputNotCoding() {
    final ElementPath input = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.INTEGER)
        .expression("valueInteger")
        .build();

    final ParserContext parserContext = new ParserContextBuilder(spark,
        fhirContext).terminologyClientFactory(terminologyServiceFactory)
        .build();
    final NamedFunctionInput displayInput = new NamedFunctionInput(parserContext, input,
        Collections.emptyList());

    final NamedFunction displayFunction = NamedFunction.getInstance("display");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> displayFunction.invoke(displayInput));
    assertEquals(
        "Input to display function must be Coding but is: valueInteger",
        error.getMessage());
  }
}
