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

import au.csiro.pathling.fhirpath.annotations.NotImplemented;
import au.csiro.pathling.test.SpringBootUnitTest;

/**
 * @author John Grimes
 */
@SpringBootUnitTest
@NotImplemented
class EmptyFunctionTest {

  // TODO: implement with columns

  // @Autowired
  // SparkSession spark;
  //
  // @Autowired
  // FhirContext fhirContext;
  //
  // @Test
  // void returnsCorrectResults() {
  //   final Coding coding1 = new Coding(TestHelpers.SNOMED_URL, "840546002", "Exposure to COVID-19");
  //   final CodeableConcept concept1 = new CodeableConcept(coding1);
  //   final Coding coding2 = new Coding(TestHelpers.SNOMED_URL, "248427009", "Fever symptoms");
  //   final CodeableConcept concept2 = new CodeableConcept(coding2);
  //
  //   final Dataset<Row> dataset = new DatasetBuilder(spark)
  //       .withIdColumn()
  //       .withColumn(codeableConceptStructType())
  //       .withRow("observation-1", null)
  //       .withRow("observation-2", null)
  //       .withRow("observation-2", null)
  //       .withRow("observation-3", rowFromCodeableConcept(concept1))
  //       .withRow("observation-4", rowFromCodeableConcept(concept1))
  //       .withRow("observation-4", null)
  //       .withRow("observation-5", rowFromCodeableConcept(concept1))
  //       .withRow("observation-5", rowFromCodeableConcept(concept2))
  //       .build();
  //   final PrimitivePath input = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.CODEABLECONCEPT)
  //       .dataset(dataset)
  //       .idAndValueColumns()
  //       .expression("code")
  //       .build();
  //   final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
  //       .groupingColumns(Collections.singletonList(input.getIdColumn()))
  //       .build();
  //
  //   // Set up the function input.
  //   final NamedFunctionInput emptyInput = new NamedFunctionInput(parserContext, input,
  //       Collections.emptyList());
  //
  //   // Invoke the function.
  //   final NamedFunction emptyFunction = NamedFunction.getInstance("empty");
  //   final Collection result = emptyFunction.invoke(emptyInput);
  //
  //   // Check the result.
  //   final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
  //       .withIdColumn()
  //       .withColumn(DataTypes.BooleanType)
  //       .withRow("observation-1", true)
  //       .withRow("observation-2", true)
  //       .withRow("observation-3", false)
  //       .withRow("observation-4", false)
  //       .withRow("observation-5", false)
  //       .build();
  //   assertThat(result)
  //       .hasExpression("code.empty()")
  //       .isSingular()
  //       .isElementPath(BooleanCollection.class)
  //       .selectOrderedResult()
  //       .hasRows(expectedDataset);
  // }
  //
  // @Test
  // void inputMustNotContainArguments() {
  //   final PrimitivePath input = new ElementPathBuilder(spark).build();
  //   final StringLiteralPath argument = StringCollection
  //       .fromLiteral("'some argument'", input);
  //
  //   final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();
  //   final NamedFunctionInput emptyInput = new NamedFunctionInput(parserContext, input,
  //       Collections.singletonList(argument));
  //
  //   final NamedFunction emptyFunction = NamedFunction.getInstance("empty");
  //   final InvalidUserInputError error = assertThrows(
  //       InvalidUserInputError.class,
  //       () -> emptyFunction.invoke(emptyInput));
  //   assertEquals(
  //       "Arguments can not be passed to empty function",
  //       error.getMessage());
  // }

}
