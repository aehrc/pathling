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
class NotFunctionTest {

  // TODO: implement with columns

  //
  // @Autowired
  // SparkSession spark;
  //
  // @Autowired
  // FhirContext fhirContext;
  //
  // @Test
  // void returnsCorrectResults() {
  //   final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();
  //   final Dataset<Row> dataset = new DatasetBuilder(spark)
  //       .withIdColumn()
  //       .withEidColumn()
  //       .withColumn(DataTypes.BooleanType)
  //       .withRow("observation-1", makeEid(0), true)
  //       .withRow("observation-2", makeEid(0), false)
  //       .withRow("observation-3", makeEid(0), null)
  //       .withRow("observation-4", makeEid(0), true)
  //       .withRow("observation-4", makeEid(1), false)
  //       .withRow("observation-5", makeEid(0), null)
  //       .withRow("observation-5", makeEid(1), null)
  //       .build();
  //   final PrimitivePath input = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.BOOLEAN)
  //       .dataset(dataset)
  //       .idAndEidAndValueColumns()
  //       .expression("valueBoolean")
  //       .singular(false)
  //       .build();
  //
  //   final NamedFunctionInput notInput = new NamedFunctionInput(parserContext, input,
  //       Collections.emptyList());
  //   final NamedFunction notFunction = NamedFunction.getInstance("not");
  //   final Collection result = notFunction.invoke(notInput);
  //
  //   final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
  //       .withIdColumn()
  //       .withColumn(DataTypes.BooleanType)
  //       .withRow("observation-1", false)
  //       .withRow("observation-2", true)
  //       .withRow("observation-3", null)
  //       .withRow("observation-4", false)
  //       .withRow("observation-4", true)
  //       .withRow("observation-5", null)
  //       .withRow("observation-5", null)
  //       .build();
  //   assertThat(result)
  //       .hasExpression("valueBoolean.not()")
  //       .isNotSingular()
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
  //   final NamedFunctionInput notInput = new NamedFunctionInput(parserContext, input,
  //       Collections.singletonList(argument));
  //
  //   final NamedFunction notFunction = NamedFunction.getInstance("not");
  //   final InvalidUserInputError error = assertThrows(
  //       InvalidUserInputError.class,
  //       () -> notFunction.invoke(notInput));
  //   assertEquals(
  //       "Arguments can not be passed to not function",
  //       error.getMessage());
  // }
  //
  // @Test
  // void throwsErrorIfInputNotBoolean() {
  //   final PrimitivePath input = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.INTEGER)
  //       .expression("valueInteger")
  //       .build();
  //
  //   final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext).build();
  //   final NamedFunctionInput notInput = new NamedFunctionInput(parserContext, input,
  //       Collections.emptyList());
  //
  //   final NamedFunction notFunction = NamedFunction.getInstance("not");
  //   final InvalidUserInputError error = assertThrows(
  //       InvalidUserInputError.class,
  //       () -> notFunction.invoke(notInput));
  //   assertEquals(
  //       "Input to not function must be Boolean: valueInteger",
  //       error.getMessage());
  // }
  //
}