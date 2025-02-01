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

package au.csiro.pathling.fhirpath.operator;

import au.csiro.pathling.fhirpath.annotations.NotImplemented;
import au.csiro.pathling.test.SpringBootUnitTest;

/**
 * @author John Grimes
 */
@SpringBootUnitTest
@NotImplemented
class BooleanOperatorValidationTest {

  // TODO: implement with columns
  //
  //
  // @Autowired
  // SparkSession spark;
  //
  // @Autowired
  // FhirContext fhirContext;
  //
  // ParserContext parserContext;
  //
  // @BeforeEach
  // void setUp() {
  //   parserContext = new ParserContextBuilder(spark, fhirContext).build();
  // }
  //
  // @Test
  // void operandIsNotSingular() {
  //   final PrimitivePath left = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.BOOLEAN)
  //       .singular(false)
  //       .expression("estimatedAge")
  //       .build();
  //   final PrimitivePath right = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.BOOLEAN)
  //       .singular(true)
  //       .expression("deceasedBoolean")
  //       .build();
  //
  //   final BinaryOperatorInput input = new BinaryOperatorInput(parserContext, left, right);
  //
  //   final BinaryOperator booleanOperator = BinaryOperator.getInstance("and");
  //   final InvalidUserInputError error = assertThrows(
  //       InvalidUserInputError.class,
  //       () -> booleanOperator.invoke(input));
  //   assertEquals(
  //       "Left operand to and operator must be singular: estimatedAge",
  //       error.getMessage());
  //
  //   // Now test the right operand.
  //   final BinaryOperatorInput reversedInput = new BinaryOperatorInput(parserContext, right, left);
  //   final InvalidUserInputError reversedError = assertThrows(
  //       InvalidUserInputError.class,
  //       () -> booleanOperator.invoke(reversedInput));
  //   assertEquals(
  //       "Right operand to and operator must be singular: estimatedAge",
  //       reversedError.getMessage());
  // }
  //
  // @Test
  // void operandIsNotBoolean() {
  //   final PrimitivePath left = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.STRING)
  //       .singular(true)
  //       .expression("estimatedAge")
  //       .build();
  //   final PrimitivePath right = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.BOOLEAN)
  //       .singular(true)
  //       .expression("deceasedBoolean")
  //       .build();
  //
  //   final BinaryOperatorInput input = new BinaryOperatorInput(parserContext, left, right);
  //
  //   final BinaryOperator booleanOperator = BinaryOperator.getInstance("and");
  //   final InvalidUserInputError error = assertThrows(
  //       InvalidUserInputError.class,
  //       () -> booleanOperator.invoke(input));
  //   assertEquals(
  //       "Left operand to and operator must be Boolean: estimatedAge",
  //       error.getMessage());
  //
  //   // Now test the right operand.
  //   final BinaryOperatorInput reversedInput = new BinaryOperatorInput(parserContext, right, left);
  //   final InvalidUserInputError reversedError = assertThrows(
  //       InvalidUserInputError.class,
  //       () -> booleanOperator.invoke(reversedInput));
  //   assertEquals(
  //       "Right operand to and operator must be Boolean: estimatedAge",
  //       reversedError.getMessage());
  // }
  //
}
