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
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
@NotImplemented
public class MathOperatorQuantityTest {

  // TODO: implement with columns

  //
  // @Autowired
  // SparkSession spark;
  //
  // @Autowired
  // FhirContext fhirContext;
  //
  // @Autowired
  // UcumService ucumService;
  //
  // static final List<String> OPERATORS = List.of("+", "-", "*", "/");
  // static final String ID_ALIAS = "_abc123";
  //
  // @Value
  // static class TestParameters {
  //
  //   @Nonnull
  //   String name;
  //
  //   @Nonnull
  //   Collection left;
  //
  //   @Nonnull
  //   Collection right;
  //
  //   @Nonnull
  //   ParserContext context;
  //
  //   @Nonnull
  //   BinaryOperator operator;
  //
  //   @Nonnull
  //   Dataset<Row> expectedResult;
  //
  //   @Override
  //   public String toString() {
  //     return name;
  //   }
  //
  // }
  //
  // @Nonnull
  // Stream<TestParameters> parameters() {
  //   final java.util.Collection<TestParameters> parameters = new ArrayList<>();
  //   for (final String operator : OPERATORS) {
  //     final String name = "Quantity " + operator + " Quantity";
  //     final Collection left = buildQuantityExpression(true);
  //     final Collection right = buildQuantityExpression(false);
  //     final ParserContext context = new ParserContextBuilder(spark, fhirContext)
  //         .groupingColumns(Collections.singletonList(left.getIdColumn()))
  //         .build();
  //     parameters.add(
  //         new TestParameters(name, left, right, context, BinaryOperator.getInstance(operator),
  //             expectedResult(operator)));
  //   }
  //   return parameters.stream();
  // }
  //
  // Dataset<Row> expectedResult(@Nonnull final String operator) {
  //   final Row result1;
  //   final Row result2;
  //
  //   switch (operator) {
  //     case "+":
  //       result1 = rowForUcumQuantity(new BigDecimal("1650.0"), "m");
  //       result2 = null;
  //       break;
  //     case "-":
  //       result1 = rowForUcumQuantity(new BigDecimal("-1350.0"), "m");
  //       result2 = null;
  //       break;
  //     case "*":
  //       result1 = null;
  //       result2 = rowForUcumQuantity("1000.0", "g");
  //       break;
  //     case "/":
  //       result1 = rowForUcumQuantity(new BigDecimal("0.1"), "1");
  //       result2 = rowForUcumQuantity("4000", "g");
  //       break;
  //     default:
  //       result1 = null;
  //       result2 = null;
  //   }
  //   return new DatasetBuilder(spark)
  //       .withIdColumn(ID_ALIAS)
  //       .withStructTypeColumns(quantityStructType())
  //       .withRow("patient-1", result1)
  //       .withRow("patient-2", result2)
  //       .withRow("patient-3", null)
  //       .withRow("patient-4", null)
  //       .withRow("patient-5", null)
  //       .withRow("patient-6", null)
  //       .withRow("patient-7", null)
  //       .buildWithStructValue();
  // }
  //
  // @Nonnull
  // Collection buildQuantityExpression(final boolean leftOperand) {
  //   final Quantity nonUcumQuantity = new Quantity();
  //   nonUcumQuantity.setValue(15);
  //   nonUcumQuantity.setUnit("mSv");
  //   nonUcumQuantity.setSystem(TestHelpers.SNOMED_URL);
  //   nonUcumQuantity.setCode("282250007");
  //
  //   final Dataset<Row> dataset = new DatasetBuilder(spark)
  //       .withIdColumn(ID_ALIAS)
  //       .withStructTypeColumns(quantityStructType())
  //       .withRow("patient-1", leftOperand
  //                             ? rowForUcumQuantity(new BigDecimal("150.0"), "m")
  //                             : rowForUcumQuantity(new BigDecimal("1.5"), "km"))
  //       .withRow("patient-2", leftOperand
  //                             ? rowForUcumQuantity(new BigDecimal("2.0"), "kg")
  //                             : rowForUcumQuantity(new BigDecimal("0.5"), "1"))
  //       .withRow("patient-3", leftOperand
  //                             ? rowForUcumQuantity(new BigDecimal("7.7"), "mSv")
  //                             : rowForUcumQuantity(new BigDecimal("1.5"), "h"))
  //       // Not comparable
  //       .withRow("patient-4", leftOperand
  //                             ? rowForUcumQuantity(new BigDecimal("7.7"), "mSv")
  //                             : rowFromQuantity(nonUcumQuantity))
  //       // Not comparable
  //       .withRow("patient-5", leftOperand
  //                             ? null
  //                             : rowForUcumQuantity(new BigDecimal("1.5"), "h"))
  //       .withRow("patient-6", leftOperand
  //                             ? rowForUcumQuantity(new BigDecimal("7.7"), "mSv")
  //                             : null)
  //       .withRow("patient-7", null)
  //       .buildWithStructValue();
  //   return new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.QUANTITY)
  //       .dataset(dataset)
  //       .idAndValueColumns()
  //       .singular(true)
  //       .build();
  // }
  //
  // @ParameterizedTest
  // @MethodSource("parameters")
  // void test(@Nonnull final TestParameters parameters) {
  //   final BinaryOperatorInput input = new BinaryOperatorInput(parameters.getContext(),
  //       parameters.getLeft(), parameters.getRight());
  //   final Collection result = parameters.getOperator().invoke(input);
  //   assertThat(result).selectOrderedResult().hasRows(parameters.getExpectedResult());
  // }
  //
  // /*
  //  * This test requires a very high precision decimal representation, due to these units being
  //  * canonicalized by UCUM to "reciprocal cubic meters".
  //  * See: https://www.wolframalpha.com/input?i=3+mmol%2FL+in+m%5E-3
  //  */
  // @Test
  // void volumeArithmetic() {
  //   final Dataset<Row> rightDataset = new DatasetBuilder(spark)
  //       .withIdColumn(ID_ALIAS)
  //       .withStructTypeColumns(quantityStructType())
  //       .withRow("patient-1", rowForUcumQuantity(new BigDecimal("1.0"), "mmol/L"))
  //       .buildWithStructValue();
  //   final Collection right = new ElementPathBuilder(spark)
  //       .expression("valueQuantity")
  //       .fhirType(FHIRDefinedType.QUANTITY)
  //       .singular(true)
  //       .dataset(rightDataset)
  //       .idAndValueColumns()
  //       .build();
  //   final Collection left = QuantityCollection.fromUcumString("1.0 'mmol/L'", right,
  //       ucumService);
  //   final ParserContext context = new ParserContextBuilder(spark, fhirContext)
  //       .groupingColumns(Collections.singletonList(left.getIdColumn()))
  //       .build();
  //   final BinaryOperatorInput input = new BinaryOperatorInput(context, left, right);
  //   final Collection result = BinaryOperator.getInstance("+").invoke(input);
  //
  //   final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
  //       .withIdColumn()
  //       .withStructTypeColumns(quantityStructType())
  //       .withRow("patient-1",
  //           rowForUcumQuantity(new BigDecimal("1204427340000000000000000"), "m-3"))
  //       .buildWithStructValue();
  //   assertThat(result).selectOrderedResult().hasRows(expectedDataset);
  // }

}