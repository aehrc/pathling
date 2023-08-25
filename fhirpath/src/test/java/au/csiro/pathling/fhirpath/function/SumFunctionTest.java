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
class SumFunctionTest {

  // TODO: implement with columns

  //
  // @Autowired
  // SparkSession spark;
  //
  // @Autowired
  // FhirContext fhirContext;
  //
  // ParserContext parserContext;
  //
  // @Test
  // void returnsCorrectIntegerResult() {
  //   final Dataset<Row> inputDataset = new DatasetBuilder(spark)
  //       .withIdColumn()
  //       .withEidColumn()
  //       .withColumn(DataTypes.IntegerType)
  //       .withRow("observation-1", makeEid(0), 3)
  //       .withRow("observation-1", makeEid(1), 5)
  //       .withRow("observation-1", makeEid(2), 7)
  //       .withRow("observation-2", null, null)
  //       .withRow("observation-3", makeEid(0), -1)
  //       .withRow("observation-3", makeEid(1), null)
  //       .build();
  //   final PrimitivePath inputPath = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.INTEGER)
  //       .dataset(inputDataset)
  //       .idAndEidAndValueColumns()
  //       .expression("valueInteger")
  //       .singular(false)
  //       .build();
  //   parserContext = new ParserContextBuilder(spark, fhirContext)
  //       .groupingColumns(Collections.singletonList(inputPath.getIdColumn()))
  //       .build();
  //
  //   final NamedFunctionInput sumInput = new NamedFunctionInput(parserContext, inputPath,
  //       Collections.emptyList());
  //   final Collection result = NamedFunction.getInstance("sum").invoke(sumInput);
  //
  //   final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
  //       .withIdColumn()
  //       .withColumn(DataTypes.IntegerType)
  //       .withRow("observation-1", 15)
  //       .withRow("observation-2", null)
  //       .withRow("observation-3", -1)
  //       .build();
  //   assertThat(result)
  //       .hasExpression("valueInteger.sum()")
  //       .isSingular()
  //       .isElementPath(IntegerCollection.class)
  //       .selectResult()
  //       .hasRows(expectedDataset);
  // }
  //
  // @Test
  // void returnsCorrectDecimalResult() {
  //   final Dataset<Row> inputDataset = new DatasetBuilder(spark)
  //       .withIdColumn()
  //       .withEidColumn()
  //       .withColumn(DataTypes.createDecimalType())
  //       .withRow("observation-1", makeEid(0), new BigDecimal("3.0"))
  //       .withRow("observation-1", makeEid(1), new BigDecimal("5.5"))
  //       .withRow("observation-1", makeEid(2), new BigDecimal("7"))
  //       .withRow("observation-2", null, null)
  //       .withRow("observation-3", makeEid(0), new BigDecimal("-2.50"))
  //       .build();
  //   final PrimitivePath inputPath = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.DECIMAL)
  //       .dataset(inputDataset)
  //       .idAndEidAndValueColumns()
  //       .expression("valueDecimal")
  //       .singular(false)
  //       .build();
  //   parserContext = new ParserContextBuilder(spark, fhirContext)
  //       .groupingColumns(Collections.singletonList(inputPath.getIdColumn()))
  //       .build();
  //
  //   final NamedFunctionInput sumInput = new NamedFunctionInput(parserContext, inputPath,
  //       Collections.emptyList());
  //   final Collection result = NamedFunction.getInstance("sum").invoke(sumInput);
  //
  //   final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
  //       .withIdColumn()
  //       .withColumn(DataTypes.createDecimalType())
  //       .withRow("observation-1", new BigDecimal("15.5"))
  //       .withRow("observation-2", null)
  //       .withRow("observation-3", new BigDecimal("-2.5"))
  //       .build();
  //   assertThat(result)
  //       .hasExpression("valueDecimal.sum()")
  //       .isSingular()
  //       .isElementPath(DecimalCollection.class)
  //       .selectResult()
  //       .hasRows(expectedDataset);
  // }
  //
  // @Test
  // void throwsErrorIfInputNotNumeric() {
  //   final PrimitivePath inputPath = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.STRING)
  //       .expression("valueString")
  //       .build();
  //
  //   final NamedFunctionInput sumInput = new NamedFunctionInput(parserContext, inputPath,
  //       Collections.emptyList());
  //   final NamedFunction sumFunction = NamedFunction.getInstance("sum");
  //
  //   final InvalidUserInputError error = assertThrows(
  //       InvalidUserInputError.class,
  //       () -> sumFunction.invoke(sumInput));
  //   assertEquals(
  //       "Input to sum function must be numeric: valueString",
  //       error.getMessage());
  // }
  //
}
