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
class ComparisonOperatorInstantTest {

  // TODO: implement with columns
  //
  //
  // @Autowired
  // SparkSession spark;
  //
  // @Autowired
  // FhirContext fhirContext;
  //
  // static final String ID_ALIAS = "_abc123";
  // private PrimitivePath left;
  // private PrimitivePath right;
  // private ParserContext parserContext;
  //
  // @BeforeEach
  // void setUp() {
  //   final Optional<ElementDefinition> optionalLeftDefinition = FhirHelpers
  //       .getChildOfResource(fhirContext, "Observation", "issued");
  //   assertTrue(optionalLeftDefinition.isPresent());
  //   final ElementDefinition leftDefinition = optionalLeftDefinition.get();
  //   assertTrue(leftDefinition.getFhirType().isPresent());
  //   assertEquals(FHIRDefinedType.INSTANT, leftDefinition.getFhirType().get());
  //
  //   final Dataset<Row> leftDataset = new DatasetBuilder(spark)
  //       .withIdColumn(ID_ALIAS)
  //       .withColumn(DataTypes.TimestampType)
  //       .withRow("patient-1", Instant.ofEpochMilli(1667690454622L))  // Equal, exact
  //       .withRow("patient-2", Instant.ofEpochMilli(1667690454621L))  // Less than
  //       .withRow("patient-3", Instant.ofEpochMilli(1667690454623L))  // Greater than
  //       .build();
  //   left = new ElementPathBuilder(spark)
  //       .dataset(leftDataset)
  //       .idAndValueColumns()
  //       .expression("authoredOn")
  //       .singular(true)
  //       .definition(leftDefinition)
  //       .buildDefined();
  //
  //   final Optional<ElementDefinition> optionalRightDefinition = FhirHelpers
  //       .getChildOfResource(fhirContext, "Condition", "onsetDateTime");
  //   assertTrue(optionalRightDefinition.isPresent());
  //   final ElementDefinition rightDefinition = optionalRightDefinition.get();
  //   assertTrue(rightDefinition.getFhirType().isPresent());
  //   assertEquals(FHIRDefinedType.DATETIME, rightDefinition.getFhirType().get());
  //
  //   final Dataset<Row> rightDataset = new DatasetBuilder(spark)
  //       .withIdColumn(ID_ALIAS)
  //       .withColumn(DataTypes.StringType)
  //       .withRow("patient-1", "2022-11-06T09:20:54.622+10:00")  // Equal, exact
  //       .withRow("patient-2", "2022-11-06T09:20:54.622+10:00")  // Less than
  //       .withRow("patient-3", "2022-11-06T09:20:54.622+10:00")  // Greater than
  //       .build();
  //   right = new ElementPathBuilder(spark)
  //       .dataset(rightDataset)
  //       .idAndValueColumns()
  //       .expression("reverseResolve(Condition.subject).onsetDateTime")
  //       .singular(true)
  //       .definition(rightDefinition)
  //       .buildDefined();
  //
  //   parserContext = new ParserContextBuilder(spark, fhirContext)
  //       .groupingColumns(Collections.singletonList(left.getIdColumn()))
  //       .build();
  // }
  //
  // @Test
  // void equals() {
  //   final BinaryOperatorInput comparisonInput = new BinaryOperatorInput(parserContext, left, right);
  //   final BinaryOperator lessThan = BinaryOperator.getInstance("=");
  //   final Collection result = lessThan.invoke(comparisonInput);
  //
  //   assertThat(result).selectOrderedResult().hasRows(
  //       RowFactory.create("patient-1", true),  // Equal, exact
  //       RowFactory.create("patient-2", false), // Less than
  //       RowFactory.create("patient-3", false)  // Greater than
  //   );
  // }
  //
  // @Test
  // void notEquals() {
  //   final BinaryOperatorInput comparisonInput = new BinaryOperatorInput(parserContext, left, right);
  //   final BinaryOperator lessThan = BinaryOperator.getInstance("!=");
  //   final Collection result = lessThan.invoke(comparisonInput);
  //
  //   assertThat(result).selectOrderedResult().hasRows(
  //       RowFactory.create("patient-1", false),  // Equal, exact
  //       RowFactory.create("patient-2", true),   // Less than
  //       RowFactory.create("patient-3", true)    // Greater than
  //   );
  // }
  //
  // @Test
  // void lessThan() {
  //   final BinaryOperatorInput comparisonInput = new BinaryOperatorInput(parserContext, left, right);
  //   final BinaryOperator lessThan = BinaryOperator.getInstance("<");
  //   final Collection result = lessThan.invoke(comparisonInput);
  //
  //   assertThat(result).selectOrderedResult().hasRows(
  //       RowFactory.create("patient-1", false),  // Equal, exact
  //       RowFactory.create("patient-2", true),   // Less than
  //       RowFactory.create("patient-3", false)   // Greater than
  //   );
  // }
  //
  // @Test
  // void lessThanOrEqualTo() {
  //   final BinaryOperatorInput comparisonInput = new BinaryOperatorInput(parserContext, left, right);
  //   final BinaryOperator lessThan = BinaryOperator.getInstance("<=");
  //   final Collection result = lessThan.invoke(comparisonInput);
  //
  //   assertThat(result).selectOrderedResult().hasRows(
  //       RowFactory.create("patient-1", true),  // Equal, exact
  //       RowFactory.create("patient-2", true),  // Less than
  //       RowFactory.create("patient-3", false)  // Greater than
  //   );
  // }
  //
  // @Test
  // void greaterThan() {
  //   final BinaryOperatorInput comparisonInput = new BinaryOperatorInput(parserContext, left, right);
  //   final BinaryOperator lessThan = BinaryOperator.getInstance(">");
  //   final Collection result = lessThan.invoke(comparisonInput);
  //
  //   assertThat(result).selectOrderedResult().hasRows(
  //       RowFactory.create("patient-1", false),  // Equal, exact
  //       RowFactory.create("patient-2", false),  // Less than
  //       RowFactory.create("patient-3", true)    // Greater than
  //   );
  // }
  //
  // @Test
  // void greaterThanOrEqualTo() {
  //   final BinaryOperatorInput comparisonInput = new BinaryOperatorInput(parserContext, left, right);
  //   final BinaryOperator lessThan = BinaryOperator.getInstance(">=");
  //   final Collection result = lessThan.invoke(comparisonInput);
  //
  //   assertThat(result).selectOrderedResult().hasRows(
  //       RowFactory.create("patient-1", true),  // Equal, exact
  //       RowFactory.create("patient-2", false), // Less than
  //       RowFactory.create("patient-3", true)   // Greater than
  //   );
  // }
  //
}
