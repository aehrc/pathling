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
class EqualityOperatorCodingTest {

  // TODO: implement with columns

  //
  // @Autowired
  // SparkSession spark;
  //
  // @Autowired
  // FhirContext fhirContext;
  //
  // static final String ID_ALIAS = "_abc123";
  //
  // Collection left;
  // Collection right;
  // Collection literalSnomedAll;
  // Collection literalLoincSystemCode;
  // ParserContext parserContext;
  //
  // @BeforeEach
  // void setUp() {
  //   // all components
  //   final Coding coding1 = new Coding(SNOMED_URL, "56459004", null);
  //   coding1.setVersion("http://snomed.info/sct/32506021000036107/version/20191231");
  //   coding1.setDisplay("Display name");
  //   coding1.setUserSelected(true);
  //   coding1.setId("some-fake-id");
  //
  //   final Coding coding2 = new Coding(SNOMED_URL, "56459004", null);
  //   coding2.setVersion("http://snomed.info/sct/32506021000036107/version/20191231");
  //   coding2.setDisplay("Display name");
  //
  //   final Coding coding3 = new Coding(SNOMED_URL, "56459004", null);
  //   coding3.setVersion("http://snomed.info/sct/32506021000036107/version/20191231");
  //
  //   final Coding coding4 = new Coding(LOINC_URL, "222|33", null);
  //   coding4.setId("fake-id-1");
  //   final Coding coding5 = new Coding(LOINC_URL, "222|33", null);
  //   coding5.setId("fake-id-2");
  //
  //   final Coding coding6 = new Coding(LOINC_URL, "56459004", null);
  //   coding6.setVersion("http://snomed.info/sct/32506021000036107/version/20191231");
  //   coding6.setDisplay("Display name");
  //   coding6.setUserSelected(true);
  //   coding6.setId("some-fake-id");
  //
  //   final Coding coding1_other = coding1.copy();
  //   coding1_other.setId("some-other-fake-id");
  //
  //   final Dataset<Row> leftDataset = new DatasetBuilder(spark)
  //       .withIdColumn(ID_ALIAS)
  //       .withStructTypeColumns(codingStructType())
  //       .withRow("patient-1", rowFromCoding(coding1))
  //       .withRow("patient-2", rowFromCoding(coding2))
  //       .withRow("patient-3", rowFromCoding(coding3))
  //       .withRow("patient-4", rowFromCoding(coding4))
  //       .withRow("patient-5", rowFromCoding(coding5))
  //       .withRow("patient-6", rowFromCoding(coding6))
  //       .withRow("patient-7", null)
  //       .buildWithStructValue();
  //   left = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.CODING)
  //       .singular(true)
  //       .dataset(leftDataset)
  //       .idAndValueColumns()
  //       .build();
  //   final Dataset<Row> rightDataset = new DatasetBuilder(spark)
  //       .withIdColumn(ID_ALIAS)
  //       .withStructTypeColumns(codingStructType())
  //       .withRow("patient-1", rowFromCoding(coding1_other))
  //       .withRow("patient-2", rowFromCoding(coding3))
  //       .withRow("patient-3", rowFromCoding(coding3))
  //       .withRow("patient-4", rowFromCoding(coding5))
  //       .withRow("patient-5", rowFromCoding(coding6))
  //       .withRow("patient-6", null)
  //       .withRow("patient-7", null)
  //       .buildWithStructValue();
  //   right = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.CODING)
  //       .singular(true)
  //       .dataset(rightDataset)
  //       .idAndValueColumns()
  //       .build();
  //   literalSnomedAll = CodingCollection.fromLiteral(
  //       "http://snomed.info/sct|56459004|http://snomed.info/sct/32506021000036107/version/20191231|'Display name'|true",
  //       left);
  //   literalLoincSystemCode = CodingCollection.fromLiteral("http://loinc.org|'222|33'", left);
  //
  //   parserContext = new ParserContextBuilder(spark, fhirContext)
  //       .groupingColumns(Collections.singletonList(left.getIdColumn()))
  //       .build();
  // }
  //
  // @Test
  // void equals() {
  //   final BinaryOperatorInput input = new BinaryOperatorInput(parserContext, left, right);
  //   final BinaryOperator equalityOperator = BinaryOperator.getInstance("=");
  //   final Collection result = equalityOperator.invoke(input);
  //
  //   assertThat(result).selectOrderedResult().hasRows(
  //       RowFactory.create("patient-1", true),
  //       RowFactory.create("patient-2", false),
  //       RowFactory.create("patient-3", true),
  //       RowFactory.create("patient-4", true),
  //       RowFactory.create("patient-5", false),
  //       RowFactory.create("patient-6", null),
  //       RowFactory.create("patient-7", null)
  //   );
  // }
  //
  // @Test
  // void notEquals() {
  //   final BinaryOperatorInput input = new BinaryOperatorInput(parserContext, left, right);
  //   final BinaryOperator equalityOperator = BinaryOperator.getInstance("!=");
  //   final Collection result = equalityOperator.invoke(input);
  //
  //   assertThat(result).selectOrderedResult().hasRows(
  //       RowFactory.create("patient-1", false),
  //       RowFactory.create("patient-2", true),
  //       RowFactory.create("patient-3", false),
  //       RowFactory.create("patient-4", false),
  //       RowFactory.create("patient-5", true),
  //       RowFactory.create("patient-6", null),
  //       RowFactory.create("patient-7", null)
  //   );
  // }
  //
  // @Test
  // void literalEquals() {
  //   final BinaryOperatorInput input = new BinaryOperatorInput(parserContext, literalLoincSystemCode,
  //       left);
  //   final BinaryOperator equalityOperator = BinaryOperator.getInstance("=");
  //   final Collection result = equalityOperator.invoke(input);
  //
  //   assertThat(result).selectOrderedResult().hasRows(
  //       RowFactory.create("patient-1", false),
  //       RowFactory.create("patient-2", false),
  //       RowFactory.create("patient-3", false),
  //       RowFactory.create("patient-4", true),
  //       RowFactory.create("patient-5", true),
  //       RowFactory.create("patient-6", false),
  //       RowFactory.create("patient-7", null)
  //   );
  // }
  //
  // @Test
  // void equalsLiteral() {
  //   final BinaryOperatorInput input = new BinaryOperatorInput(parserContext, left,
  //       literalSnomedAll);
  //   final BinaryOperator equalityOperator = BinaryOperator.getInstance("=");
  //   final Collection result = equalityOperator.invoke(input);
  //
  //   assertThat(result).selectOrderedResult().hasRows(
  //       RowFactory.create("patient-1", true),
  //       RowFactory.create("patient-2", false),
  //       RowFactory.create("patient-3", false),
  //       RowFactory.create("patient-4", false),
  //       RowFactory.create("patient-5", false),
  //       RowFactory.create("patient-6", false),
  //       RowFactory.create("patient-7", null)
  //   );
  // }
  //
  // @Test
  // void literalNotEquals() {
  //   final BinaryOperatorInput input = new BinaryOperatorInput(parserContext, literalLoincSystemCode,
  //       left);
  //   final BinaryOperator equalityOperator = BinaryOperator.getInstance("!=");
  //   final Collection result = equalityOperator.invoke(input);
  //
  //   assertThat(result).selectOrderedResult().hasRows(
  //       RowFactory.create("patient-1", true),
  //       RowFactory.create("patient-2", true),
  //       RowFactory.create("patient-3", true),
  //       RowFactory.create("patient-4", false),
  //       RowFactory.create("patient-5", false),
  //       RowFactory.create("patient-6", true),
  //       RowFactory.create("patient-7", null)
  //   );
  // }
  //
  // @Test
  // void notEqualsLiteral() {
  //   final BinaryOperatorInput input = new BinaryOperatorInput(parserContext, left,
  //       literalSnomedAll);
  //   final BinaryOperator equalityOperator = BinaryOperator.getInstance("!=");
  //   final Collection result = equalityOperator.invoke(input);
  //
  //   assertThat(result).selectOrderedResult().hasRows(
  //       RowFactory.create("patient-1", false),
  //       RowFactory.create("patient-2", true),
  //       RowFactory.create("patient-3", true),
  //       RowFactory.create("patient-4", true),
  //       RowFactory.create("patient-5", true),
  //       RowFactory.create("patient-6", true),
  //       RowFactory.create("patient-7", null)
  //   );
  // }

}
