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
 * @author Piotr Szul
 */
@SpringBootUnitTest
@NotImplemented
class MembershipOperatorTest {

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
  // @BeforeEach
  // void setUp() {
  //   parserContext = new ParserContextBuilder(spark, fhirContext).build();
  // }
  //
  // static Stream<String> parameters() {
  //   return Stream.of("in", "contains");
  // }
  //
  // Collection testOperator(final String operator, final Collection collection,
  //     final Collection element) {
  //   final BinaryOperatorInput operatorInput;
  //   if ("in".equals(operator)) {
  //     operatorInput = new BinaryOperatorInput(parserContext, element, collection);
  //   } else if ("contains".equals(operator)) {
  //     operatorInput = new BinaryOperatorInput(parserContext, collection, element);
  //   } else {
  //     throw new IllegalArgumentException("Membership operator '" + operator + "' cannot be tested");
  //   }
  //
  //   final Collection result = BinaryOperator.getInstance(operator).invoke(operatorInput);
  //   assertThat(result)
  //       .isElementPath(BooleanCollection.class)
  //       .isSingular();
  //   return result;
  // }
  //
  // @ParameterizedTest
  // @MethodSource("parameters")
  // void returnsCorrectResultWhenElementIsLiteral(final String operator) {
  //   final PrimitivePath collection = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.STRING)
  //       .dataset(StringPrimitiveRowFixture.createCompleteDataset(spark))
  //       .idAndValueColumns()
  //       .build();
  //   final StringLiteralPath element = StringCollection.fromLiteral("'Samuel'", collection);
  //   parserContext = new ParserContextBuilder(spark, fhirContext)
  //       .groupingColumns(Collections.singletonList(collection.getIdColumn()))
  //       .build();
  //
  //   final Collection result = testOperator(operator, collection, element);
  //   assertThat(result)
  //       .selectOrderedResult()
  //       .hasRows(
  //           RowFactory.create(StringPrimitiveRowFixture.ROW_ID_1, false),
  //           RowFactory.create(StringPrimitiveRowFixture.ROW_ID_2, true),
  //           RowFactory.create(StringPrimitiveRowFixture.ROW_ID_3, false),
  //           RowFactory.create(StringPrimitiveRowFixture.ROW_ID_4, false),
  //           RowFactory.create(StringPrimitiveRowFixture.ROW_ID_5, false));
  // }
  //
  // @ParameterizedTest
  // @MethodSource("parameters")
  // void returnsCorrectResultWhenElementIsExpression(final String operator) {
  //   final PrimitivePath collection = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.STRING)
  //       .dataset(StringPrimitiveRowFixture.createCompleteDataset(spark))
  //       .idAndValueColumns()
  //       .build();
  //   final PrimitivePath element = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.STRING)
  //       .dataset(StringPrimitiveRowFixture
  //           .createDataset(spark, RowFactory.create(StringPrimitiveRowFixture.ROW_ID_1, "Eva"),
  //               StringPrimitiveRowFixture.STRING_2_SAMUEL,
  //               StringPrimitiveRowFixture.STRING_3_NULL,
  //               StringPrimitiveRowFixture.STRING_4_ADAM,
  //               StringPrimitiveRowFixture.STRING_5_NULL))
  //       .idAndValueColumns()
  //       .singular(true)
  //       .expression("name.family.first()")
  //       .build();
  //   parserContext = new ParserContextBuilder(spark, fhirContext)
  //       .groupingColumns(Collections.singletonList(collection.getIdColumn()))
  //       .build();
  //
  //   final Collection result = testOperator(operator, collection, element);
  //   assertThat(result)
  //       .selectOrderedResult()
  //       .hasRows(
  //           RowFactory.create(StringPrimitiveRowFixture.ROW_ID_1, false),
  //           RowFactory.create(StringPrimitiveRowFixture.ROW_ID_2, true),
  //           RowFactory.create(StringPrimitiveRowFixture.ROW_ID_3, null),
  //           RowFactory.create(StringPrimitiveRowFixture.ROW_ID_4, true),
  //           RowFactory.create(StringPrimitiveRowFixture.ROW_ID_5, null));
  // }
  //
  // @ParameterizedTest
  // @MethodSource("parameters")
  // void resultIsFalseWhenCollectionIsEmpty(final String operator) {
  //   final PrimitivePath collection = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.STRING)
  //       .dataset(StringPrimitiveRowFixture.createNullRowsDataset(spark))
  //       .idAndValueColumns()
  //       .build();
  //   final StringLiteralPath element = StringCollection.fromLiteral("'Samuel'", collection);
  //   parserContext = new ParserContextBuilder(spark, fhirContext)
  //       .groupingColumns(Collections.singletonList(collection.getIdColumn()))
  //       .build();
  //
  //   final Collection result = testOperator(operator, collection, element);
  //   assertThat(result)
  //       .selectOrderedResult()
  //       .hasRows(
  //           RowFactory.create(StringPrimitiveRowFixture.ROW_ID_3, false),
  //           RowFactory.create(StringPrimitiveRowFixture.ROW_ID_5, false));
  // }
  //
  // @ParameterizedTest
  // @MethodSource("parameters")
  // void returnsEmptyWhenElementIsEmpty(final String operator) {
  //   final PrimitivePath collection = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.STRING)
  //       .dataset(StringPrimitiveRowFixture.createCompleteDataset(spark))
  //       .idAndValueColumns()
  //       .build();
  //   final PrimitivePath element = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.STRING)
  //       .dataset(StringPrimitiveRowFixture.createAllRowsNullDataset(spark))
  //       .idAndValueColumns()
  //       .singular(true)
  //       .expression("name.family.first()")
  //       .build();
  //   parserContext = new ParserContextBuilder(spark, fhirContext)
  //       .groupingColumns(Collections.singletonList(collection.getIdColumn()))
  //       .build();
  //
  //   final Collection result = testOperator(operator, collection, element);
  //   assertThat(result)
  //       .selectOrderedResult()
  //       .hasRows(
  //           RowFactory.create(StringPrimitiveRowFixture.ROW_ID_1, null),
  //           RowFactory.create(StringPrimitiveRowFixture.ROW_ID_2, null),
  //           RowFactory.create(StringPrimitiveRowFixture.ROW_ID_3, null),
  //           RowFactory.create(StringPrimitiveRowFixture.ROW_ID_4, null),
  //           RowFactory.create(StringPrimitiveRowFixture.ROW_ID_5, null));
  // }
  //
  // @ParameterizedTest
  // @MethodSource("parameters")
  // void worksForCodingLiterals(final String operator) {
  //
  //   final Coding snomedCoding = new Coding(SNOMED_URL, "56459004", null);
  //   final Coding loincCoding1 = new Coding(LOINC_URL, "56459004", null);
  //   loincCoding1.setId("fake-id-1");
  //   final Coding loincCoding2 = new Coding(LOINC_URL, "56459004", null);
  //   loincCoding2.setId("fake-id-2");
  //   final Coding loincCodingWithVersion = new Coding(LOINC_URL, "56459004", null);
  //   loincCodingWithVersion.setVersion("version1");
  //
  //   final Dataset<Row> codingDataset = new DatasetBuilder(spark)
  //       .withIdColumn()
  //       .withStructTypeColumns(codingStructType())
  //       .withRow(StringPrimitiveRowFixture.ROW_ID_1, rowFromCoding(snomedCoding))
  //       .withRow(StringPrimitiveRowFixture.ROW_ID_1, rowFromCoding(loincCoding1))
  //       .withRow(StringPrimitiveRowFixture.ROW_ID_2, rowFromCoding(snomedCoding))
  //       .withRow(StringPrimitiveRowFixture.ROW_ID_2, rowFromCoding(loincCoding2))
  //       .withRow(StringPrimitiveRowFixture.ROW_ID_3, rowFromCoding(snomedCoding))
  //       .withRow(StringPrimitiveRowFixture.ROW_ID_3, rowFromCoding(loincCodingWithVersion))
  //       .withRow(StringPrimitiveRowFixture.ROW_ID_4, null)
  //       .withRow(StringPrimitiveRowFixture.ROW_ID_4, null)
  //       .buildWithStructValue();
  //
  //   final PrimitivePath collection = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.CODING)
  //       .dataset(codingDataset)
  //       .idAndValueColumns()
  //       .build();
  //
  //   final CodingLiteralPath element = CodingCollection
  //       .fromLiteral("http://loinc.org|56459004", collection);
  //   parserContext = new ParserContextBuilder(spark, fhirContext)
  //       .groupingColumns(Collections.singletonList(collection.getIdColumn()))
  //       .build();
  //
  //   final Collection result = testOperator(operator, collection, element);
  //   assertThat(result)
  //       .selectOrderedResult()
  //       .hasRows(
  //           RowFactory.create(StringPrimitiveRowFixture.ROW_ID_1, true),
  //           RowFactory.create(StringPrimitiveRowFixture.ROW_ID_2, true),
  //           RowFactory.create(StringPrimitiveRowFixture.ROW_ID_3, false),
  //           RowFactory.create(StringPrimitiveRowFixture.ROW_ID_4, false));
  // }
  //
  // @ParameterizedTest
  // @MethodSource("parameters")
  // void throwExceptionWhenElementIsNotSingular(final String operator) {
  //   final PrimitivePath collection = new ElementPathBuilder(spark)
  //       .singular(false)
  //       .build();
  //   final PrimitivePath element = new ElementPathBuilder(spark)
  //       .singular(false)
  //       .expression("name.given")
  //       .build();
  //
  //   final InvalidUserInputError error = assertThrows(
  //       InvalidUserInputError.class,
  //       () -> testOperator(operator, collection, element));
  //   assertEquals("Element operand used with " + operator + " operator is not singular: name.given",
  //       error.getMessage());
  // }
  //
  //
  // @ParameterizedTest
  // @MethodSource("parameters")
  // void throwExceptionWhenIncompatibleTypes(final String operator) {
  //   final PrimitivePath collection = new ElementPathBuilder(spark)
  //       .fhirType(FHIRDefinedType.STRING)
  //       .expression("foo")
  //       .build();
  //   final BooleanLiteralPath element = BooleanLiteralPath.fromString("true", collection);
  //
  //   final InvalidUserInputError error = assertThrows(
  //       InvalidUserInputError.class,
  //       () -> testOperator(operator, collection, element));
  //   assertEquals(
  //       "Left operand to " + operator + " operator is not comparable to right operand: "
  //           + (operator.equals("in")
  //              ? "true in foo"
  //              : "foo contains true"),
  //       error.getMessage());
  // }

}
