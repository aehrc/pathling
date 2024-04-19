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
import static au.csiro.pathling.test.helpers.SparkHelpers.codeableConceptStructType;
import static au.csiro.pathling.test.helpers.SparkHelpers.codingStructType;
import static au.csiro.pathling.test.helpers.SparkHelpers.rowFromCodeableConcept;
import static au.csiro.pathling.test.helpers.SparkHelpers.rowFromCoding;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.element.BooleanPath;
import au.csiro.pathling.fhirpath.element.CodingPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.function.NamedFunction;
import au.csiro.pathling.fhirpath.function.NamedFunctionInput;
import au.csiro.pathling.fhirpath.literal.CodingLiteralPath;
import au.csiro.pathling.fhirpath.literal.StringLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.assertions.DatasetAssert;
import au.csiro.pathling.test.assertions.ElementPathAssertion;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import au.csiro.pathling.test.helpers.TerminologyServiceHelpers;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author Piotr Szul
 */
@SpringBootUnitTest
class SubsumesFunctionTest {

  static final String TEST_SYSTEM = "uuid:1";

  static final Coding CODING_SMALL = new Coding(TEST_SYSTEM, "SMALL", null);
  static final Coding CODING_MEDIUM = new Coding(TEST_SYSTEM, "MEDIUM", null);
  static final Coding CODING_LARGE = new Coding(TEST_SYSTEM, "LARGE", null);
  static final Coding CODING_OTHER1 = new Coding(TEST_SYSTEM, "OTHER1", null);
  static final Coding CODING_OTHER2 = new Coding(TEST_SYSTEM, "OTHER2", null);
  static final Coding CODING_OTHER3 = new Coding(TEST_SYSTEM, "OTHER3", null);
  static final Coding CODING_OTHER4 = new Coding(TEST_SYSTEM, "OTHER4", null);
  static final Coding CODING_OTHER5 = new Coding(TEST_SYSTEM, "OTHER5", null);

  static final String RES_ID1 = "condition-xyz1";
  static final String RES_ID2 = "condition-xyz2";
  static final String RES_ID3 = "condition-xyz3";
  static final String RES_ID4 = "condition-xyz4";
  static final String RES_ID5 = "condition-xyz5";

  static final List<String> ALL_RES_IDS =
      Arrays.asList(RES_ID1, RES_ID2, RES_ID3, RES_ID4, RES_ID5);

  @Autowired
  SparkSession spark;

  @Autowired
  FhirContext fhirContext;

  @Autowired
  TerminologyService terminologyService;

  @Autowired
  TerminologyServiceFactory terminologyServiceFactory;

  static Row codeableConceptRowFromCoding(final Coding coding) {
    return codeableConceptRowFromCoding(coding, CODING_OTHER4);
  }

  static Row codeableConceptRowFromCoding(final Coding coding, final Coding otherCoding) {
    return rowFromCodeableConcept(new CodeableConcept(coding).addCoding(otherCoding));
  }

  @BeforeEach
  void setUp() {
    SharedMocks.resetAll();
    TerminologyServiceHelpers.setupSubsumes(terminologyService)
        .withSubsumes(CODING_LARGE, CODING_MEDIUM)
        .withSubsumes(CODING_MEDIUM, CODING_SMALL)
        .withSubsumes(CODING_LARGE, CODING_SMALL);
  }

  CodingPath createCodingInput() {
    final Dataset<Row> dataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withStructTypeColumns(codingStructType())
        .withRow(RES_ID1, makeEid(1), rowFromCoding(CODING_SMALL))
        .withRow(RES_ID2, makeEid(1), rowFromCoding(CODING_MEDIUM))
        .withRow(RES_ID3, makeEid(1), rowFromCoding(CODING_LARGE))
        .withRow(RES_ID4, makeEid(1), rowFromCoding(CODING_OTHER1))
        .withRow(RES_ID5, null, null /* NULL coding value */)
        .withRow(RES_ID1, makeEid(0), rowFromCoding(CODING_OTHER2))
        .withRow(RES_ID2, makeEid(0), rowFromCoding(CODING_OTHER2))
        .withRow(RES_ID3, makeEid(0), rowFromCoding(CODING_OTHER2))
        .withRow(RES_ID4, makeEid(0), rowFromCoding(CODING_OTHER2))
        .buildWithStructValue();
    final ElementPath inputExpression = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.CODING)
        .dataset(dataset)
        .idAndEidAndValueColumns()
        .singular(false)
        .build();

    return (CodingPath) inputExpression;
  }

  CodingPath createSingularCodingInput() {
    final Dataset<Row> dataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withStructTypeColumns(codingStructType())
        .withRow(RES_ID1, rowFromCoding(CODING_SMALL))
        .withRow(RES_ID2, rowFromCoding(CODING_MEDIUM))
        .withRow(RES_ID3, rowFromCoding(CODING_LARGE))
        .withRow(RES_ID4, rowFromCoding(CODING_OTHER1))
        .withRow(RES_ID5, null /* NULL coding value */)
        .buildWithStructValue();
    final ElementPath inputExpression = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.CODING)
        .dataset(dataset)
        .idAndValueColumns()
        .singular(true)
        .build();

    return (CodingPath) inputExpression;
  }


  ElementPath createCodeableConceptInput() {
    final Dataset<Row> dataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withStructTypeColumns(codeableConceptStructType())
        .withRow(RES_ID1, makeEid(1), codeableConceptRowFromCoding(CODING_SMALL))
        .withRow(RES_ID2, makeEid(1), codeableConceptRowFromCoding(CODING_MEDIUM))
        .withRow(RES_ID3, makeEid(1), codeableConceptRowFromCoding(CODING_LARGE))
        .withRow(RES_ID4, makeEid(1), codeableConceptRowFromCoding(CODING_OTHER1))
        .withRow(RES_ID5, null, null /* NULL codeable concept value */)
        .withRow(RES_ID1, makeEid(0), codeableConceptRowFromCoding(CODING_OTHER2))
        .withRow(RES_ID2, makeEid(0), codeableConceptRowFromCoding(CODING_OTHER2))
        .withRow(RES_ID3, makeEid(0), codeableConceptRowFromCoding(CODING_OTHER2))
        .withRow(RES_ID4, makeEid(0), codeableConceptRowFromCoding(CODING_OTHER2))
        .buildWithStructValue();

    return new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.CODEABLECONCEPT)
        .dataset(dataset)
        .idAndEidAndValueColumns()
        .singular(false)
        .build();
  }

  CodingLiteralPath createLiteralArgOrInput() {
    final Dataset<Row> literalContextDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withColumn(DataTypes.BooleanType)
        .withIdsAndValue(false, ALL_RES_IDS)
        .build();
    final ElementPath literalContext = new ElementPathBuilder(spark)
        .dataset(literalContextDataset)
        .idAndValueColumns()
        .build();

    return CodingLiteralPath.fromString(CODING_MEDIUM.getSystem() + "|" + CODING_MEDIUM.getCode(),
        literalContext);
  }

  CodingPath createCodingArg() {
    final Dataset<Row> dataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withStructTypeColumns(codingStructType())
        .withIdValueRows(ALL_RES_IDS, id -> rowFromCoding(CODING_MEDIUM))
        .withIdValueRows(ALL_RES_IDS, id -> rowFromCoding(CODING_OTHER3))
        .buildWithStructValue();
    final ElementPath argument = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.CODING)
        .dataset(dataset)
        .idAndValueColumns()
        .build();

    return (CodingPath) argument;
  }

  ElementPath createCodeableConceptArg() {
    final Dataset<Row> dataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withStructTypeColumns(codeableConceptStructType())
        .withIdValueRows(ALL_RES_IDS,
            id -> codeableConceptRowFromCoding(CODING_MEDIUM, CODING_OTHER5))
        .withIdValueRows(ALL_RES_IDS,
            id -> codeableConceptRowFromCoding(CODING_OTHER3, CODING_OTHER5))
        .buildWithStructValue();

    return new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.CODEABLECONCEPT)
        .dataset(dataset)
        .idAndValueColumns()
        .build();
  }

  CodingPath createEmptyCodingInput() {
    final Dataset<Row> dataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withStructTypeColumns(codingStructType())
        .buildWithStructValue();

    final ElementPath argument = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.CODING)
        .dataset(dataset)
        .idAndEidAndValueColumns()
        .build();

    return (CodingPath) argument;
  }

  CodingPath createNullCodingInput() {
    final Dataset<Row> dataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withIdEidValueRows(ALL_RES_IDS, id -> null, id -> null)
        .withStructTypeColumns(codingStructType())
        .buildWithStructValue();

    final ElementPath argument = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.CODING)
        .dataset(dataset)
        .idAndEidAndValueColumns()
        .build();

    return (CodingPath) argument;
  }

  ElementPath createEmptyCodeableConceptInput() {
    final Dataset<Row> dataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withStructTypeColumns(codeableConceptStructType())
        .buildWithStructValue();

    return new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.CODEABLECONCEPT)
        .dataset(dataset)
        .idAndEidAndValueColumns()
        .build();
  }

  ElementPath createNullCodeableConceptInput() {
    final Dataset<Row> dataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withIdEidValueRows(ALL_RES_IDS, id -> null, id -> null)
        .withStructTypeColumns(codeableConceptStructType())
        .buildWithStructValue();

    return new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.CODEABLECONCEPT)
        .dataset(dataset)
        .idAndEidAndValueColumns()
        .build();
  }

  CodingPath createNullCodingArg() {
    final Dataset<Row> dataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withStructTypeColumns(codingStructType())
        .withIdValueRows(ALL_RES_IDS, id -> null)
        .buildWithStructValue();

    final ElementPath argument = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.CODING)
        .dataset(dataset)
        .idAndValueColumns()
        .build();

    return (CodingPath) argument;
  }

  DatasetBuilder expectedSubsumes() {
    return new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.BooleanType)
        .withRow(RES_ID1, makeEid(0), false)
        .withRow(RES_ID1, makeEid(1), false)
        .withRow(RES_ID2, makeEid(0), false)
        .withRow(RES_ID2, makeEid(1), true)
        .withRow(RES_ID3, makeEid(0), false)
        .withRow(RES_ID3, makeEid(1), true)
        .withRow(RES_ID4, makeEid(0), false)
        .withRow(RES_ID4, makeEid(1), false)
        .withRow(RES_ID5, null, null);
  }

  DatasetBuilder expectedSubsumedBy() {
    return new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.BooleanType)
        .withRow(RES_ID1, makeEid(0), false)
        .withRow(RES_ID1, makeEid(1), true)
        .withRow(RES_ID2, makeEid(0), false)
        .withRow(RES_ID2, makeEid(1), true)
        .withRow(RES_ID3, makeEid(0), false)
        .withRow(RES_ID3, makeEid(1), false)
        .withRow(RES_ID4, makeEid(0), false)
        .withRow(RES_ID4, makeEid(1), false)
        .withRow(RES_ID5, null, null);
  }

  @Nonnull
  DatasetBuilder expectedAllNonNull(final boolean result) {
    return new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.BooleanType)
        .withRow(RES_ID1, makeEid(0), result)
        .withRow(RES_ID1, makeEid(1), result)
        .withRow(RES_ID2, makeEid(0), result)
        .withRow(RES_ID2, makeEid(1), result)
        .withRow(RES_ID3, makeEid(0), result)
        .withRow(RES_ID3, makeEid(1), result)
        .withRow(RES_ID4, makeEid(0), result)
        .withRow(RES_ID4, makeEid(1), result)
        .withRow(RES_ID5, null, null);
  }

  @Nonnull
  DatasetBuilder expectedEmpty() {
    return new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.BooleanType);
  }

  @Nonnull
  DatasetBuilder expectedNull() {
    return new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withIdEidValueRows(ALL_RES_IDS, id -> null, id -> null)
        .withColumn(DataTypes.BooleanType);
  }

  ElementPathAssertion assertCallSuccess(final NamedFunction function,
      final NonLiteralPath inputExpression, final FhirPath argumentExpression) {
    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .terminologyClientFactory(terminologyServiceFactory)
        .build();

    final NamedFunctionInput functionInput = new NamedFunctionInput(parserContext, inputExpression,
        Collections.singletonList(argumentExpression));
    final FhirPath result = function.invoke(functionInput);

    return assertThat(result)
        .isElementPath(BooleanPath.class)
        .preservesCardinalityOf(inputExpression);
  }

  DatasetAssert assertSubsumesSuccess(final NonLiteralPath inputExpression,
      final FhirPath argumentExpression) {
    return assertCallSuccess(NamedFunction.getInstance("subsumes"), inputExpression,
        argumentExpression).selectOrderedResultWithEid();
  }

  DatasetAssert assertSubsumedBySuccess(final NonLiteralPath inputExpression,
      final FhirPath argumentExpression) {
    return assertCallSuccess(NamedFunction.getInstance("subsumedBy"), inputExpression,
        argumentExpression).selectOrderedResultWithEid();
  }

  //
  // Test subsumes on selected pairs of argument types
  // (Coding, CodingLiteral) && (CodeableConcept, Coding) && (Literal, CodeableConcept)
  // plus (Coding, Coding)
  //
  @Test
  void testSubsumesCodingWithLiteralCorrectly() {
    assertSubsumesSuccess(createCodingInput(), createLiteralArgOrInput())
        .hasRows(expectedSubsumes());
  }

  @Test
  void testSubsumesCodeableConceptWithCodingCorrectly() {
    assertSubsumesSuccess(createCodeableConceptInput(), createCodingArg())
        .hasRows(expectedSubsumes());
  }

  @Test
  void testSubsumesCodingWithCodingCorrectly() {
    assertSubsumesSuccess(createCodingInput(), createCodingArg()).hasRows(expectedSubsumes());
  }

  //
  // Test subsumedBy on selected pairs of argument types
  // (Coding, CodeableConcept) && (CodeableConcept, Literal) && (Literal, Coding)
  // plus (CodeableConcept, CodeableConcept)
  //

  @Test
  void testSubsumedByCodingWithCodeableConceptCorrectly() {

    assertSubsumedBySuccess(createCodingInput(), createCodeableConceptArg())
        .hasRows(expectedSubsumedBy());
  }

  @Test
  void testSubsumedByCodeableConceptWithLiteralCorrectly() {
    assertSubsumedBySuccess(createCodeableConceptInput(), createLiteralArgOrInput())
        .hasRows(expectedSubsumedBy());
  }

  @Test
  void testSubsumedByCodeableConceptWithCodeableConceptCorrectly() {
    // call subsumedBy but expect subsumes result
    // because input is switched with argument
    assertSubsumedBySuccess(createCodeableConceptInput(), createCodeableConceptArg())
        .hasRows(expectedSubsumedBy());
  }

  //
  // Test against nulls
  //

  @Test
  void testAllFalseWhenSubsumesNullCoding() {
    assertSubsumesSuccess(createCodingInput(), createNullCodingArg())
        .hasRows(expectedAllNonNull(false));
  }

  @Test
  void testAllFalseWhenSubsumedByNullCoding() {
    assertSubsumedBySuccess(createCodeableConceptInput(), createNullCodingArg())
        .hasRows(expectedAllNonNull(false));
  }


  @Test
  void testAllNonNullTrueWhenSubsumesItself() {

    final DatasetBuilder expectedResult = new DatasetBuilder(spark)
        .withIdColumn()
        .withEidColumn()
        .withColumn(DataTypes.BooleanType)
        .withRow(RES_ID1, null, true)
        .withRow(RES_ID2, null, true)
        .withRow(RES_ID3, null, true)
        .withRow(RES_ID4, null, true)
        .withRow(RES_ID5, null, null);

    assertSubsumesSuccess(createSingularCodingInput(), createSingularCodingInput())
        .hasRows(expectedResult);
  }


  @Test
  void testAllNonNullTrueSubsumedByItself() {
    assertSubsumedBySuccess(createCodeableConceptInput(), createCodeableConceptInput())
        .hasRows(expectedAllNonNull(true));
  }

  @Test
  void testEmptyCodingInput() {
    assertSubsumedBySuccess(createEmptyCodingInput(), createCodingArg())
        .hasRows(expectedEmpty());
  }

  @Test
  void testNullCodingInput() {
    assertSubsumedBySuccess(createNullCodingInput(), createCodingArg())
        .hasRows(expectedNull());
  }

  @Test
  void testEmptyCodeableConceptInput() {
    assertSubsumedBySuccess(createEmptyCodeableConceptInput(), createCodingArg())
        .hasRows(expectedEmpty());
  }

  @Test
  void testNullCodeableConceptInput() {
    assertSubsumedBySuccess(createNullCodeableConceptInput(), createCodingArg())
        .hasRows(expectedNull());
  }

  //
  // Test for various validation errors
  //

  @Test
  void throwsErrorIfInputTypeIsUnsupported() {
    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .terminologyClientFactory(mock(TerminologyServiceFactory.class))
        .build();

    final ElementPath argument = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.CODEABLECONCEPT)
        .build();

    final ElementPath input = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.STRING)
        .build();

    final NamedFunctionInput functionInput = new NamedFunctionInput(parserContext, input,
        Collections.singletonList(argument));
    final NamedFunction subsumesFunction = NamedFunction.getInstance("subsumedBy");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> subsumesFunction.invoke(functionInput));
    assertEquals(
        "subsumedBy function accepts input of type Coding or CodeableConcept",
        error.getMessage());
  }

  @Test
  void throwsErrorIfArgumentTypeIsUnsupported() {
    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .terminologyClientFactory(mock(TerminologyServiceFactory.class))
        .build();

    final ElementPath input = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.CODEABLECONCEPT)
        .build();
    final StringLiteralPath argument = StringLiteralPath
        .fromString("'str'", input);

    final NamedFunctionInput functionInput = new NamedFunctionInput(parserContext, input,
        Collections.singletonList(argument));
    final NamedFunction subsumesFunction = NamedFunction.getInstance("subsumes");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> subsumesFunction.invoke(functionInput));
    assertEquals("subsumes function accepts argument of type Coding or CodeableConcept",
        error.getMessage());
  }


  @Test
  void throwsErrorIfMoreThanOneArgument() {

    final ParserContext parserContext = new ParserContextBuilder(spark, fhirContext)
        .terminologyClientFactory(mock(TerminologyServiceFactory.class))
        .build();

    final ElementPath input = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.CODEABLECONCEPT)
        .build();

    final CodingLiteralPath argument1 = CodingLiteralPath
        .fromString(CODING_MEDIUM.getSystem() + "|" + CODING_MEDIUM.getCode(),
            input);

    final CodingLiteralPath argument2 = CodingLiteralPath
        .fromString(CODING_MEDIUM.getSystem() + "|" + CODING_MEDIUM.getCode(),
            input);

    final NamedFunctionInput functionInput = new NamedFunctionInput(parserContext, input,
        Arrays.asList(argument1, argument2));
    final NamedFunction subsumesFunction = NamedFunction.getInstance("subsumes");
    final InvalidUserInputError error = assertThrows(
        InvalidUserInputError.class,
        () -> subsumesFunction.invoke(functionInput));
    assertEquals("subsumes function accepts one argument of type Coding or CodeableConcept",
        error.getMessage());
  }

  @Test
  void throwsErrorIfTerminologyServiceNotConfigured() {
    final ElementPath input = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.CODEABLECONCEPT)
        .build();

    final CodingLiteralPath argument = CodingLiteralPath
        .fromString(CODING_MEDIUM.getSystem() + "|" + CODING_MEDIUM.getCode(),
            input);

    final ParserContext context = new ParserContextBuilder(spark, fhirContext).build();

    final NamedFunctionInput functionInput = new NamedFunctionInput(context, input,
        Collections.singletonList(argument));

    final InvalidUserInputError error = assertThrows(InvalidUserInputError.class,
        () -> new SubsumesFunction().invoke(functionInput));
    assertEquals(
        "Attempt to call terminology function subsumes when terminology service has not been configured",
        error.getMessage());
  }
}
