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

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.helpers.SparkHelpers.quantityStructType;
import static au.csiro.pathling.test.helpers.SparkHelpers.rowForUcumQuantity;
import static au.csiro.pathling.test.helpers.SparkHelpers.rowFromQuantity;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.literal.QuantityLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import au.csiro.pathling.test.helpers.TestHelpers;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.fhir.ucum.UcumService;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Quantity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
public class MathOperatorQuantityTest {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirContext fhirContext;

  @Autowired
  UcumService ucumService;

  static final List<String> OPERATORS = List.of("+", "-", "*", "/");
  static final String ID_ALIAS = "_abc123";

  @Value
  static class TestParameters {

    @Nonnull
    String name;

    @Nonnull
    FhirPath left;

    @Nonnull
    FhirPath right;

    @Nonnull
    ParserContext context;

    @Nonnull
    Operator operator;

    @Nonnull
    Dataset<Row> expectedResult;

    @Override
    public String toString() {
      return name;
    }

  }

  @Nonnull
  Stream<TestParameters> parameters() {
    final Collection<TestParameters> parameters = new ArrayList<>();
    for (final String operator : OPERATORS) {
      final String name = "Quantity " + operator + " Quantity";
      final FhirPath left = buildQuantityExpression(true);
      final FhirPath right = buildQuantityExpression(false);
      final ParserContext context = new ParserContextBuilder(spark, fhirContext)
          .groupingColumns(Collections.singletonList(left.getIdColumn()))
          .build();
      parameters.add(new TestParameters(name, left, right, context, Operator.getInstance(operator),
          expectedResult(operator)));
    }
    return parameters.stream();
  }

  Dataset<Row> expectedResult(@Nonnull final String operator) {
    final Row result1;
    final Row result2;

    switch (operator) {
      case "+":
        result1 = rowForUcumQuantity(new BigDecimal("1650.0"), "m");
        result2 = null;
        break;
      case "-":
        result1 = rowForUcumQuantity(new BigDecimal("-1350.0"), "m");
        result2 = null;
        break;
      case "*":
        result1 = null;
        result2 = rowForUcumQuantity("1000.0", "g");
        break;
      case "/":
        result1 = rowForUcumQuantity(new BigDecimal("0.1"), "1");
        result2 = rowForUcumQuantity("4000", "g");
        break;
      default:
        result1 = null;
        result2 = null;
    }
    return new DatasetBuilder(spark)
        .withIdColumn(ID_ALIAS)
        .withStructTypeColumns(quantityStructType())
        .withRow("patient-1", result1)
        .withRow("patient-2", result2)
        .withRow("patient-3", null)
        .withRow("patient-4", null)
        .withRow("patient-5", null)
        .withRow("patient-6", null)
        .withRow("patient-7", null)
        .buildWithStructValue();
  }

  @Nonnull
  FhirPath buildQuantityExpression(final boolean leftOperand) {
    final Quantity nonUcumQuantity = new Quantity();
    nonUcumQuantity.setValue(15);
    nonUcumQuantity.setUnit("mSv");
    nonUcumQuantity.setSystem(TestHelpers.SNOMED_URL);
    nonUcumQuantity.setCode("282250007");

    final Dataset<Row> dataset = new DatasetBuilder(spark)
        .withIdColumn(ID_ALIAS)
        .withStructTypeColumns(quantityStructType())
        .withRow("patient-1", leftOperand
                              ? rowForUcumQuantity(new BigDecimal("150.0"), "m")
                              : rowForUcumQuantity(new BigDecimal("1.5"), "km"))
        .withRow("patient-2", leftOperand
                              ? rowForUcumQuantity(new BigDecimal("2.0"), "kg")
                              : rowForUcumQuantity(new BigDecimal("0.5"), "1"))
        .withRow("patient-3", leftOperand
                              ? rowForUcumQuantity(new BigDecimal("7.7"), "mSv")
                              : rowForUcumQuantity(new BigDecimal("1.5"), "h"))
        // Not comparable
        .withRow("patient-4", leftOperand
                              ? rowForUcumQuantity(new BigDecimal("7.7"), "mSv")
                              : rowFromQuantity(nonUcumQuantity))
        // Not comparable
        .withRow("patient-5", leftOperand
                              ? null
                              : rowForUcumQuantity(new BigDecimal("1.5"), "h"))
        .withRow("patient-6", leftOperand
                              ? rowForUcumQuantity(new BigDecimal("7.7"), "mSv")
                              : null)
        .withRow("patient-7", null)
        .buildWithStructValue();
    return new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.QUANTITY)
        .dataset(dataset)
        .idAndValueColumns()
        .singular(true)
        .build();
  }

  @ParameterizedTest
  @MethodSource("parameters")
  void test(@Nonnull final TestParameters parameters) {
    final OperatorInput input = new OperatorInput(parameters.getContext(),
        parameters.getLeft(), parameters.getRight());
    final FhirPath result = parameters.getOperator().invoke(input);
    assertThat(result).selectOrderedResult().hasRows(parameters.getExpectedResult());
  }

  /*
   * This test requires a very high precision decimal representation, due to these units being
   * canonicalized by UCUM to "reciprocal cubic meters".
   * See: https://www.wolframalpha.com/input?i=3+mmol%2FL+in+m%5E-3
   */
  @Test
  void volumeArithmetic() {
    final Dataset<Row> rightDataset = new DatasetBuilder(spark)
        .withIdColumn(ID_ALIAS)
        .withStructTypeColumns(quantityStructType())
        .withRow("patient-1", rowForUcumQuantity(new BigDecimal("1.0"), "mmol/L"))
        .buildWithStructValue();
    final FhirPath right = new ElementPathBuilder(spark)
        .expression("valueQuantity")
        .fhirType(FHIRDefinedType.QUANTITY)
        .singular(true)
        .dataset(rightDataset)
        .idAndValueColumns()
        .build();
    final FhirPath left = QuantityLiteralPath.fromUcumString("1.0 'mmol/L'", right,
        ucumService);
    final ParserContext context = new ParserContextBuilder(spark, fhirContext)
        .groupingColumns(Collections.singletonList(left.getIdColumn()))
        .build();
    final OperatorInput input = new OperatorInput(context, left, right);
    final FhirPath result = Operator.getInstance("+").invoke(input);

    final Dataset<Row> expectedDataset = new DatasetBuilder(spark)
        .withIdColumn()
        .withStructTypeColumns(quantityStructType())
        .withRow("patient-1",
            rowForUcumQuantity(new BigDecimal("1204427340000000000000000"), "m-3"))
        .buildWithStructValue();
    assertThat(result).selectOrderedResult().hasRows(expectedDataset);
  }

}
