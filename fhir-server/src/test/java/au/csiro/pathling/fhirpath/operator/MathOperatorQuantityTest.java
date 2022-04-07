/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.operator;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.helpers.SparkHelpers.quantityStructType;
import static au.csiro.pathling.test.helpers.SparkHelpers.rowForUcumQuantity;
import static au.csiro.pathling.test.helpers.SparkHelpers.rowFromQuantity;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import au.csiro.pathling.test.helpers.TestHelpers;
import ca.uhn.fhir.context.FhirContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Quantity;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
@TestInstance(Lifecycle.PER_CLASS)
@Tag("UnitTest")
public class MathOperatorQuantityTest {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirContext fhirContext;

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
    final Row result;
    switch (operator) {
      case "+":
        result = rowForUcumQuantity(1650.0, "m");
        break;
      case "-":
        result = rowForUcumQuantity(-1350.0, "m");
        break;
      case "*":
        result = rowForUcumQuantity(225000.0, "m");
        break;
      case "/":
        result = rowForUcumQuantity(0.1, "m");
        break;
      default:
        result = null;
    }
    return new DatasetBuilder(spark)
        .withIdColumn(ID_ALIAS)
        .withStructTypeColumns(quantityStructType())
        .withRow("patient-1", result)
        .withRow("patient-2", null)
        .withRow("patient-3", null)
        .withRow("patient-4", null)
        .withRow("patient-5", null)
        .withRow("patient-6", null)
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
                              ? rowForUcumQuantity(150.0, "m")
                              : rowForUcumQuantity(1.5, "km"))
        .withRow("patient-2", leftOperand
                              ? rowForUcumQuantity(7.7, "mSv")
                              : rowForUcumQuantity(1.5, "h"))  // Not comparable
        .withRow("patient-3", leftOperand
                              ? rowForUcumQuantity(7.7, "mSv")
                              : rowFromQuantity(nonUcumQuantity))         // Not comparable
        .withRow("patient-4", leftOperand
                              ? null
                              : rowForUcumQuantity(1.5, "h"))
        .withRow("patient-5", leftOperand
                              ? rowForUcumQuantity(7.7, "mSv")
                              : null)
        .withRow("patient-6", null)
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

}
