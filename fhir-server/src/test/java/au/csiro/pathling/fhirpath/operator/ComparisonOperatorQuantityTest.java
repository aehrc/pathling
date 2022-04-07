/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.operator;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static au.csiro.pathling.test.helpers.SparkHelpers.quantityStructType;
import static au.csiro.pathling.test.helpers.SparkHelpers.rowFromQuantity;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.literal.QuantityLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import au.csiro.pathling.test.helpers.TestHelpers;
import ca.uhn.fhir.context.FhirContext;
import java.util.Collections;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.fhir.ucum.UcumService;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Quantity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
@Tag("UnitTest")
public class ComparisonOperatorQuantityTest {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirContext fhirContext;

  @Autowired
  UcumService ucumService;

  static final String ID_ALIAS = "_abc123";

  FhirPath left;
  FhirPath right;
  ParserContext parserContext;
  QuantityLiteralPath ucumQuantityLiteral1;
  QuantityLiteralPath ucumQuantityLiteral2;
  QuantityLiteralPath ucumQuantityLiteral3;
  QuantityLiteralPath calendarDurationLiteral1;
  QuantityLiteralPath calendarDurationLiteral2;
  QuantityLiteralPath calendarDurationLiteral3;

  @BeforeEach
  void setUp() {
    final Quantity quantity1 = new Quantity();
    quantity1.setValue(500);
    quantity1.setUnit("mg");
    quantity1.setSystem(TestHelpers.UCUM_URL);
    quantity1.setCode("mg");

    final Quantity quantity2 = new Quantity();
    quantity2.setValue(0.5);
    quantity2.setUnit("g");
    quantity2.setSystem(TestHelpers.UCUM_URL);
    quantity2.setCode("g");

    final Quantity quantity3 = new Quantity();
    quantity3.setValue(1.8);
    quantity3.setUnit("m");
    quantity3.setSystem(TestHelpers.UCUM_URL);
    quantity3.setCode("m");

    final Quantity quantity4 = new Quantity();
    quantity4.setValue(0.5);
    quantity4.setUnit("g");
    quantity4.setSystem(TestHelpers.SNOMED_URL);
    quantity4.setCode("258682000");

    final Quantity quantity5 = new Quantity();
    quantity1.setValue(650);
    quantity1.setUnit("mg");
    quantity1.setSystem(TestHelpers.UCUM_URL);
    quantity1.setCode("mg");

    final Quantity quantity6 = new Quantity();
    quantity1.setValue(30);
    quantity1.setUnit("d");
    quantity1.setSystem(TestHelpers.UCUM_URL);
    quantity1.setCode("d");

    final Quantity quantity7 = new Quantity();
    quantity1.setValue(60);
    quantity1.setUnit("s");
    quantity1.setSystem(TestHelpers.UCUM_URL);
    quantity1.setCode("s");

    final Quantity quantity8 = new Quantity();
    quantity1.setValue(1000);
    quantity1.setUnit("ms");
    quantity1.setSystem(TestHelpers.UCUM_URL);
    quantity1.setCode("ms");

    final Quantity quantity9 = new Quantity();
    quantity2.setValue(0.2);
    quantity2.setUnit("g");
    quantity2.setSystem(TestHelpers.UCUM_URL);
    quantity2.setCode("g");

    final Dataset<Row> leftDataset = new DatasetBuilder(spark)
        .withIdColumn(ID_ALIAS)
        .withStructTypeColumns(quantityStructType())
        .withRow("patient-1", rowFromQuantity(quantity1))  // 500 mg
        .withRow("patient-2", rowFromQuantity(quantity1))  // 500 mg
        .withRow("patient-3", rowFromQuantity(quantity1))  // 500 mg
        .withRow("patient-4", rowFromQuantity(quantity5))  // 650 mg
        .withRow("patient-5", null)
        .withRow("patient-6", rowFromQuantity(quantity1))  // 500 mg
        .withRow("patient-7", rowFromQuantity(quantity6))  // 30 d
        .withRow("patient-8", rowFromQuantity(quantity7))  // 60 s
        .withRow("patient-9", rowFromQuantity(quantity8))  // 1000 ms
        .withRow("patient-10", rowFromQuantity(quantity9)) // 0.2 g
        .buildWithStructValue();
    left = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.QUANTITY)
        .singular(true)
        .dataset(leftDataset)
        .idAndValueColumns()
        .build();

    final Dataset<Row> rightDataset = new DatasetBuilder(spark)
        .withIdColumn(ID_ALIAS)
        .withStructTypeColumns(quantityStructType())
        .withRow("patient-1", rowFromQuantity(quantity1))  // 500 mg
        .withRow("patient-2", rowFromQuantity(quantity2))  // 0.5 g
        .withRow("patient-3", rowFromQuantity(quantity3))  // 1.8 m
        .withRow("patient-4", rowFromQuantity(quantity1))  // 500 mg
        .withRow("patient-5", rowFromQuantity(quantity1))  // 500 mg
        .withRow("patient-6", null)
        .withRow("patient-7", rowFromQuantity(quantity6))  // 30 d
        .withRow("patient-8", rowFromQuantity(quantity7))  // 60 s
        .withRow("patient-9", rowFromQuantity(quantity8))  // 1000 ms
        .withRow("patient-10", rowFromQuantity(quantity1)) // 500 mg
        .buildWithStructValue();
    right = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.QUANTITY)
        .singular(true)
        .dataset(rightDataset)
        .idAndValueColumns()
        .build();

    ucumQuantityLiteral1 = QuantityLiteralPath.fromUcumString("500 'mg'", left, ucumService);
    ucumQuantityLiteral2 = QuantityLiteralPath.fromUcumString("0.5 'g'", left, ucumService);
    ucumQuantityLiteral3 = QuantityLiteralPath.fromUcumString("1.8 'm'", left, ucumService);
    calendarDurationLiteral1 = QuantityLiteralPath.fromCalendarDurationString("30 days", left);
    calendarDurationLiteral2 = QuantityLiteralPath.fromCalendarDurationString("60 seconds", left);
    calendarDurationLiteral3 = QuantityLiteralPath.fromCalendarDurationString("1000 milliseconds",
        left);

    parserContext = new ParserContextBuilder(spark, fhirContext)
        .groupingColumns(Collections.singletonList(left.getIdColumn()))
        .build();
  }

  @Test
  void lessThan() {
    final OperatorInput input = new OperatorInput(parserContext, left, right);
    final Operator equalityOperator = Operator.getInstance("<");
    final FhirPath result = equalityOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", false),  // 500 mg < 500 mg
        RowFactory.create("patient-2", false),  // 500 mg < 0.5 g
        RowFactory.create("patient-3", null),  // 500 mg < 1.8 m
        RowFactory.create("patient-4", false), // 650 mg < 500 mg
        RowFactory.create("patient-5", null),  // {} < 500 mg
        RowFactory.create("patient-6", null),  // 500 mg < {}
        RowFactory.create("patient-7", false),  // 30 d < 30 d
        RowFactory.create("patient-8", false),  // 60 s < 60 s
        RowFactory.create("patient-9", false),  // 1000 ms < 1000 ms
        RowFactory.create("patient-10", true)  // 0.2 g < 500 mg
    );
  }

  @Test
  void lessThanOrEqualTo() {
    final OperatorInput input = new OperatorInput(parserContext, left, right);
    final Operator equalityOperator = Operator.getInstance("<=");
    final FhirPath result = equalityOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", true),  // 500 mg <= 500 mg
        RowFactory.create("patient-2", true),  // 500 mg <= 0.5 g
        RowFactory.create("patient-3", null),  // 500 mg <= 1.8 m
        RowFactory.create("patient-4", false), // 650 mg <= 500 mg
        RowFactory.create("patient-5", null),  // {} <= 500 mg
        RowFactory.create("patient-6", null),  // 500 mg <= {}
        RowFactory.create("patient-7", true),  // 30 d <= 30 d
        RowFactory.create("patient-8", true),  // 60 s <= 60 s
        RowFactory.create("patient-9", true),  // 1000 ms <= 1000 ms
        RowFactory.create("patient-10", true)  // 0.2 g <= 500 mg
    );
  }

  @Test
  void greaterThanOrEqualTo() {
    final OperatorInput input = new OperatorInput(parserContext, left, right);
    final Operator equalityOperator = Operator.getInstance(">=");
    final FhirPath result = equalityOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", true),   // 500 mg >= 500 mg
        RowFactory.create("patient-2", true),   // 500 mg >= 0.5 g
        RowFactory.create("patient-3", null),   // 500 mg >= 1.8 m
        RowFactory.create("patient-4", true),   // 650 mg >= 500 mg
        RowFactory.create("patient-5", null),   // {} >= 500 mg
        RowFactory.create("patient-6", null),   // 500 mg >= {}
        RowFactory.create("patient-7", true),   // 30 d >= 30 d
        RowFactory.create("patient-8", true),   // 60 s >= 60 s
        RowFactory.create("patient-9", true),   // 1000 ms >= 1000 ms
        RowFactory.create("patient-10", false)  // 0.2 g >= 500 mg
    );
  }

  @Test
  void greaterThan() {
    final OperatorInput input = new OperatorInput(parserContext, left, right);
    final Operator equalityOperator = Operator.getInstance(">");
    final FhirPath result = equalityOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", false),  // 500 mg > 500 mg
        RowFactory.create("patient-2", false),  // 500 mg > 0.5 g
        RowFactory.create("patient-3", null),   // 500 mg > 1.8 m
        RowFactory.create("patient-4", true),   // 650 mg > 500 mg
        RowFactory.create("patient-5", null),   // {} > 500 mg
        RowFactory.create("patient-6", null),   // 500 mg > {}
        RowFactory.create("patient-7", false),  // 30 d > 30 d
        RowFactory.create("patient-8", false),  // 60 s > 60 s
        RowFactory.create("patient-9", false),  // 1000 ms > 1000 ms
        RowFactory.create("patient-10", false)  // 0.2 g > 500 mg
    );
  }

}
