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
import au.csiro.pathling.fhirpath.literal.CalendarDurationLiteralPath;
import au.csiro.pathling.fhirpath.literal.UcumQuantityLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import au.csiro.pathling.test.helpers.TestHelpers;
import ca.uhn.fhir.context.FhirContext;
import java.util.Collections;
import java.util.List;
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
public class EqualityOperatorQuantityTest {

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
  UcumQuantityLiteralPath ucumQuantityLiteral1;
  UcumQuantityLiteralPath ucumQuantityLiteral2;
  UcumQuantityLiteralPath ucumQuantityLiteral3;
  CalendarDurationLiteralPath calendarDurationLiteral1;
  CalendarDurationLiteralPath calendarDurationLiteral2;
  CalendarDurationLiteralPath calendarDurationLiteral3;

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
        .buildWithStructValue();
    right = new ElementPathBuilder(spark)
        .fhirType(FHIRDefinedType.QUANTITY)
        .singular(true)
        .dataset(rightDataset)
        .idAndValueColumns()
        .build();

    ucumQuantityLiteral1 = UcumQuantityLiteralPath.fromString("500 'mg'", left, ucumService);
    ucumQuantityLiteral2 = UcumQuantityLiteralPath.fromString("0.5 'g'", left, ucumService);
    ucumQuantityLiteral3 = UcumQuantityLiteralPath.fromString("1.8 'm'", left, ucumService);
    calendarDurationLiteral1 = CalendarDurationLiteralPath.fromString("30 days", left);
    calendarDurationLiteral2 = CalendarDurationLiteralPath.fromString("60 seconds", left);
    calendarDurationLiteral3 = CalendarDurationLiteralPath.fromString("1000 milliseconds", left);

    parserContext = new ParserContextBuilder(spark, fhirContext)
        .groupingColumns(Collections.singletonList(left.getIdColumn()))
        .build();
  }

  @Test
  void equals() {
    final OperatorInput input = new OperatorInput(parserContext, left, right);
    final Operator equalityOperator = Operator.getInstance("=");
    final FhirPath result = equalityOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", true),  // 500 mg = 500 mg
        RowFactory.create("patient-2", true),  // 500 mg = 0.5 g
        RowFactory.create("patient-3", null),  // 500 mg = 1.8 m
        RowFactory.create("patient-4", false), // 650 mg = 500 mg
        RowFactory.create("patient-5", null),  // {} = 500 mg
        RowFactory.create("patient-6", null),  // 500 mg = {}
        RowFactory.create("patient-7", true),  // 30 d = 30 d
        RowFactory.create("patient-8", true),  // 60 s = 60 s
        RowFactory.create("patient-9", true)   // 1000 ms = 1000 ms
    );
  }

  @Test
  void notEquals() {
    final OperatorInput input = new OperatorInput(parserContext, left, right);
    final Operator equalityOperator = Operator.getInstance("!=");
    final FhirPath result = equalityOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", false),  // 500 mg != 500 mg
        RowFactory.create("patient-2", false),  // 500 mg != 0.5 g
        RowFactory.create("patient-3", null),   // 500 mg != 1.8 m
        RowFactory.create("patient-4", true),   // 650 mg != 500 mg
        RowFactory.create("patient-5", null),   // {} != 500 mg
        RowFactory.create("patient-6", null),   // 500 mg != {}
        RowFactory.create("patient-7", false),  // 30 d != 30 d
        RowFactory.create("patient-8", false),  // 60 s != 60 s
        RowFactory.create("patient-9", false)   // 1000 ms != 1000 ms
    );
  }

  @Test
  void ucumLiteralEquals() {
    OperatorInput input = new OperatorInput(parserContext, left, ucumQuantityLiteral1);
    final Operator equalityOperator = Operator.getInstance("=");
    FhirPath result = equalityOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", true),   // 500 mg = 500 mg
        RowFactory.create("patient-2", true),   // 500 mg = 500 mg
        RowFactory.create("patient-3", true),   // 500 mg = 500 mg
        RowFactory.create("patient-4", false),  // 650 mg = 500 mg
        RowFactory.create("patient-5", null),   // {} = 500 mg
        RowFactory.create("patient-6", true),   // 500 mg = 500 mg 
        RowFactory.create("patient-7", null),   // 30 d = 500 mg
        RowFactory.create("patient-8", null),   // 60 s = 500 mg
        RowFactory.create("patient-9", null)    // 1000 ms = 500 mg
    );

    input = new OperatorInput(parserContext, left, ucumQuantityLiteral2);
    result = equalityOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", true),   // 500 mg = 0.5 mg
        RowFactory.create("patient-2", true),   // 500 mg = 0.5 mg
        RowFactory.create("patient-3", true),   // 500 mg = 0.5 mg
        RowFactory.create("patient-4", false),  // 650 mg = 0.5 mg
        RowFactory.create("patient-5", null),   // {} = 0.5 mg
        RowFactory.create("patient-6", true),   // 500 mg = 0.5 mg 
        RowFactory.create("patient-7", null),   // 30 d = 0.5 mg
        RowFactory.create("patient-8", null),   // 60 s = 0.5 mg
        RowFactory.create("patient-9", null)    // 1000 ms = 0.5 mg
    );

    input = new OperatorInput(parserContext, left, ucumQuantityLiteral3);
    result = equalityOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", null),   // 500 mg = 1.8 m
        RowFactory.create("patient-2", null),   // 500 mg = 1.8 m
        RowFactory.create("patient-3", null),   // 500 mg = 1.8 m
        RowFactory.create("patient-4", null),   // 650 mg = 1.8 m
        RowFactory.create("patient-5", null),   // {} = 1.8 m
        RowFactory.create("patient-6", null),   // 500 mg = 1.8 m 
        RowFactory.create("patient-7", null),   // 30 d = 1.8 m
        RowFactory.create("patient-8", null),   // 60 s = 1.8 m
        RowFactory.create("patient-9", null)    // 1000 ms = 1.8 m
    );

    input = new OperatorInput(parserContext, ucumQuantityLiteral1, ucumQuantityLiteral1);
    result = equalityOperator.invoke(input);

    final Dataset<Row> allTrue = new DatasetBuilder(spark)
        .withIdsAndValue(true,
            List.of("patient-1", "patient-2", "patient-3", "patient-4", "patient-5", "patient-6",
                "patient-7", "patient-8", "patient-9"))
        .build();
    assertThat(result).selectOrderedResult().hasRows(allTrue);
  }

  @Test
  void ucumLiteralNotEquals() {
    OperatorInput input = new OperatorInput(parserContext, left, ucumQuantityLiteral1);
    final Operator equalityOperator = Operator.getInstance("!=");
    FhirPath result = equalityOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", false),   // 500 mg != 500 mg
        RowFactory.create("patient-2", false),   // 500 mg != 500 mg
        RowFactory.create("patient-3", false),   // 500 mg != 500 mg
        RowFactory.create("patient-4", true),    // 650 mg != 500 mg
        RowFactory.create("patient-5", null),    // {} != 500 mg
        RowFactory.create("patient-6", false),   // 500 mg != 500 mg 
        RowFactory.create("patient-7", null),    // 30 d != 500 mg
        RowFactory.create("patient-8", null),    // 60 s != 500 mg
        RowFactory.create("patient-9", null)     // 1000 ms != 500 mg
    );

    input = new OperatorInput(parserContext, left, ucumQuantityLiteral2);
    result = equalityOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", false),   // 500 mg != 0.5 mg
        RowFactory.create("patient-2", false),   // 500 mg != 0.5 mg
        RowFactory.create("patient-3", false),   // 500 mg != 0.5 mg
        RowFactory.create("patient-4", true),    // 650 mg != 0.5 mg
        RowFactory.create("patient-5", null),    // {} != 0.5 mg
        RowFactory.create("patient-6", false),   // 500 mg != 0.5 mg 
        RowFactory.create("patient-7", null),    // 30 d != 0.5 mg
        RowFactory.create("patient-8", null),    // 60 s != 0.5 mg
        RowFactory.create("patient-9", null)     // 1000 ms != 0.5 mg
    );

    input = new OperatorInput(parserContext, left, ucumQuantityLiteral3);
    result = equalityOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", null),   // 500 mg != 1.8 m
        RowFactory.create("patient-2", null),   // 500 mg != 1.8 m
        RowFactory.create("patient-3", null),   // 500 mg != 1.8 m
        RowFactory.create("patient-4", null),   // 650 mg != 1.8 m
        RowFactory.create("patient-5", null),   // {} != 1.8 m
        RowFactory.create("patient-6", null),   // 500 mg != 1.8 m 
        RowFactory.create("patient-7", null),   // 30 d != 1.8 m
        RowFactory.create("patient-8", null),   // 60 s != 1.8 m
        RowFactory.create("patient-9", null)    // 1000 ms != 1.8 m
    );

    input = new OperatorInput(parserContext, ucumQuantityLiteral1, ucumQuantityLiteral1);
    result = equalityOperator.invoke(input);

    final Dataset<Row> allFalse = new DatasetBuilder(spark)
        .withIdsAndValue(false,
            List.of("patient-1", "patient-2", "patient-3", "patient-4", "patient-5", "patient-6",
                "patient-7", "patient-8", "patient-9"))
        .build();
    assertThat(result).selectOrderedResult().hasRows(allFalse);
  }

  @Test
  void calendarLiteralEquals() {
    OperatorInput input = new OperatorInput(parserContext, left, calendarDurationLiteral1);
    final Operator equalityOperator = Operator.getInstance("=");
    FhirPath result = equalityOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", null),   // 500 mg = 30 days
        RowFactory.create("patient-2", null),   // 500 mg = 30 days
        RowFactory.create("patient-3", null),   // 500 mg = 30 days
        RowFactory.create("patient-4", null),   // 650 mg = 30 days
        RowFactory.create("patient-5", null),   // {} = 30 days
        RowFactory.create("patient-6", null),   // 500 mg = 30 days 
        RowFactory.create("patient-7", true),   // 30 d = 30 days
        RowFactory.create("patient-8", null),   // 60 s = 30 days
        RowFactory.create("patient-9", null)    // 1000 ms = 30 days
    );

    input = new OperatorInput(parserContext, left, calendarDurationLiteral2);
    result = equalityOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", null),   // 500 mg = 60 s
        RowFactory.create("patient-2", null),   // 500 mg = 60 s
        RowFactory.create("patient-3", null),   // 500 mg = 60 s
        RowFactory.create("patient-4", null),   // 650 mg = 60 s
        RowFactory.create("patient-5", null),   // {} = 60 s
        RowFactory.create("patient-6", null),   // 500 mg = 60 s 
        RowFactory.create("patient-7", null),   // 30 d = 60 s
        RowFactory.create("patient-8", true),   // 60 s = 60 s
        RowFactory.create("patient-9", false)   // 1000 ms = 60 s
    );

    input = new OperatorInput(parserContext, left, calendarDurationLiteral3);
    result = equalityOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", null),   // 500 mg = 1000 ms
        RowFactory.create("patient-2", null),   // 500 mg = 1000 ms
        RowFactory.create("patient-3", null),   // 500 mg = 1000 ms
        RowFactory.create("patient-4", null),   // 650 mg = 1000 ms
        RowFactory.create("patient-5", null),   // {} = 1000 ms
        RowFactory.create("patient-6", null),   // 500 mg = 1000 ms 
        RowFactory.create("patient-7", null),   // 30 d = 1000 ms
        RowFactory.create("patient-8", false),  // 60 s = 1000 ms
        RowFactory.create("patient-9", true)    // 1000 ms = 1000 ms
    );

    input = new OperatorInput(parserContext, calendarDurationLiteral1, calendarDurationLiteral1);
    result = equalityOperator.invoke(input);

    final Dataset<Row> allTrue = new DatasetBuilder(spark)
        .withIdsAndValue(true,
            List.of("patient-1", "patient-2", "patient-3", "patient-4", "patient-5", "patient-6",
                "patient-7", "patient-8", "patient-9"))
        .build();
    assertThat(result).selectOrderedResult().hasRows(allTrue);
  }

  @Test
  void calendarLiteralNotEquals() {
    OperatorInput input = new OperatorInput(parserContext, left, calendarDurationLiteral1);
    final Operator equalityOperator = Operator.getInstance("!=");
    FhirPath result = equalityOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", null),   // 500 mg != 30 days
        RowFactory.create("patient-2", null),   // 500 mg != 30 days
        RowFactory.create("patient-3", null),   // 500 mg != 30 days
        RowFactory.create("patient-4", null),   // 650 mg != 30 days
        RowFactory.create("patient-5", null),   // {} != 30 days
        RowFactory.create("patient-6", null),   // 500 mg != 30 days 
        RowFactory.create("patient-7", false),  // 30 d != 30 days
        RowFactory.create("patient-8", null),   // 60 s != 30 days
        RowFactory.create("patient-9", null)    // 1000 ms != 30 days
    );

    input = new OperatorInput(parserContext, left, calendarDurationLiteral2);
    result = equalityOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", null),   // 500 mg != 60 s
        RowFactory.create("patient-2", null),   // 500 mg != 60 s
        RowFactory.create("patient-3", null),   // 500 mg != 60 s
        RowFactory.create("patient-4", null),   // 650 mg != 60 s
        RowFactory.create("patient-5", null),   // {} != 60 s
        RowFactory.create("patient-6", null),   // 500 mg != 60 s 
        RowFactory.create("patient-7", null),   // 30 d != 60 s
        RowFactory.create("patient-8", false),  // 60 s != 60 s
        RowFactory.create("patient-9", true)    // 1000 ms != 60 s
    );

    input = new OperatorInput(parserContext, left, calendarDurationLiteral3);
    result = equalityOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", null),   // 500 mg != 1000 ms
        RowFactory.create("patient-2", null),   // 500 mg != 1000 ms
        RowFactory.create("patient-3", null),   // 500 mg != 1000 ms
        RowFactory.create("patient-4", null),   // 650 mg != 1000 ms
        RowFactory.create("patient-5", null),   // {} != 1000 ms
        RowFactory.create("patient-6", null),   // 500 mg != 1000 ms 
        RowFactory.create("patient-7", null),   // 30 d != 1000 ms
        RowFactory.create("patient-8", true),   // 60 s != 1000 ms
        RowFactory.create("patient-9", false)   // 1000 ms != 1000 ms
    );

    input = new OperatorInput(parserContext, calendarDurationLiteral1, calendarDurationLiteral1);
    result = equalityOperator.invoke(input);

    final Dataset<Row> allFalse = new DatasetBuilder(spark)
        .withIdsAndValue(false,
            List.of("patient-1", "patient-2", "patient-3", "patient-4", "patient-5", "patient-6",
                "patient-7", "patient-8", "patient-9"))
        .build();
    assertThat(result).selectOrderedResult().hasRows(allFalse);
  }

}
