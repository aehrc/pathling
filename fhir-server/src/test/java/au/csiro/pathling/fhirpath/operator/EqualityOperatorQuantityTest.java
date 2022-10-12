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
import au.csiro.pathling.fhirpath.literal.QuantityLiteralPath;
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
import org.apache.spark.sql.types.DataTypes;
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
  QuantityLiteralPath ucumQuantityLiteral1;
  QuantityLiteralPath ucumQuantityLiteral2;
  QuantityLiteralPath ucumQuantityLiteral3;
  QuantityLiteralPath calendarDurationLiteral1;
  QuantityLiteralPath calendarDurationLiteral2;
  QuantityLiteralPath calendarDurationLiteral3;
  QuantityLiteralPath ucumQuantityLiteralNoUnit;

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
    quantity5.setValue(650);
    quantity5.setUnit("mg");
    quantity5.setSystem(TestHelpers.UCUM_URL);
    quantity5.setCode("mg");

    final Quantity quantity6 = new Quantity();
    quantity6.setValue(30);
    quantity6.setUnit("d");
    quantity6.setSystem(TestHelpers.UCUM_URL);
    quantity6.setCode("d");

    final Quantity quantity7 = new Quantity();
    quantity7.setValue(60);
    quantity7.setUnit("s");
    quantity7.setSystem(TestHelpers.UCUM_URL);
    quantity7.setCode("s");

    final Quantity quantity8 = new Quantity();
    quantity8.setValue(1000);
    quantity8.setUnit("ms");
    quantity8.setSystem(TestHelpers.UCUM_URL);
    quantity8.setCode("ms");

    final Quantity nonUcumQuantity = new Quantity();
    nonUcumQuantity.setValue(15);
    nonUcumQuantity.setUnit("mSv");
    nonUcumQuantity.setSystem(TestHelpers.SNOMED_URL);
    nonUcumQuantity.setCode("282250007");

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
        .withRow("patient-a", rowForUcumQuantity("1000", "mmol"))  // 1000 mmol
        .withRow("patient-b", rowForUcumQuantity("49", "%"))  // 49 %
        .withRow("patient-c", rowFromQuantity(nonUcumQuantity))  // non-ucum
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
        .withRow("patient-a", rowForUcumQuantity("1", "mol"))  // 1 mol
        .withRow("patient-b", rowForUcumQuantity("0.5", "1"))  // 0.5 '1'
        .withRow("patient-c", rowForUcumQuantity("1", "%"))  // 1 %
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
    ucumQuantityLiteralNoUnit = QuantityLiteralPath.fromUcumString("0.49 '1'", left, ucumService);

    calendarDurationLiteral1 = QuantityLiteralPath.fromCalendarDurationString("30 days", left);
    calendarDurationLiteral2 = QuantityLiteralPath.fromCalendarDurationString("60 seconds", left);
    calendarDurationLiteral3 = QuantityLiteralPath.fromCalendarDurationString("1000 milliseconds",
        left);

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
        RowFactory.create("patient-9", true),  // 1000 ms = 1000 ms
        RowFactory.create("patient-a", true),  // 1000 mmol = 1 mol
        RowFactory.create("patient-b", false), // 49 % = 0.5 ''
        RowFactory.create("patient-c", null)   //  non-ucum = 1 %
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
        RowFactory.create("patient-9", false),  // 1000 ms != 1000 ms
        RowFactory.create("patient-a", false),  // 1000 mmol != 1 mol
        RowFactory.create("patient-b", true),   // 49 % != 0.5 ''
        RowFactory.create("patient-c", null)    //  non-ucum != 1 %
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
        RowFactory.create("patient-9", null),   // 1000 ms = 500 mg
        RowFactory.create("patient-a", null),   // 1000 mmol = 500 mg
        RowFactory.create("patient-b", null),   // 49 % = 500 mg
        RowFactory.create("patient-c", null)    // non-ucum = 500 mg
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
        RowFactory.create("patient-9", null),   // 1000 ms = 0.5 mg
        RowFactory.create("patient-a", null),   // 1000 mmol = 0.5 mg
        RowFactory.create("patient-b", null),   // 49 % = 0.5 mg
        RowFactory.create("patient-c", null)    //  non-ucum = 0.5 mg        
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
        RowFactory.create("patient-9", null),   // 1000 ms = 1.8 m
        RowFactory.create("patient-a", null),   // 1000 mmol = 1.8 m
        RowFactory.create("patient-b", null),   // 49 %  = 1.8 m
        RowFactory.create("patient-c", null)    //  non-ucum = 1.8 m   
    );

    input = new OperatorInput(parserContext, left, ucumQuantityLiteralNoUnit);
    result = equalityOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", null),   // 500 mg = 0.49 '1'
        RowFactory.create("patient-2", null),   // 500 mg = 0.49 '1'
        RowFactory.create("patient-3", null),   // 500 mg = 0.49 '1'
        RowFactory.create("patient-4", null),   // 650 mg = 0.49 '1'
        RowFactory.create("patient-5", null),   // {} = 0.49 '1'
        RowFactory.create("patient-6", null),   // 500 mg = 0.49 '1 
        RowFactory.create("patient-7", null),   // 30 d = 0.49 '1'
        RowFactory.create("patient-8", null),   // 60 s = 0.49 '1'
        RowFactory.create("patient-9", null),   // 1000 ms = 0.49 '1'
        RowFactory.create("patient-a", false),  // 1000 mmol = 0.49 '1'
        RowFactory.create("patient-b", true),   // 49 % = 0.49 '1'
        RowFactory.create("patient-c", null)    //  non-ucum = 0.49 '1'   
    );

    input = new OperatorInput(parserContext, ucumQuantityLiteral1, ucumQuantityLiteral1);
    result = equalityOperator.invoke(input);

    final Dataset<Row> allTrue = new DatasetBuilder(spark)
        .withIdColumn()
        .withColumn(DataTypes.BooleanType)
        .withIdsAndValue(true,
            List.of("patient-1", "patient-2", "patient-3", "patient-4", "patient-5", "patient-6",
                "patient-7", "patient-8", "patient-9", "patient-a", "patient-b", "patient-c"))
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
        RowFactory.create("patient-9", null),    // 1000 ms != 500 mg
        RowFactory.create("patient-a", null),    // 1000 mmol != 500 mg
        RowFactory.create("patient-b", null),    // 49 % != 500 mg
        RowFactory.create("patient-c", null)     //  non-ucum != 500 mg
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
        RowFactory.create("patient-9", null),    // 1000 ms != 0.5 mg
        RowFactory.create("patient-a", null),    // 1000 mmol != 0.5 mg
        RowFactory.create("patient-b", null),    // 49 % != 0.5 mg
        RowFactory.create("patient-c", null)     //  non-ucum != 0.5 mg
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
        RowFactory.create("patient-9", null),    // 1000 ms != 1.8 m
        RowFactory.create("patient-a", null),    // 1000 mmol != 1.8 m
        RowFactory.create("patient-b", null),    // 49 % != 1.8 m
        RowFactory.create("patient-c", null)     //  non-ucum != 1.8 m
    );

    input = new OperatorInput(parserContext, left, ucumQuantityLiteralNoUnit);
    result = equalityOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", null),   // 500 mg != 0.49 ''
        RowFactory.create("patient-2", null),   // 500 mg != 0.49 ''
        RowFactory.create("patient-3", null),   // 500 mg != 0.49 ''
        RowFactory.create("patient-4", null),   // 650 mg != 0.49 ''
        RowFactory.create("patient-5", null),   // {} != 0.49 ''
        RowFactory.create("patient-6", null),   // 500 mg != 0.49 '' 
        RowFactory.create("patient-7", null),   // 30 d != 0.49 ''
        RowFactory.create("patient-8", null),   // 60 s != 0.49 ''
        RowFactory.create("patient-9", null),    // 1000 ms != 0.49 ''
        RowFactory.create("patient-a", true),    // 1000 mmol != 0.49 ''
        RowFactory.create("patient-b", false),    // 49 % != 0.49 ''
        RowFactory.create("patient-c", null)     //  non-ucum != 0.49 ''
    );

    input = new OperatorInput(parserContext, ucumQuantityLiteral1, ucumQuantityLiteral1);
    result = equalityOperator.invoke(input);

    final Dataset<Row> allFalse = new DatasetBuilder(spark)
        .withIdColumn()
        .withColumn(DataTypes.BooleanType)
        .withIdsAndValue(false,
            List.of("patient-1", "patient-2", "patient-3", "patient-4", "patient-5", "patient-6",
                "patient-7", "patient-8", "patient-9", "patient-a", "patient-b", "patient-c"))
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
        RowFactory.create("patient-7", null),   // 30 d = 30 days
        RowFactory.create("patient-8", null),   // 60 s = 30 days
        RowFactory.create("patient-9", null),    // 1000 ms = 30 days
        RowFactory.create("patient-a", null),    // 1000 mmol = 30 days
        RowFactory.create("patient-b", null),    // 49 % = 30 days
        RowFactory.create("patient-c", null)     //  non-ucum = 30 days
    );

    input = new OperatorInput(parserContext, left, calendarDurationLiteral2);
    result = equalityOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", null),   // 500 mg = 60 seconds
        RowFactory.create("patient-2", null),   // 500 mg = 60 seconds
        RowFactory.create("patient-3", null),   // 500 mg = 60 seconds
        RowFactory.create("patient-4", null),   // 650 mg = 60 seconds
        RowFactory.create("patient-5", null),   // {} = 60 seconds
        RowFactory.create("patient-6", null),   // 500 mg = 60 seconds 
        RowFactory.create("patient-7", false),  // 30 d = 60 seconds
        RowFactory.create("patient-8", true),   // 60 s = 60 seconds
        RowFactory.create("patient-9", false),   // 1000 ms = 60 seconds
        RowFactory.create("patient-a", null),    // 1000 mmol = 60 seconds
        RowFactory.create("patient-b", null),    // 49 % = 60 seconds
        RowFactory.create("patient-c", null)     //  non-ucum = 60 seconds
    );

    input = new OperatorInput(parserContext, left, calendarDurationLiteral3);
    result = equalityOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", null),   // 500 mg = 1000 milliseconds
        RowFactory.create("patient-2", null),   // 500 mg = 1000 milliseconds
        RowFactory.create("patient-3", null),   // 500 mg = 1000 milliseconds
        RowFactory.create("patient-4", null),   // 650 mg = 1000 milliseconds
        RowFactory.create("patient-5", null),   // {} = 1000 milliseconds
        RowFactory.create("patient-6", null),   // 500 mg = 1000 milliseconds 
        RowFactory.create("patient-7", false),  // 30 d = 1000 milliseconds
        RowFactory.create("patient-8", false),  // 60 s = 1000 milliseconds
        RowFactory.create("patient-9", true),    // 1000 ms = 1000 milliseconds
        RowFactory.create("patient-a", null),    // 1000 mmol = 1000 milliseconds
        RowFactory.create("patient-b", null),    // 49 % = 1000 milliseconds
        RowFactory.create("patient-c", null)     //  non-ucum = 1000 milliseconds
    );

    input = new OperatorInput(parserContext, calendarDurationLiteral2, calendarDurationLiteral2);
    result = equalityOperator.invoke(input);

    final Dataset<Row> allTrue = new DatasetBuilder(spark)
        .withIdColumn()
        .withColumn(DataTypes.BooleanType)
        .withIdsAndValue(true,
            List.of("patient-1", "patient-2", "patient-3", "patient-4", "patient-5", "patient-6",
                "patient-7", "patient-8", "patient-9", "patient-a", "patient-b", "patient-c"))
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
        RowFactory.create("patient-7", null),   // 30 d != 30 days
        RowFactory.create("patient-8", null),   // 60 s != 30 days
        RowFactory.create("patient-9", null),   // 1000 ms != 30 days
        RowFactory.create("patient-a", null),   // 1000 mmol != 30 days
        RowFactory.create("patient-b", null),   // 49 % != 30 days
        RowFactory.create("patient-c", null)    //  non-ucum != 30 days
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
        RowFactory.create("patient-7", true),   // 30 d != 60 s
        RowFactory.create("patient-8", false),  // 60 s != 60 s
        RowFactory.create("patient-9", true),   // 1000 ms != 60 s
        RowFactory.create("patient-a", null),   // 1000 mmol != 60 seconds
        RowFactory.create("patient-b", null),   // 49 % != 60 seconds
        RowFactory.create("patient-c", null)    //  non-ucum != 60 seconds
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
        RowFactory.create("patient-7", true),   // 30 d != 1000 ms
        RowFactory.create("patient-8", true),   // 60 s != 1000 ms
        RowFactory.create("patient-9", false),   // 1000 ms != 1000 ms
        RowFactory.create("patient-a", null),    // 1000 mmol != 1000 milliseconds
        RowFactory.create("patient-b", null),    // 49 % != 1000 milliseconds
        RowFactory.create("patient-c", null)     //  non-ucum != 1000 milliseconds
    );

    input = new OperatorInput(parserContext, calendarDurationLiteral2, calendarDurationLiteral2);
    result = equalityOperator.invoke(input);

    final Dataset<Row> allFalse = new DatasetBuilder(spark)
        .withIdColumn()
        .withColumn(DataTypes.BooleanType)
        .withIdsAndValue(false,
            List.of("patient-1", "patient-2", "patient-3", "patient-4", "patient-5", "patient-6",
                "patient-7", "patient-8", "patient-9", "patient-a", "patient-b", "patient-c"))
        .build();
    assertThat(result).selectOrderedResult().hasRows(allFalse);
  }

}
