/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.operator;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import au.csiro.pathling.test.helpers.FhirHelpers;
import ca.uhn.fhir.context.FhirContext;
import java.util.Collections;
import java.util.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * @author John Grimes
 */
@SpringBootTest
@Tag("UnitTest")
class ComparisonOperatorTimeTest {

  @Autowired
  SparkSession spark;

  @Autowired
  FhirContext fhirContext;

  static final String ID_ALIAS = "_abc123";
  private ElementPath left;
  private ElementPath right;
  private ParserContext parserContext;

  @BeforeEach
  void setUp() {
    final Optional<ElementDefinition> optionalDefinition = FhirHelpers
        .getChildOfResource(fhirContext, "Observation", "valueTime");
    assertTrue(optionalDefinition.isPresent());
    final ElementDefinition definition = optionalDefinition.get();
    assertTrue(definition.getFhirType().isPresent());
    assertEquals(FHIRDefinedType.TIME, definition.getFhirType().get());

    final Dataset<Row> leftDataset = new DatasetBuilder(spark)
        .withIdColumn(ID_ALIAS)
        .withColumn(DataTypes.StringType)
        .withRow("patient-01", "03:15:36")   // Equal, hours, minutes and seconds
        .withRow("patient-02", "03:15")      // Equal, hours and minutes
        .withRow("patient-03", "03")         // Equal, hours
        .withRow("patient-04", "03:15:36")   // Different precisions
        .withRow("patient-05", "03:15:36")   // Less than, hours, minutes and seconds
        .withRow("patient-06", "03:15")      // Less than, hours and minutes
        .withRow("patient-07", "03")         // Less than, hours
        .withRow("patient-08", "03:15:36")   // Greater than, hours, minutes and seconds
        .withRow("patient-09", "03:15")      // Greater than, hours and minutes
        .withRow("patient-10", "03")         // Greater than, hours
        .build();
    left = new ElementPathBuilder(spark)
        .dataset(leftDataset)
        .idAndValueColumns()
        .expression("valueTime")
        .singular(true)
        .definition(definition)
        .buildDefined();

    final Dataset<Row> rightDataset = new DatasetBuilder(spark)
        .withIdColumn(ID_ALIAS)
        .withColumn(DataTypes.StringType)
        .withRow("patient-01", "03:15:36")   // Equal, hours, minutes and seconds
        .withRow("patient-02", "03:15")      // Equal, hours and minutes
        .withRow("patient-03", "03")         // Equal, hours
        .withRow("patient-04", "03:15")      // Different precisions
        .withRow("patient-05", "03:16:36")   // Less than, hours, minutes and seconds
        .withRow("patient-06", "03:16")      // Less than, hours and minutes
        .withRow("patient-07", "04")         // Less than, hours
        .withRow("patient-08", "02:15:36")   // Greater than, hours, minutes and seconds
        .withRow("patient-09", "03:14")      // Greater than, hours and minutes
        .withRow("patient-10", "01")         // Greater than, hours
        .build();
    right = new ElementPathBuilder(spark)
        .dataset(rightDataset)
        .idAndValueColumns()
        .expression("valueTime")
        .singular(true)
        .definition(definition)
        .buildDefined();

    parserContext = new ParserContextBuilder(spark, fhirContext)
        .groupingColumns(Collections.singletonList(left.getIdColumn()))
        .build();
  }

  @Test
  void equals() {
    final OperatorInput comparisonInput = new OperatorInput(parserContext, left, right);
    final Operator lessThan = Operator.getInstance("=");
    final FhirPath result = lessThan.invoke(comparisonInput);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-01", true),  // Equal, hours, minutes and seconds
        RowFactory.create("patient-02", true),  // Equal, hours and minutes
        RowFactory.create("patient-03", true),  // Equal, hours
        RowFactory.create("patient-04", null),  // Different precisions
        RowFactory.create("patient-05", false), // Less than, hours, minutes and seconds
        RowFactory.create("patient-06", false), // Less than, hours and minutes
        RowFactory.create("patient-07", false), // Less than, hours
        RowFactory.create("patient-08", false), // Greater than, hours, minutes and seconds
        RowFactory.create("patient-09", false), // Greater than, hours and minutes
        RowFactory.create("patient-10", false)  // Greater than, hours
    );
  }

  @Test
  void notEquals() {
    final OperatorInput comparisonInput = new OperatorInput(parserContext, left, right);
    final Operator lessThan = Operator.getInstance("!=");
    final FhirPath result = lessThan.invoke(comparisonInput);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-01", false),  // Equal, hours, minutes and seconds
        RowFactory.create("patient-02", false),  // Equal, hours and minutes
        RowFactory.create("patient-03", false),  // Equal, hours
        RowFactory.create("patient-04", null),   // Different precisions
        RowFactory.create("patient-05", true),   // Less than, hours, minutes and seconds
        RowFactory.create("patient-06", true),   // Less than, hours and minutes
        RowFactory.create("patient-07", true),   // Less than, hours
        RowFactory.create("patient-08", true),   // Greater than, hours, minutes and seconds
        RowFactory.create("patient-09", true),   // Greater than, hours and minutes
        RowFactory.create("patient-10", true)    // Greater than, hours
    );
  }

  @Test
  void lessThan() {
    final OperatorInput comparisonInput = new OperatorInput(parserContext, left, right);
    final Operator lessThan = Operator.getInstance("<");
    final FhirPath result = lessThan.invoke(comparisonInput);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-01", false),  // Equal, hours, minutes and seconds
        RowFactory.create("patient-02", false),  // Equal, hours and minutes
        RowFactory.create("patient-03", false),  // Equal, hours
        RowFactory.create("patient-04", null),   // Different precisions
        RowFactory.create("patient-05", true),   // Less than, hours, minutes and seconds
        RowFactory.create("patient-06", true),   // Less than, hours and minutes
        RowFactory.create("patient-07", true),   // Less than, hours
        RowFactory.create("patient-08", false),  // Greater than, hours, minutes and seconds
        RowFactory.create("patient-09", false),  // Greater than, hours and minutes
        RowFactory.create("patient-10", false)   // Greater than, hours
    );
  }

  @Test
  void lessThanOrEqualTo() {
    final OperatorInput comparisonInput = new OperatorInput(parserContext, left, right);
    final Operator lessThan = Operator.getInstance("<=");
    final FhirPath result = lessThan.invoke(comparisonInput);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-01", true),   // Equal, hours, minutes and seconds
        RowFactory.create("patient-02", true),   // Equal, hours and minutes
        RowFactory.create("patient-03", true),   // Equal, hours
        RowFactory.create("patient-04", null),   // Different precisions
        RowFactory.create("patient-05", true),   // Less than, hours, minutes and seconds
        RowFactory.create("patient-06", true),   // Less than, hours and minutes
        RowFactory.create("patient-07", true),   // Less than, hours
        RowFactory.create("patient-08", false),  // Greater than, hours, minutes and seconds
        RowFactory.create("patient-09", false),  // Greater than, hours and minutes
        RowFactory.create("patient-10", false)   // Greater than, hours
    );
  }

  @Test
  void greaterThan() {
    final OperatorInput comparisonInput = new OperatorInput(parserContext, left, right);
    final Operator lessThan = Operator.getInstance(">");
    final FhirPath result = lessThan.invoke(comparisonInput);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-01", false),  // Equal, hours, minutes and seconds
        RowFactory.create("patient-02", false),  // Equal, hours and minutes
        RowFactory.create("patient-03", false),  // Equal, hours
        RowFactory.create("patient-04", null),   // Different precisions
        RowFactory.create("patient-05", false),  // Less than, hours, minutes and seconds
        RowFactory.create("patient-06", false),  // Less than, hours and minutes
        RowFactory.create("patient-07", false),  // Less than, hours
        RowFactory.create("patient-08", true),   // Greater than, hours, minutes and seconds
        RowFactory.create("patient-09", true),   // Greater than, hours and minutes
        RowFactory.create("patient-10", true)    // Greater than, hours
    );
  }

  @Test
  void greaterThanOrEqualTo() {
    final OperatorInput comparisonInput = new OperatorInput(parserContext, left, right);
    final Operator lessThan = Operator.getInstance(">=");
    final FhirPath result = lessThan.invoke(comparisonInput);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-01", true),   // Equal, hours, minutes and seconds
        RowFactory.create("patient-02", true),   // Equal, hours and minutes
        RowFactory.create("patient-03", true),   // Equal, hours
        RowFactory.create("patient-04", null),   // Different precisions
        RowFactory.create("patient-05", false),  // Less than, hours, minutes and seconds
        RowFactory.create("patient-06", false),  // Less than, hours and minutes
        RowFactory.create("patient-07", false),  // Less than, hours
        RowFactory.create("patient-08", true),   // Greater than, hours, minutes and seconds
        RowFactory.create("patient-09", true),   // Greater than, hours and minutes
        RowFactory.create("patient-10", true)    // Greater than, hours
    );
  }

}