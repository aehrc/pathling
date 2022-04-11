/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.operator;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;
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
class ComparisonOperatorDateTimeTest {

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
    final Optional<ElementDefinition> optionalLeftDefinition = FhirHelpers
        .getChildOfResource(fhirContext, "MedicationRequest", "authoredOn");
    assertTrue(optionalLeftDefinition.isPresent());
    final ElementDefinition leftDefinition = optionalLeftDefinition.get();

    final Dataset<Row> leftDataset = new DatasetBuilder(spark)
        .withIdColumn(ID_ALIAS)
        .withColumn(DataTypes.StringType)
        .withRow("patient-1", "2013-06-10T15:33:22Z")       // Equal, exact
        .withRow("patient-2", "2013-06-10T15:33:22Z")       // Equal, different time zones
        .withRow("patient-3", "2013-06-10T15:33:22+00:00")  // Equal, different time zone syntax
        .withRow("patient-4", "2013-06-10T15:33:22.000Z")   // Equal, different precisions
        .withRow("patient-5", "2013-06-10T15:33:21.900Z")   // Less than
        .withRow("patient-6", "2013-06-11T15:33:22Z")       // Greater than
        .build();
    left = new ElementPathBuilder(spark)
        .dataset(leftDataset)
        .idAndValueColumns()
        .expression("authoredOn")
        .singular(true)
        .definition(leftDefinition)
        .buildDefined();

    final Optional<ElementDefinition> optionalRightDefinition = FhirHelpers
        .getChildOfResource(fhirContext, "Condition", "onsetDateTime");
    assertTrue(optionalRightDefinition.isPresent());
    final ElementDefinition rightDefinition = optionalRightDefinition.get();

    final Dataset<Row> rightDataset = new DatasetBuilder(spark)
        .withIdColumn(ID_ALIAS)
        .withColumn(DataTypes.StringType)
        .withRow("patient-1", "2013-06-10T15:33:22Z")       // Equal, exact
        .withRow("patient-2", "2013-06-11T01:33:22+10:00")  // Equal, different time zones
        .withRow("patient-3", "2013-06-10T15:33:22Z")       // Equal, different time zone syntax
        .withRow("patient-4", "2013-06-10T15:33:22Z")       // Equal, different precisions
        .withRow("patient-5", "2013-06-10T15:33:22Z")       // Less than
        .withRow("patient-6", "2013-06-10T15:33:22Z")       // Greater than
        .build();
    right = new ElementPathBuilder(spark)
        .dataset(rightDataset)
        .idAndValueColumns()
        .expression("reverseResolve(Condition.subject).onsetDateTime")
        .singular(true)
        .definition(rightDefinition)
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
        RowFactory.create("patient-1", true),  // Equal, exact
        RowFactory.create("patient-2", true),  // Equal, different time zones
        RowFactory.create("patient-3", true),  // Equal, different time zone syntax
        RowFactory.create("patient-4", true),  // Equal, different precisions
        RowFactory.create("patient-5", false), // Less than
        RowFactory.create("patient-6", false)  // Greater than
    );
  }

  @Test
  void notEquals() {
    final OperatorInput comparisonInput = new OperatorInput(parserContext, left, right);
    final Operator lessThan = Operator.getInstance("!=");
    final FhirPath result = lessThan.invoke(comparisonInput);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", false),  // Equal, exact
        RowFactory.create("patient-2", false),  // Equal, different time zones
        RowFactory.create("patient-3", false),  // Equal, different time zone syntax
        RowFactory.create("patient-4", false),  // Equal, different precisions
        RowFactory.create("patient-5", true),   // Less than
        RowFactory.create("patient-6", true)    // Greater than
    );
  }

  @Test
  void lessThan() {
    final OperatorInput comparisonInput = new OperatorInput(parserContext, left, right);
    final Operator lessThan = Operator.getInstance("<");
    final FhirPath result = lessThan.invoke(comparisonInput);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", false),  // Equal, exact
        RowFactory.create("patient-2", false),  // Equal, different time zones
        RowFactory.create("patient-3", false),  // Equal, different time zone syntax
        RowFactory.create("patient-4", false),  // Equal, different precisions
        RowFactory.create("patient-5", true),   // Less than
        RowFactory.create("patient-6", false)   // Greater than
    );
  }

  @Test
  void lessThanOrEqualTo() {
    final OperatorInput comparisonInput = new OperatorInput(parserContext, left, right);
    final Operator lessThan = Operator.getInstance("<=");
    final FhirPath result = lessThan.invoke(comparisonInput);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", true),  // Equal, exact
        RowFactory.create("patient-2", true),  // Equal, different time zones
        RowFactory.create("patient-3", true),  // Equal, different time zone syntax
        RowFactory.create("patient-4", true),  // Equal, different precisions
        RowFactory.create("patient-5", true),   // Less than
        RowFactory.create("patient-6", false)   // Greater than
    );
  }

  @Test
  void greaterThan() {
    final OperatorInput comparisonInput = new OperatorInput(parserContext, left, right);
    final Operator lessThan = Operator.getInstance(">");
    final FhirPath result = lessThan.invoke(comparisonInput);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", false),  // Equal, exact
        RowFactory.create("patient-2", false),  // Equal, different time zones
        RowFactory.create("patient-3", false),  // Equal, different time zone syntax
        RowFactory.create("patient-4", false),  // Equal, different precisions
        RowFactory.create("patient-5", false),  // Less than
        RowFactory.create("patient-6", true)    // Greater than
    );
  }

  @Test
  void greaterThanOrEqualTo() {
    final OperatorInput comparisonInput = new OperatorInput(parserContext, left, right);
    final Operator lessThan = Operator.getInstance(">=");
    final FhirPath result = lessThan.invoke(comparisonInput);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", true),  // Equal, exact
        RowFactory.create("patient-2", true),  // Equal, different time zones
        RowFactory.create("patient-3", true),  // Equal, different time zone syntax
        RowFactory.create("patient-4", true),  // Equal, different precisions
        RowFactory.create("patient-5", false),  // Less than
        RowFactory.create("patient-6", true)    // Greater than
    );
  }

}