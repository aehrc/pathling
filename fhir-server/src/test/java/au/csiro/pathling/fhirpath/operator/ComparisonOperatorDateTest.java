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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.ElementDefinition;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.test.SpringBootUnitTest;
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
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author John Grimes
 */
@SpringBootUnitTest
class ComparisonOperatorDateTest {

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
        .getChildOfResource(fhirContext, "Patient", "birthDate");
    assertTrue(optionalDefinition.isPresent());
    final ElementDefinition definition = optionalDefinition.get();
    assertTrue(definition.getFhirType().isPresent());
    assertEquals(FHIRDefinedType.DATE, definition.getFhirType().get());

    final Dataset<Row> leftDataset = new DatasetBuilder(spark)
        .withIdColumn(ID_ALIAS)
        .withColumn(DataTypes.StringType)
        .withRow("patient-01", "2013-06-10")   // Equal, years, months and days
        .withRow("patient-02", "2013-06")      // Equal, years and months
        .withRow("patient-03", "2013")         // Equal, years
        .withRow("patient-04", "2013-06-01")   // Different precisions
        .withRow("patient-05", "2013-06-10")   // Less than, years, months and days
        .withRow("patient-06", "2013-06")      // Less than, years and months
        .withRow("patient-07", "2013")         // Less than, years
        .withRow("patient-08", "2013-06-10")   // Greater than, years, months and days
        .withRow("patient-09", "2013-06")      // Greater than, years and months
        .withRow("patient-10", "2013")         // Greater than, years
        .build();
    left = new ElementPathBuilder(spark)
        .dataset(leftDataset)
        .idAndValueColumns()
        .expression("birthDate")
        .singular(true)
        .definition(definition)
        .buildDefined();

    final Dataset<Row> rightDataset = new DatasetBuilder(spark)
        .withIdColumn(ID_ALIAS)
        .withColumn(DataTypes.StringType)
        .withRow("patient-01", "2013-06-10")   // Equal, years, months and days
        .withRow("patient-02", "2013-06")      // Equal, years and months
        .withRow("patient-03", "2013")         // Equal, years
        .withRow("patient-04", "2013-06")      // Different precisions
        .withRow("patient-05", "2013-06-11")   // Less than, years, months and days
        .withRow("patient-06", "2013-07")      // Less than, years and months
        .withRow("patient-07", "2014")         // Less than, years
        .withRow("patient-08", "2013-05-10")   // Greater than, years, months and days
        .withRow("patient-09", "2012-06")      // Greater than, years and months
        .withRow("patient-10", "2012")         // Greater than, years
        .build();
    right = new ElementPathBuilder(spark)
        .dataset(rightDataset)
        .idAndValueColumns()
        .expression("birthDate")
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
        RowFactory.create("patient-01", true),   // Equal, years, months and days
        RowFactory.create("patient-02", true),   // Equal, years and months
        RowFactory.create("patient-03", true),   // Equal, years
        RowFactory.create("patient-04", true),   // Different precisions
        RowFactory.create("patient-05", false),  // Less than, years, months and days
        RowFactory.create("patient-06", false),  // Less than, years and months
        RowFactory.create("patient-07", false),  // Less than, years
        RowFactory.create("patient-08", false),  // Greater than, years, months and days
        RowFactory.create("patient-09", false),  // Greater than, years and months
        RowFactory.create("patient-10", false)   // Greater than, years
    );
  }

  @Test
  void notEquals() {
    final OperatorInput comparisonInput = new OperatorInput(parserContext, left, right);
    final Operator lessThan = Operator.getInstance("!=");
    final FhirPath result = lessThan.invoke(comparisonInput);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-01", false),  // Equal, years, months and days
        RowFactory.create("patient-02", false),  // Equal, years and months
        RowFactory.create("patient-03", false),  // Equal, years
        RowFactory.create("patient-04", false),  // Different precisions
        RowFactory.create("patient-05", true),   // Less than, years, months and days
        RowFactory.create("patient-06", true),   // Less than, years and months
        RowFactory.create("patient-07", true),   // Less than, years
        RowFactory.create("patient-08", true),   // Greater than, years, months and days
        RowFactory.create("patient-09", true),   // Greater than, years and months
        RowFactory.create("patient-10", true)    // Greater than, years
    );
  }

  @Test
  void lessThan() {
    final OperatorInput comparisonInput = new OperatorInput(parserContext, left, right);
    final Operator lessThan = Operator.getInstance("<");
    final FhirPath result = lessThan.invoke(comparisonInput);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-01", false),  // Equal, years, months and days
        RowFactory.create("patient-02", false),  // Equal, years and months
        RowFactory.create("patient-03", false),  // Equal, years
        RowFactory.create("patient-04", false),  // Different precisions
        RowFactory.create("patient-05", true),   // Less than, years, months and days
        RowFactory.create("patient-06", true),   // Less than, years and months
        RowFactory.create("patient-07", true),   // Less than, years
        RowFactory.create("patient-08", false),  // Greater than, years, months and days
        RowFactory.create("patient-09", false),  // Greater than, years and months
        RowFactory.create("patient-10", false)   // Greater than, years
    );
  }

  @Test
  void lessThanOrEqualTo() {
    final OperatorInput comparisonInput = new OperatorInput(parserContext, left, right);
    final Operator lessThan = Operator.getInstance("<=");
    final FhirPath result = lessThan.invoke(comparisonInput);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-01", true),   // Equal, years, months and days
        RowFactory.create("patient-02", true),   // Equal, years and months
        RowFactory.create("patient-03", true),   // Equal, years
        RowFactory.create("patient-04", true),   // Different precisions
        RowFactory.create("patient-05", true),   // Less than, years, months and days
        RowFactory.create("patient-06", true),   // Less than, years and months
        RowFactory.create("patient-07", true),   // Less than, years
        RowFactory.create("patient-08", false),  // Greater than, years, months and days
        RowFactory.create("patient-09", false),  // Greater than, years and months
        RowFactory.create("patient-10", false)   // Greater than, years
    );
  }

  @Test
  void greaterThan() {
    final OperatorInput comparisonInput = new OperatorInput(parserContext, left, right);
    final Operator lessThan = Operator.getInstance(">");
    final FhirPath result = lessThan.invoke(comparisonInput);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-01", false),  // Equal, years, months and days
        RowFactory.create("patient-02", false),  // Equal, years and months
        RowFactory.create("patient-03", false),  // Equal, years
        RowFactory.create("patient-04", false),  // Different precisions
        RowFactory.create("patient-05", false),  // Less than, years, months and days
        RowFactory.create("patient-06", false),  // Less than, years and months
        RowFactory.create("patient-07", false),  // Less than, years
        RowFactory.create("patient-08", true),   // Greater than, years, months and days
        RowFactory.create("patient-09", true),   // Greater than, years and months
        RowFactory.create("patient-10", true)    // Greater than, years
    );
  }

  @Test
  void greaterThanOrEqualTo() {
    final OperatorInput comparisonInput = new OperatorInput(parserContext, left, right);
    final Operator lessThan = Operator.getInstance(">=");
    final FhirPath result = lessThan.invoke(comparisonInput);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-01", true),   // Equal, years, months and days
        RowFactory.create("patient-02", true),   // Equal, years and months
        RowFactory.create("patient-03", true),   // Equal, years
        RowFactory.create("patient-04", true),   // Different precisions
        RowFactory.create("patient-05", false),  // Less than, years, months and days
        RowFactory.create("patient-06", false),  // Less than, years and months
        RowFactory.create("patient-07", false),  // Less than, years
        RowFactory.create("patient-08", true),   // Greater than, years, months and days
        RowFactory.create("patient-09", true),   // Greater than, years and months
        RowFactory.create("patient-10", true)    // Greater than, years
    );
  }

}
