/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.operator;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.literal.BooleanLiteralPath;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Tag;

/**
 * @author John Grimes
 */
@Tag("UnitTest")
public class BooleanOperatorTest {

  private FhirPath left;
  private FhirPath right;
  private ParserContext parserContext;

  @Before
  public void setUp() {
    final Dataset<Row> leftDataset = new DatasetBuilder()
        .withIdColumn()
        .withColumn(DataTypes.BooleanType)
        .withRow("patient-1", true)
        .withRow("patient-2", true)
        .withRow("patient-3", false)
        .withRow("patient-4", false)
        .withRow("patient-5", null)
        .withRow("patient-6", null)
        .withRow("patient-7", true)
        .withRow("patient-8", null)
        .build();
    left = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.BOOLEAN)
        .dataset(leftDataset)
        .idAndValueColumns()
        .singular(true)
        .expression("estimatedAge")
        .build();

    final Dataset<Row> rightDataset = new DatasetBuilder()
        .withIdColumn()
        .withColumn(DataTypes.BooleanType)
        .withRow("patient-1", false)
        .withRow("patient-2", null)
        .withRow("patient-3", true)
        .withRow("patient-4", null)
        .withRow("patient-5", true)
        .withRow("patient-6", false)
        .withRow("patient-7", true)
        .withRow("patient-8", null)
        .build();
    right = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.BOOLEAN)
        .dataset(rightDataset)
        .idAndValueColumns()
        .singular(true)
        .expression("deceasedBoolean")
        .build();

    parserContext = new ParserContextBuilder().build();
  }

  @Test
  public void and() {
    final OperatorInput input = new OperatorInput(parserContext, left, right);

    final Operator booleanOperator = Operator.getInstance("and");
    final FhirPath result = booleanOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", false),
        RowFactory.create("patient-2", null),
        RowFactory.create("patient-3", false),
        RowFactory.create("patient-4", false),
        RowFactory.create("patient-5", null),
        RowFactory.create("patient-6", false),
        RowFactory.create("patient-7", true),
        RowFactory.create("patient-8", null)
    );
  }

  @Test
  public void or() {
    final OperatorInput input = new OperatorInput(parserContext, left, right);

    final Operator booleanOperator = Operator.getInstance("or");
    final FhirPath result = booleanOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", true),
        RowFactory.create("patient-2", true),
        RowFactory.create("patient-3", true),
        RowFactory.create("patient-4", null),
        RowFactory.create("patient-5", true),
        RowFactory.create("patient-6", null),
        RowFactory.create("patient-7", true),
        RowFactory.create("patient-8", null)
    );
  }

  @Test
  public void xor() {
    final OperatorInput input = new OperatorInput(parserContext, left, right);

    final Operator booleanOperator = Operator.getInstance("xor");
    final FhirPath result = booleanOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", true),
        RowFactory.create("patient-2", null),
        RowFactory.create("patient-3", true),
        RowFactory.create("patient-4", null),
        RowFactory.create("patient-5", null),
        RowFactory.create("patient-6", null),
        RowFactory.create("patient-7", false),
        RowFactory.create("patient-8", null)
    );
  }

  @Test
  public void implies() {
    final OperatorInput input = new OperatorInput(parserContext, left, right);

    final Operator booleanOperator = Operator.getInstance("implies");
    final FhirPath result = booleanOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", false),
        RowFactory.create("patient-2", null),
        RowFactory.create("patient-3", true),
        RowFactory.create("patient-4", true),
        RowFactory.create("patient-5", true),
        RowFactory.create("patient-6", null),
        RowFactory.create("patient-7", true),
        RowFactory.create("patient-8", null)
    );
  }

  @Test
  public void leftIsLiteral() {
    final FhirPath literalLeft = BooleanLiteralPath.fromString("true", left);
    final OperatorInput input = new OperatorInput(parserContext, literalLeft, right);

    final Operator booleanOperator = Operator.getInstance("and");
    final FhirPath result = booleanOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", false),
        RowFactory.create("patient-2", null),
        RowFactory.create("patient-3", true),
        RowFactory.create("patient-4", null),
        RowFactory.create("patient-5", true),
        RowFactory.create("patient-6", false),
        RowFactory.create("patient-7", true),
        RowFactory.create("patient-8", null)
    );
  }

  @Test
  public void rightIsLiteral() {
    final FhirPath literalRight = BooleanLiteralPath.fromString("true", right);
    final OperatorInput input = new OperatorInput(parserContext, left, literalRight);

    final Operator booleanOperator = Operator.getInstance("and");
    final FhirPath result = booleanOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", true),
        RowFactory.create("patient-2", true),
        RowFactory.create("patient-3", false),
        RowFactory.create("patient-4", false),
        RowFactory.create("patient-5", null),
        RowFactory.create("patient-6", null),
        RowFactory.create("patient-7", true),
        RowFactory.create("patient-8", null)
    );
  }
}