/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
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
        .withRow("abc1", true)
        .withRow("abc2", true)
        .withRow("abc3", false)
        .withRow("abc4", false)
        .withRow("abc5", null)
        .withRow("abc6", null)
        .withRow("abc7", true)
        .withRow("abc8", null)
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
        .withRow("abc1", false)
        .withRow("abc2", null)
        .withRow("abc3", true)
        .withRow("abc4", null)
        .withRow("abc5", true)
        .withRow("abc6", false)
        .withRow("abc7", true)
        .withRow("abc8", null)
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

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", false),
        RowFactory.create("abc2", null),
        RowFactory.create("abc3", false),
        RowFactory.create("abc4", false),
        RowFactory.create("abc5", null),
        RowFactory.create("abc6", false),
        RowFactory.create("abc7", true),
        RowFactory.create("abc8", null)
    );
  }

  @Test
  public void or() {
    final OperatorInput input = new OperatorInput(parserContext, left, right);

    final Operator booleanOperator = Operator.getInstance("or");
    final FhirPath result = booleanOperator.invoke(input);

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", true),
        RowFactory.create("abc2", true),
        RowFactory.create("abc3", true),
        RowFactory.create("abc4", null),
        RowFactory.create("abc5", true),
        RowFactory.create("abc6", null),
        RowFactory.create("abc7", true),
        RowFactory.create("abc8", null)
    );
  }

  @Test
  public void xor() {
    final OperatorInput input = new OperatorInput(parserContext, left, right);

    final Operator booleanOperator = Operator.getInstance("xor");
    final FhirPath result = booleanOperator.invoke(input);

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", true),
        RowFactory.create("abc2", null),
        RowFactory.create("abc3", true),
        RowFactory.create("abc4", null),
        RowFactory.create("abc5", null),
        RowFactory.create("abc6", null),
        RowFactory.create("abc7", false),
        RowFactory.create("abc8", null)
    );
  }

  @Test
  public void implies() {
    final OperatorInput input = new OperatorInput(parserContext, left, right);

    final Operator booleanOperator = Operator.getInstance("implies");
    final FhirPath result = booleanOperator.invoke(input);

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", false),
        RowFactory.create("abc2", null),
        RowFactory.create("abc3", true),
        RowFactory.create("abc4", true),
        RowFactory.create("abc5", true),
        RowFactory.create("abc6", null),
        RowFactory.create("abc7", true),
        RowFactory.create("abc8", null)
    );
  }

  @Test
  public void leftIsLiteral() {
    final FhirPath literalLeft = BooleanLiteralPath.fromString("true", left);
    final OperatorInput input = new OperatorInput(parserContext, literalLeft, right);

    final Operator booleanOperator = Operator.getInstance("and");
    final FhirPath result = booleanOperator.invoke(input);

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", false),
        RowFactory.create("abc2", null),
        RowFactory.create("abc3", true),
        RowFactory.create("abc4", null),
        RowFactory.create("abc5", true),
        RowFactory.create("abc6", false),
        RowFactory.create("abc7", true),
        RowFactory.create("abc8", null)
    );
  }

  @Test
  public void rightIsLiteral() {
    final FhirPath literalRight = BooleanLiteralPath.fromString("true", right);
    final OperatorInput input = new OperatorInput(parserContext, left, literalRight);

    final Operator booleanOperator = Operator.getInstance("and");
    final FhirPath result = booleanOperator.invoke(input);

    assertThat(result).selectResult().hasRows(
        RowFactory.create("abc1", true),
        RowFactory.create("abc2", true),
        RowFactory.create("abc3", false),
        RowFactory.create("abc4", false),
        RowFactory.create("abc5", null),
        RowFactory.create("abc6", null),
        RowFactory.create("abc7", true),
        RowFactory.create("abc8", null)
    );
  }
}