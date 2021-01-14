/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.operator;

import static au.csiro.pathling.test.assertions.Assertions.assertThat;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.element.ElementPath;
import au.csiro.pathling.fhirpath.literal.*;
import au.csiro.pathling.fhirpath.parser.ParserContext;
import au.csiro.pathling.test.builders.DatasetBuilder;
import au.csiro.pathling.test.builders.ElementPathBuilder;
import au.csiro.pathling.test.builders.ParserContextBuilder;
import java.math.BigDecimal;
import java.text.ParseException;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Value;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * @author John Grimes
 */
@Tag("UnitTest")
public class ComparisonOperatorTest {

  private ParserContext parserContext;

  @BeforeEach
  void setUp() {
    parserContext = new ParserContextBuilder().build();
  }

  @Value
  private static class TestParameters {

    @Nonnull
    String name;

    @Nonnull
    FhirPath left;

    @Nonnull
    FhirPath right;

    @Nonnull
    FhirPath literal;

    @Override
    public String toString() {
      return name;
    }

  }

  public static Stream<TestParameters> parameters() {
    return Stream.of(
        "String",
        "Integer",
        "Decimal",
        "DateTime",
        "Date",
        "Date (YYYY-MM)",
        "Date (YYYY)"
    ).map(ComparisonOperatorTest::buildTestParameters);
  }

  private static TestParameters buildTestParameters(@Nonnull final String name) {
    switch (name) {
      case "String":
        return buildStringExpressions(name);
      case "Integer":
        return buildIntegerExpressions(name);
      case "Decimal":
        return buildDecimalExpressions(name);
      case "DateTime":
        return buildDateTimeExpressions(name,
            "2015-02-07T13:28:17-05:00",
            "2015-02-08T13:28:17-05:00",
            FHIRDefinedType.DATETIME);
      case "Date":
        return buildDateTimeExpressions(name,
            "2015-02-07",
            "2015-02-08",
            FHIRDefinedType.DATE);
      case "Date (YYYY-MM)":
        return buildDateTimeExpressions(name,
            "2015-02",
            "2015-03",
            FHIRDefinedType.DATE);
      case "Date (YYYY)":
        return buildDateTimeExpressions(name,
            "2015",
            "2016",
            FHIRDefinedType.DATE);
      default:
        throw new RuntimeException("Invalid data type");
    }
  }

  private static TestParameters buildStringExpressions(final String name) {
    final Dataset<Row> leftDataset = new DatasetBuilder()
        .withIdColumn()
        .withColumn(DataTypes.StringType)
        .withRow("patient-1", "Evelyn")
        .withRow("patient-2", "Evelyn")
        .withRow("patient-3", "Jude")
        .withRow("patient-4", null)
        .withRow("patient-5", "Evelyn")
        .withRow("patient-6", null)
        .build();
    final ElementPath left = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.STRING)
        .dataset(leftDataset)
        .idAndValueColumns()
        .singular(true)
        .build();
    final Dataset<Row> rightDataset = new DatasetBuilder()
        .withIdColumn()
        .withColumn(DataTypes.StringType)
        .withRow("patient-1", "Evelyn")
        .withRow("patient-2", "Jude")
        .withRow("patient-3", "Evelyn")
        .withRow("patient-4", "Evelyn")
        .withRow("patient-5", null)
        .withRow("patient-6", null)
        .build();
    final ElementPath right = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.STRING)
        .dataset(rightDataset)
        .idAndValueColumns()
        .singular(true)
        .build();
    final StringLiteralPath literal = StringLiteralPath.fromString("'Evelyn'", left);
    return new TestParameters(name, left, right, literal);
  }

  private static TestParameters buildIntegerExpressions(final String name) {
    final Dataset<Row> leftDataset = new DatasetBuilder()
        .withIdColumn()
        .withColumn(DataTypes.IntegerType)
        .withRow("patient-1", 1)
        .withRow("patient-2", 1)
        .withRow("patient-3", 2)
        .withRow("patient-4", null)
        .withRow("patient-5", 1)
        .withRow("patient-6", null)
        .build();
    final ElementPath left = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.INTEGER)
        .dataset(leftDataset)
        .idAndValueColumns()
        .singular(true)
        .build();
    final Dataset<Row> rightDataset = new DatasetBuilder()
        .withIdColumn()
        .withColumn(DataTypes.IntegerType)
        .withRow("patient-1", 1)
        .withRow("patient-2", 2)
        .withRow("patient-3", 1)
        .withRow("patient-4", 1)
        .withRow("patient-5", null)
        .withRow("patient-6", null)
        .build();
    final ElementPath right = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.INTEGER)
        .dataset(rightDataset)
        .idAndValueColumns()
        .singular(true)
        .build();
    final IntegerLiteralPath literal = IntegerLiteralPath.fromString("1", left);
    return new TestParameters(name, left, right, literal);
  }

  private static TestParameters buildDecimalExpressions(final String name) {
    final Dataset<Row> leftDataset = new DatasetBuilder()
        .withIdColumn()
        .withColumn(DataTypes.createDecimalType())
        .withRow("patient-1", new BigDecimal("1.0"))
        .withRow("patient-2", new BigDecimal("1.0"))
        .withRow("patient-3", new BigDecimal("2.0"))
        .withRow("patient-4", null)
        .withRow("patient-5", new BigDecimal("1.0"))
        .withRow("patient-6", null)
        .build();
    final ElementPath left = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.DECIMAL)
        .dataset(leftDataset)
        .idAndValueColumns()
        .singular(true)
        .build();
    final Dataset<Row> rightDataset = new DatasetBuilder()
        .withIdColumn()
        .withColumn(DataTypes.createDecimalType())
        .withRow("patient-1", new BigDecimal("1.0"))
        .withRow("patient-2", new BigDecimal("2.0"))
        .withRow("patient-3", new BigDecimal("1.0"))
        .withRow("patient-4", new BigDecimal("1.0"))
        .withRow("patient-5", null)
        .withRow("patient-6", null)
        .build();
    final ElementPath right = new ElementPathBuilder()
        .fhirType(FHIRDefinedType.DECIMAL)
        .dataset(rightDataset)
        .idAndValueColumns()
        .singular(true)
        .build();
    final DecimalLiteralPath literal = DecimalLiteralPath.fromString("1.0", left);
    return new TestParameters(name, left, right, literal);
  }

  private static TestParameters buildDateTimeExpressions(final String name,
      final String lesserDate,
      final String greaterDate,
      final FHIRDefinedType fhirType) {
    final Dataset<Row> leftDataset = new DatasetBuilder()
        .withIdColumn()
        .withColumn(DataTypes.StringType)
        .withRow("patient-1", lesserDate)
        .withRow("patient-2", lesserDate)
        .withRow("patient-3", greaterDate)
        .withRow("patient-4", null)
        .withRow("patient-5", lesserDate)
        .withRow("patient-6", null)
        .build();
    final ElementPath left = new ElementPathBuilder()
        .fhirType(fhirType)
        .dataset(leftDataset)
        .idAndValueColumns()
        .singular(true)
        .build();
    final Dataset<Row> rightDataset = new DatasetBuilder()
        .withIdColumn()
        .withColumn(DataTypes.StringType)
        .withRow("patient-1", lesserDate)
        .withRow("patient-2", greaterDate)
        .withRow("patient-3", lesserDate)
        .withRow("patient-4", lesserDate)
        .withRow("patient-5", null)
        .withRow("patient-6", null)
        .build();
    final ElementPath right = new ElementPathBuilder()
        .fhirType(fhirType)
        .dataset(rightDataset)
        .idAndValueColumns()
        .singular(true)
        .build();
    final LiteralPath literal;
    try {
      literal = (fhirType == FHIRDefinedType.DATETIME)
                ? DateTimeLiteralPath.fromString(lesserDate, left)
                : DateLiteralPath.fromString(lesserDate, left);
    } catch (final ParseException e) {
      throw new RuntimeException("Error parsing literal date or date time");
    }
    return new TestParameters(name, left, right, literal);
  }

  @ParameterizedTest
  @MethodSource("parameters")
  public void lessThanOrEqualTo(final TestParameters parameters) {
    final OperatorInput input = new OperatorInput(parserContext, parameters.getLeft(),
        parameters.getRight());
    final Operator comparisonOperator = Operator.getInstance("<=");
    final FhirPath result = comparisonOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", true),
        RowFactory.create("patient-2", true),
        RowFactory.create("patient-3", false),
        RowFactory.create("patient-4", null),
        RowFactory.create("patient-5", null),
        RowFactory.create("patient-6", null)
    );
  }

  @ParameterizedTest
  @MethodSource("parameters")
  public void lessThan(final TestParameters parameters) {
    final OperatorInput input = new OperatorInput(parserContext, parameters.getLeft(),
        parameters.getRight());
    final Operator comparisonOperator = Operator.getInstance("<");
    final FhirPath result = comparisonOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", false),
        RowFactory.create("patient-2", true),
        RowFactory.create("patient-3", false),
        RowFactory.create("patient-4", null),
        RowFactory.create("patient-5", null),
        RowFactory.create("patient-6", null)
    );
  }

  @ParameterizedTest
  @MethodSource("parameters")
  public void greaterThanOrEqualTo(final TestParameters parameters) {
    final OperatorInput input = new OperatorInput(parserContext, parameters.getLeft(),
        parameters.getRight());
    final Operator comparisonOperator = Operator.getInstance(">=");
    final FhirPath result = comparisonOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", true),
        RowFactory.create("patient-2", false),
        RowFactory.create("patient-3", true),
        RowFactory.create("patient-4", null),
        RowFactory.create("patient-5", null),
        RowFactory.create("patient-6", null)
    );
  }

  @ParameterizedTest
  @MethodSource("parameters")
  public void greaterThan(final TestParameters parameters) {
    final OperatorInput input = new OperatorInput(parserContext, parameters.getLeft(),
        parameters.getRight());
    final Operator comparisonOperator = Operator.getInstance(">");
    final FhirPath result = comparisonOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", false),
        RowFactory.create("patient-2", false),
        RowFactory.create("patient-3", true),
        RowFactory.create("patient-4", null),
        RowFactory.create("patient-5", null),
        RowFactory.create("patient-6", null)
    );
  }

  @ParameterizedTest
  @MethodSource("parameters")
  public void literalLessThanOrEqualTo(final TestParameters parameters) {
    final OperatorInput input = new OperatorInput(parserContext, parameters.getLiteral(),
        parameters.getRight());
    final Operator comparisonOperator = Operator.getInstance("<=");
    final FhirPath result = comparisonOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", true),
        RowFactory.create("patient-2", true),
        RowFactory.create("patient-3", true),
        RowFactory.create("patient-4", true),
        RowFactory.create("patient-5", null),
        RowFactory.create("patient-6", null)
    );
  }

  @ParameterizedTest
  @MethodSource("parameters")
  public void literalLessThan(final TestParameters parameters) {
    final OperatorInput input = new OperatorInput(parserContext, parameters.getLiteral(),
        parameters.getRight());
    final Operator comparisonOperator = Operator.getInstance("<");
    final FhirPath result = comparisonOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", false),
        RowFactory.create("patient-2", true),
        RowFactory.create("patient-3", false),
        RowFactory.create("patient-4", false),
        RowFactory.create("patient-5", null),
        RowFactory.create("patient-6", null)
    );
  }

  @ParameterizedTest
  @MethodSource("parameters")
  public void literalGreaterThanOrEqualTo(final TestParameters parameters) {
    final OperatorInput input = new OperatorInput(parserContext, parameters.getLiteral(),
        parameters.getRight());
    final Operator comparisonOperator = Operator.getInstance(">=");
    final FhirPath result = comparisonOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", true),
        RowFactory.create("patient-2", false),
        RowFactory.create("patient-3", true),
        RowFactory.create("patient-4", true),
        RowFactory.create("patient-5", null),
        RowFactory.create("patient-6", null)
    );
  }

  @ParameterizedTest
  @MethodSource("parameters")
  public void literalGreaterThan(final TestParameters parameters) {
    final OperatorInput input = new OperatorInput(parserContext, parameters.getLiteral(),
        parameters.getRight());
    final Operator comparisonOperator = Operator.getInstance(">");
    final FhirPath result = comparisonOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", false),
        RowFactory.create("patient-2", false),
        RowFactory.create("patient-3", false),
        RowFactory.create("patient-4", false),
        RowFactory.create("patient-5", null),
        RowFactory.create("patient-6", null)
    );
  }

  @ParameterizedTest
  @MethodSource("parameters")
  public void lessThanOrEqualToLiteral(final TestParameters parameters) {
    final OperatorInput input = new OperatorInput(parserContext, parameters.getLeft(),
        parameters.getLiteral());
    final Operator comparisonOperator = Operator.getInstance("<=");
    final FhirPath result = comparisonOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", true),
        RowFactory.create("patient-2", true),
        RowFactory.create("patient-3", false),
        RowFactory.create("patient-4", null),
        RowFactory.create("patient-5", true),
        RowFactory.create("patient-6", null)
    );
  }

  @ParameterizedTest
  @MethodSource("parameters")
  public void lessThanLiteral(final TestParameters parameters) {
    final OperatorInput input = new OperatorInput(parserContext, parameters.getLeft(),
        parameters.getLiteral());
    final Operator comparisonOperator = Operator.getInstance("<");
    final FhirPath result = comparisonOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", false),
        RowFactory.create("patient-2", false),
        RowFactory.create("patient-3", false),
        RowFactory.create("patient-4", null),
        RowFactory.create("patient-5", false),
        RowFactory.create("patient-6", null)
    );
  }

  @ParameterizedTest
  @MethodSource("parameters")
  public void greaterThanOrEqualToLiteral(final TestParameters parameters) {
    final OperatorInput input = new OperatorInput(parserContext, parameters.getLeft(),
        parameters.getLiteral());
    final Operator comparisonOperator = Operator.getInstance(">=");
    final FhirPath result = comparisonOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", true),
        RowFactory.create("patient-2", true),
        RowFactory.create("patient-3", true),
        RowFactory.create("patient-4", null),
        RowFactory.create("patient-5", true),
        RowFactory.create("patient-6", null)
    );
  }

  @ParameterizedTest
  @MethodSource("parameters")
  public void greaterThanLiteral(final TestParameters parameters) {
    final OperatorInput input = new OperatorInput(parserContext, parameters.getLeft(),
        parameters.getLiteral());
    final Operator comparisonOperator = Operator.getInstance(">");
    final FhirPath result = comparisonOperator.invoke(input);

    assertThat(result).selectOrderedResult().hasRows(
        RowFactory.create("patient-1", false),
        RowFactory.create("patient-2", false),
        RowFactory.create("patient-3", true),
        RowFactory.create("patient-4", null),
        RowFactory.create("patient-5", false),
        RowFactory.create("patient-6", null)
    );
  }

}

