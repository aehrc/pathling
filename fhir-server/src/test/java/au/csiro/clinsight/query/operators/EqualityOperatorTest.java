/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.operators;

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.clinsight.query.parsing.ParsedExpression;
import au.csiro.clinsight.query.parsing.ParsedExpression.FhirPathType;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.StringType;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * @author John Grimes
 */
@Category(au.csiro.clinsight.UnitTest.class)
public class EqualityOperatorTest {

  private SparkSession spark;

  @Before
  public void setUp() {
    spark = SparkSession.builder()
        .appName("clinsight-test")
        .config("spark.master", "local")
        .config("spark.driver.host", "localhost")
        .getOrCreate();
  }

  @Test
  public void stringEqualsLiteralString() {
    // Build up a dataset with several rows in it.
    Metadata metadata = new MetadataBuilder().build();
    StructField id = new StructField("123abcd_id", DataTypes.StringType, false, metadata);
    StructField value = new StructField("123abcd", DataTypes.StringType, true, metadata);
    StructType rowStruct = new StructType(new StructField[]{id, value});

    Row row1 = RowFactory.create("abc1", "female");
    Row row2 = RowFactory.create("abc2", "male");
    Row row3 = RowFactory.create("abc3", null);
    Dataset<Row> leftDataset = spark.createDataFrame(Arrays.asList(row1, row2, row3), rowStruct);
    Column idColumn = leftDataset.col(leftDataset.columns()[0]);
    Column valueColumn = leftDataset.col(leftDataset.columns()[1]);

    // Build up the left expression for the function.
    ParsedExpression left = new ParsedExpression();
    left.setFhirPath("gender");
    left.setFhirPathType(FhirPathType.STRING);
    left.setFhirType(FHIRDefinedType.CODE);
    left.setDataset(leftDataset);
    left.setIdColumn(idColumn);
    left.setValueColumn(valueColumn);
    left.setSingular(true);
    left.setPrimitive(true);

    // Build up the right expression for the function.
    ParsedExpression right = new ParsedExpression();
    right.setFhirPath("'female'");
    right.setFhirPathType(FhirPathType.STRING);
    right.setFhirType(FHIRDefinedType.STRING);
    right.setLiteralValue(new StringType("female"));
    right.setSingular(true);
    right.setPrimitive(true);

    BinaryOperatorInput equalityInput = new BinaryOperatorInput();
    equalityInput.setLeft(left);
    equalityInput.setRight(right);
    equalityInput.setExpression("gender = 'female'");

    // Execute the equality operator function.
    EqualityOperator equalityOperator = new EqualityOperator("=");
    ParsedExpression result = equalityOperator.invoke(equalityInput);

    // Check the result.
    assertThat(result.getFhirPath()).isEqualTo("gender = 'female'");
    assertThat(result.getFhirPathType()).isEqualTo(FhirPathType.BOOLEAN);
    assertThat(result.getFhirType()).isEqualTo(FHIRDefinedType.BOOLEAN);
    assertThat(result.isPrimitive()).isTrue();
    assertThat(result.isSingular()).isTrue();
    assertThat(result.getIdColumn()).isNotNull();
    assertThat(result.getValueColumn()).isNotNull();

    // Check that collecting the dataset result yields the correct values.
    Dataset<Row> resultDataset = result.getDataset()
        .select(result.getIdColumn(), result.getValueColumn());
    List<Row> resultRows = resultDataset.collectAsList();
    assertThat(resultRows.size()).isEqualTo(3);
    Row resultRow = resultRows.get(0);
    assertThat(resultRow.getString(0)).isEqualTo("abc1");
    assertThat(resultRow.getBoolean(1)).isEqualTo(true);
    resultRow = resultRows.get(1);
    assertThat(resultRow.getString(0)).isEqualTo("abc2");
    assertThat(resultRow.getBoolean(1)).isEqualTo(false);
    resultRow = resultRows.get(2);
    assertThat(resultRow.getString(0)).isEqualTo("abc3");
    assertThat(resultRow.getBoolean(1)).isEqualTo(false);
  }

  @Test
  public void dateNotEqualsDate() throws ParseException {
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    // Build up a dataset with several rows in it for the left expression.
    Metadata leftMetadata = new MetadataBuilder().build();
    StructField leftId = new StructField("123abcd_id", DataTypes.StringType, false, leftMetadata);
    StructField leftValue = new StructField("123abcd", DataTypes.DateType, true, leftMetadata);
    StructType leftRowStruct = new StructType(new StructField[]{leftId, leftValue});

    Row leftRow1 = RowFactory.create("abc1", new Date(dateFormat.parse("1983-06-21").getTime()));
    Row leftRow2 = RowFactory.create("abc2", new Date(dateFormat.parse("1981-07-26").getTime()));
    Row leftRow3 = RowFactory.create("abc3", new Date(dateFormat.parse("1955-05-21").getTime()));
    Row leftRow4 = RowFactory.create("abc4", null);
    Dataset<Row> leftDataset = spark
        .createDataFrame(Arrays.asList(leftRow1, leftRow2, leftRow3, leftRow4), leftRowStruct);
    Column leftIdColumn = leftDataset.col(leftDataset.columns()[0]);
    Column leftValueColumn = leftDataset.col(leftDataset.columns()[1]);

    // Build up a dataset with several rows in it for the right expression.
    Metadata rightMetadata = new MetadataBuilder().build();
    StructField rightId = new StructField("789wxyz_id", DataTypes.StringType, false, rightMetadata);
    StructField rightValue = new StructField("789wxyz", DataTypes.DateType, true, rightMetadata);
    StructType rightRowStruct = new StructType(new StructField[]{rightId, rightValue});

    Row rightRow1 = RowFactory.create("abc1", new Date(dateFormat.parse("1983-06-21").getTime()));
    Row rightRow2 = RowFactory.create("abc2", new Date(dateFormat.parse("2018-05-18").getTime()));
    Row rightRow3 = RowFactory.create("abc3", null);
    Row rightRow4 = RowFactory.create("abc4", null);
    Dataset<Row> rightDataset = spark
        .createDataFrame(Arrays.asList(rightRow1, rightRow2, rightRow3, rightRow4), rightRowStruct);
    Column rightIdColumn = rightDataset.col(rightDataset.columns()[0]);
    Column rightValueColumn = rightDataset.col(rightDataset.columns()[1]);

    // Build up the left expression for the function.
    ParsedExpression left = new ParsedExpression();
    left.setFhirPath("location.period.start");
    left.setFhirPathType(FhirPathType.DATE_TIME);
    left.setFhirType(FHIRDefinedType.DATETIME);
    left.setDataset(leftDataset);
    left.setIdColumn(leftIdColumn);
    left.setValueColumn(leftValueColumn);
    left.setSingular(true);
    left.setPrimitive(true);

    // Build up the right expression for the function.
    ParsedExpression right = new ParsedExpression();
    right.setFhirPath("location.period.end");
    right.setFhirPathType(FhirPathType.DATE_TIME);
    right.setFhirType(FHIRDefinedType.DATETIME);
    right.setDataset(rightDataset);
    right.setIdColumn(rightIdColumn);
    right.setValueColumn(rightValueColumn);
    right.setSingular(true);
    right.setPrimitive(true);

    BinaryOperatorInput equalityInput = new BinaryOperatorInput();
    equalityInput.setLeft(left);
    equalityInput.setRight(right);
    equalityInput.setExpression("location.period.start = location.period.end");

    // Execute the equality operator function.
    EqualityOperator equalityOperator = new EqualityOperator("!=");
    ParsedExpression result = equalityOperator.invoke(equalityInput);

    // Check the result.
    assertThat(result.getFhirPath()).isEqualTo(equalityInput.getExpression());
    assertThat(result.getFhirPathType()).isEqualTo(FhirPathType.BOOLEAN);
    assertThat(result.getFhirType()).isEqualTo(FHIRDefinedType.BOOLEAN);
    assertThat(result.isPrimitive()).isTrue();
    assertThat(result.isSingular()).isTrue();
    assertThat(result.getIdColumn()).isNotNull();
    assertThat(result.getValueColumn()).isNotNull();

    // Check that collecting the dataset result yields the correct values.
    Dataset<Row> resultDataset = result.getDataset()
        .select(result.getIdColumn(), result.getValueColumn());
    List<Row> resultRows = resultDataset.collectAsList();
    assertThat(resultRows.size()).isEqualTo(4);
    Row resultRow = resultRows.get(0);
    assertThat(resultRow.getString(0)).isEqualTo("abc1");
    assertThat(resultRow.getBoolean(1)).isEqualTo(false);
    resultRow = resultRows.get(1);
    assertThat(resultRow.getString(0)).isEqualTo("abc2");
    assertThat(resultRow.getBoolean(1)).isEqualTo(true);
    resultRow = resultRows.get(2);
    assertThat(resultRow.getString(0)).isEqualTo("abc3");
    assertThat(resultRow.getBoolean(1)).isEqualTo(true);
    resultRow = resultRows.get(3);
    assertThat(resultRow.getString(0)).isEqualTo("abc4");
    assertThat(resultRow.getBoolean(1)).isEqualTo(false);
  }
}
