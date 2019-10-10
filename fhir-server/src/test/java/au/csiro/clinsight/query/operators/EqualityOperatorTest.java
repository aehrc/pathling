package au.csiro.clinsight.query.operators;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import au.csiro.clinsight.TestUtilities;
import au.csiro.clinsight.fhir.TerminologyClient;
import au.csiro.clinsight.fhir.definitions.PathResolver;
import au.csiro.clinsight.fhir.definitions.ResourceDefinitions;
import au.csiro.clinsight.query.parsing.ParsedExpression;
import au.csiro.clinsight.query.parsing.ParsedExpression.FhirPathType;
import au.csiro.clinsight.query.parsing.ParsedExpression.FhirType;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.hl7.fhir.r4.model.StringType;
import org.junit.Before;
import org.junit.Test;

/**
 * @author John Grimes
 */
public class EqualityOperatorTest {

  private SparkSession spark;

  @Before
  public void setUp() {
    spark = SparkSession.builder()
        .appName("clinsight-test")
        .config("spark.master", "local")
        .getOrCreate();

    TerminologyClient terminologyClient = mock(TerminologyClient.class);
    TestUtilities.mockDefinitionRetrieval(terminologyClient);
    ResourceDefinitions.ensureInitialized(terminologyClient);
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

    // Build up the left expression for the function.
    ParsedExpression left = new ParsedExpression();
    left.setFhirPath("gender");
    left.setFhirPathType(FhirPathType.STRING);
    left.setFhirType(FhirType.CODE);
    left.setDataset(leftDataset);
    left.setDatasetColumn("123abcd");
    left.setSingular(true);
    left.setPrimitive(true);
    left.setPathTraversal(PathResolver.resolvePath("Patient.gender"));

    // Build up the right expression for the function.
    ParsedExpression right = new ParsedExpression();
    right.setFhirPath("'female'");
    right.setFhirPathType(FhirPathType.STRING);
    right.setFhirType(FhirType.STRING);
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
    assertThat(result.getFhirType()).isEqualTo(FhirType.BOOLEAN);
    assertThat(result.isPrimitive()).isTrue();
    assertThat(result.isSingular()).isTrue();

    // Check that collecting the dataset result yields the correct values.
    Dataset<Row> resultDataset = result.getDataset();
    assertThat(resultDataset.columns().length).isEqualTo(2);
    assertThat(resultDataset.columns()[0]).isEqualTo(result.getDatasetColumn() + "_id");
    assertThat(resultDataset.columns()[1]).isEqualTo(result.getDatasetColumn());
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

    // Build up the left expression for the function.
    ParsedExpression left = new ParsedExpression();
    left.setFhirPath("location.period.start");
    left.setFhirPathType(FhirPathType.DATE_TIME);
    left.setFhirType(FhirType.DATE_TIME);
    left.setDataset(leftDataset);
    left.setDatasetColumn("123abcd");
    left.setSingular(true);
    left.setPrimitive(true);
    left.setPathTraversal(PathResolver.resolvePath("Period.start"));

    // Build up the right expression for the function.
    ParsedExpression right = new ParsedExpression();
    right.setFhirPath("location.period.end");
    right.setFhirPathType(FhirPathType.DATE_TIME);
    right.setFhirType(FhirType.DATE_TIME);
    right.setDataset(rightDataset);
    right.setDatasetColumn("789wxyz");
    right.setSingular(true);
    right.setPrimitive(true);
    right.setPathTraversal(PathResolver.resolvePath("Period.end"));

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
    assertThat(result.getFhirType()).isEqualTo(FhirType.BOOLEAN);
    assertThat(result.isPrimitive()).isTrue();
    assertThat(result.isSingular()).isTrue();

    // Check that collecting the dataset result yields the correct values.
    Dataset<Row> resultDataset = result.getDataset();
    assertThat(resultDataset.columns().length).isEqualTo(2);
    assertThat(resultDataset.columns()[0]).isEqualTo(result.getDatasetColumn() + "_id");
    assertThat(resultDataset.columns()[1]).isEqualTo(result.getDatasetColumn());
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
