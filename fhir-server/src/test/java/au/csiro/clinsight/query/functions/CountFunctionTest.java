package au.csiro.clinsight.query.functions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;

import au.csiro.clinsight.TestUtilities;
import au.csiro.clinsight.fhir.TerminologyClient;
import au.csiro.clinsight.fhir.definitions.PathResolver;
import au.csiro.clinsight.fhir.definitions.ResourceDefinitions;
import au.csiro.clinsight.query.parsing.ExpressionParserContext;
import au.csiro.clinsight.query.parsing.ParsedExpression;
import au.csiro.clinsight.query.parsing.ParsedExpression.FhirPathType;
import au.csiro.clinsight.query.parsing.ParsedExpression.FhirType;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.junit.Before;
import org.junit.Test;

/**
 * @author John Grimes
 */
public class CountFunctionTest {

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
  public void resourceCount() {
    // Build a Dataset with several rows in it.
    Metadata metadata = new MetadataBuilder().build();
    StructField genderColumn = new StructField("gender", DataTypes.StringType, true, metadata);
    StructField activeColumn = new StructField("active", DataTypes.BooleanType, true, metadata);
    StructType resourceStruct = new StructType(new StructField[]{genderColumn, activeColumn});
    StructField id = new StructField("123abcd_id", DataTypes.StringType, false, metadata);
    StructField resource = new StructField("123abcd", resourceStruct, false, metadata);
    StructType rowStruct = new StructType(new StructField[]{id, resource});

    Row row1 = RowFactory.create("abc1", RowFactory.create("female", true));
    Row row2 = RowFactory.create("abc2", RowFactory.create("female", false));
    Row row3 = RowFactory.create("abc3", RowFactory.create("male", true));
    Dataset<Row> dataset = spark.createDataFrame(Arrays.asList(row1, row2, row3), rowStruct);

    // Build up an input for the function.
    ParsedExpression input = new ParsedExpression();
    input.setFhirPath("%resource");
    input.setResource(true);
    input.setResourceDefinition("http://hl7.org/fhir/StructureDefinition/Patient");
    input.setOrigin(input);
    input.setDataset(dataset);
    input.setDatasetColumn("123abcd");
    input.setPathTraversal(PathResolver.resolvePath("Patient"));

    FunctionInput countInput = new FunctionInput();
    countInput.setInput(input);
    countInput.setExpression("count()");

    // Execute the count function.
    CountFunction countFunction = new CountFunction();
    ParsedExpression result = countFunction.invoke(countInput);

    // Check the result.
    assertThat(result.getFhirPath()).isEqualTo("count()");
    assertThat(result.getFhirPathType()).isEqualTo(FhirPathType.INTEGER);
    assertThat(result.getFhirType()).isEqualTo(FhirType.UNSIGNED_INT);
    assertThat(result.isPrimitive()).isTrue();
    assertThat(result.isSingular()).isTrue();
    assertThat(result.getAggregation()).isInstanceOf(Column.class);

    // Check that running the aggregation against the Dataset results in the correct count.
    Dataset<Row> resultDataset = result.getDataset();
    assertThat(resultDataset.columns().length).isEqualTo(2);
    assertThat(resultDataset.columns()[0]).isEqualTo(result.getDatasetColumn() + "_id");
    assertThat(resultDataset.columns()[1]).isEqualTo(result.getDatasetColumn());
    List<Row> resultRows = resultDataset.agg(result.getAggregation()).collectAsList();
    assertThat(resultRows.size()).isEqualTo(1);
    Row resultRow = resultRows.get(0);
    assertThat(resultRow.getLong(0)).isEqualTo(3);

    // Check that running the aggregation results in the correct distinct count when joined against
    // grouping rows that can have multiple cardinalities.
    Metadata groupingMetadata = new MetadataBuilder().build();
    StructField groupingId = new StructField("789wxyz_id", DataTypes.StringType, false,
        groupingMetadata);
    StructField groupingValue = new StructField("789wxyz", DataTypes.StringType, true,
        groupingMetadata);
    StructType groupingStruct = new StructType(new StructField[]{groupingId, groupingValue});

    Row groupingRow1 = RowFactory.create("abc1", "en");
    Row groupingRow2 = RowFactory.create("abc2", "en");
    Row groupingRow3 = RowFactory.create("abc2", "de");
    Row groupingRow4 = RowFactory.create("abc3", "fr");
    // This one might be doubled up due to joining against some other multi-cardinality grouping.
    Row groupingRow5 = RowFactory.create("abc3", "fr");
    Dataset<Row> groupingDataset = spark.createDataFrame(
        Arrays.asList(groupingRow1, groupingRow2, groupingRow3, groupingRow4, groupingRow5),
        groupingStruct);
    Dataset<Row> groupedResult = groupingDataset.join(resultDataset,
        resultDataset.col("123abcd_id").equalTo(groupingDataset.col("789wxyz_id")), "left_outer");
    groupedResult = groupedResult.groupBy(groupingDataset.col("789wxyz"))
        .agg(result.getAggregation());
    List<Row> groupedResultRows = groupedResult.collectAsList();

    assertThat(groupedResultRows.size()).isEqualTo(3);
    Row groupedResultRow = groupedResultRows.get(0);
    assertThat(groupedResultRow.getString(0)).isEqualTo("en");
    assertThat(groupedResultRow.getLong(1)).isEqualTo(2);
    groupedResultRow = groupedResultRows.get(1);
    assertThat(groupedResultRow.getString(0)).isEqualTo("de");
    assertThat(groupedResultRow.getLong(1)).isEqualTo(1);
    groupedResultRow = groupedResultRows.get(2);
    assertThat(groupedResultRow.getString(0)).isEqualTo("fr");
    // This should only be 1, as there is only one Patient resource captured in the "fr" group.
    assertThat(groupedResultRow.getLong(1)).isEqualTo(1);
  }

  @Test
  public void elementCount() {
    // Build a Dataset with several rows in it.
    Metadata metadata = new MetadataBuilder().build();
    StructField id = new StructField("123abcd_id", DataTypes.StringType, false, metadata);
    StructField value = new StructField("123abcd", DataTypes.StringType, true, metadata);
    StructType rowStruct = new StructType(new StructField[]{id, value});

    // Multiple values against the same resource should be counted individually.
    Row row1 = RowFactory.create("abc1", "Jude");
    Row row2 = RowFactory.create("abc1", "Samuel");
    Row row3 = RowFactory.create("abc2", "Thomas");
    // The absence of the element should not be counted.
    Row row4 = RowFactory.create("abc3", null);
    // A duplicate value should be counted.
    Row row5 = RowFactory.create("abc4", "Thomas");
    Dataset<Row> dataset = spark
        .createDataFrame(Arrays.asList(row1, row2, row3, row4, row5), rowStruct);

    ExpressionParserContext expressionParserContext = new ExpressionParserContext();
    expressionParserContext.getGroupings().add(mock(ParsedExpression.class));

    // Build up an input for the function.
    ParsedExpression input = new ParsedExpression();
    input.setFhirPath("%resource.name.given");
    input.setDataset(dataset);
    input.setDatasetColumn("123abcd");
    input.setPathTraversal(PathResolver.resolvePath("Patient"));

    FunctionInput countInput = new FunctionInput();
    countInput.setInput(input);
    countInput.setExpression("count()");
    countInput.setContext(expressionParserContext);

    // Execute the count function.
    CountFunction countFunction = new CountFunction();
    ParsedExpression result = countFunction.invoke(countInput);

    // Check the result.
    assertThat(result.getFhirPath()).isEqualTo("count()");
    assertThat(result.getFhirPathType()).isEqualTo(FhirPathType.INTEGER);
    assertThat(result.getFhirType()).isEqualTo(FhirType.UNSIGNED_INT);
    assertThat(result.isPrimitive()).isTrue();
    assertThat(result.isSingular()).isTrue();
    assertThat(result.getAggregation()).isInstanceOf(Column.class);

    // Check that running the aggregation against the Dataset results in the correct count.
    Dataset<Row> resultDataset = result.getDataset();
    assertThat(resultDataset.columns().length).isEqualTo(2);
    assertThat(resultDataset.columns()[0]).isEqualTo(result.getDatasetColumn() + "_id");
    assertThat(resultDataset.columns()[1]).isEqualTo(result.getDatasetColumn());
    List<Row> resultRows = resultDataset.agg(result.getAggregation()).collectAsList();
    assertThat(resultRows.size()).isEqualTo(1);
    Row resultRow = resultRows.get(0);
    assertThat(resultRow.getLong(0)).isEqualTo(4);

    // Check that running the aggregation results in the correct distinct count when joined against
    // grouping rows that can have multiple cardinalities.
    //
    // For example, I want to count patient given names, and group on languages. If I join from
    // [patient ID, language] to [patient ID, given name] and count the given names, I will get
    // [number of languages] * [number of given names] as the count, rather than [number of given
    // names].
    Metadata groupingMetadata = new MetadataBuilder().build();
    StructField groupingId = new StructField("789wxyz_id", DataTypes.StringType, false,
        groupingMetadata);
    StructField groupingValue = new StructField("789wxyz", DataTypes.StringType, true,
        groupingMetadata);
    StructType groupingStruct = new StructType(new StructField[]{groupingId, groupingValue});

    Row groupingRow1 = RowFactory.create("abc1", "en");
    Row groupingRow2 = RowFactory.create("abc2", "en");
    Row groupingRow3 = RowFactory.create("abc2", "de");
    Row groupingRow4 = RowFactory.create("abc3", "en");
    Row groupingRow5 = RowFactory.create("abc4", "fr");
    // This one might be doubled up due to joining against some other multi-cardinality grouping.
    Row groupingRow6 = RowFactory.create("abc4", "fr");
    Dataset<Row> groupingDataset = spark.createDataFrame(
        Arrays.asList(groupingRow1, groupingRow2, groupingRow3, groupingRow4, groupingRow5,
            groupingRow6),
        groupingStruct);
    Dataset<Row> groupedResult = groupingDataset.join(resultDataset,
        resultDataset.col("123abcd_id").equalTo(groupingDataset.col("789wxyz_id")), "left_outer");
    groupedResult = groupedResult.groupBy(groupingDataset.col("789wxyz"))
        .agg(result.getAggregation());
    List<Row> groupedResultRows = groupedResult.collectAsList();

    assertThat(groupedResultRows.size()).isEqualTo(3);
    Row groupedResultRow = groupedResultRows.get(0);
    assertThat(groupedResultRow.getString(0)).isEqualTo("en");
    assertThat(groupedResultRow.getLong(1)).isEqualTo(3);
    groupedResultRow = groupedResultRows.get(1);
    assertThat(groupedResultRow.getString(0)).isEqualTo("de");
    assertThat(groupedResultRow.getLong(1)).isEqualTo(1);
    groupedResultRow = groupedResultRows.get(2);
    assertThat(groupedResultRow.getString(0)).isEqualTo("fr");
    // This should only be 1, as there is only one given name for all of the Patient resources
    // captured in the "fr" group.
    assertThat(groupedResultRow.getLong(1)).isEqualTo(1);
  }

  @Test
  public void inputMustNotContainArguments() {
    // Build up an input for the function.
    ExpressionParserContext expressionParserContext = new ExpressionParserContext();
    expressionParserContext.getGroupings().add(mock(ParsedExpression.class));

    FunctionInput countInput = new FunctionInput();
    countInput.setInput(mock(ParsedExpression.class));
    countInput.setExpression("count('some argument')");
    countInput.setContext(expressionParserContext);
    countInput.getArguments().add(mock(ParsedExpression.class));

    // Execute the function and assert that it throws the right exception.
    CountFunction countFunction = new CountFunction();
    assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> countFunction.invoke(countInput))
        .withMessage("Arguments can not be passed to count function: count('some argument')");
  }
}