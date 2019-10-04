package au.csiro.clinsight.query.functions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.clinsight.TestUtilities;
import au.csiro.clinsight.fhir.TerminologyClient;
import au.csiro.clinsight.fhir.definitions.PathResolver;
import au.csiro.clinsight.fhir.definitions.PathTraversal;
import au.csiro.clinsight.fhir.definitions.ResourceDefinitions;
import au.csiro.clinsight.query.ResourceReader;
import au.csiro.clinsight.query.parsing.ExpressionParserContext;
import au.csiro.clinsight.query.parsing.ParsedExpression;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.Before;
import org.junit.Test;

/**
 * @author John Grimes
 */
public class ResolveFunctionTest {

  private SparkSession spark;

  @Before
  public void setUp() {
    spark = SparkSession.builder()
        .appName("clinsight-test")
        .config("spark.master", "local")
        .config("spark.driver.host", "localhost")
        .getOrCreate();

    TerminologyClient terminologyClient = mock(TerminologyClient.class);
    TestUtilities.mockDefinitionRetrieval(terminologyClient);
    ResourceDefinitions.ensureInitialized(terminologyClient);
  }

  @Test
  public void simpleResolve() {
    // Build a dataset which represents the input to the function.
    Metadata inputMetadata = new MetadataBuilder().build();
    StructField reference = new StructField("reference", DataTypes.StringType, true, inputMetadata);
    StructField display = new StructField("display", DataTypes.StringType, true, inputMetadata);
    StructType referenceStruct = new StructType(new StructField[]{reference, display});
    StructField inputId = new StructField("789wxyz_id", DataTypes.StringType, false,
        inputMetadata);
    StructField inputValue = new StructField("789wxyz", referenceStruct, true,
        inputMetadata);
    StructType inputStruct = new StructType(new StructField[]{inputId, inputValue});

    Row inputRow1 = RowFactory
        .create("Encounter/xyz1", RowFactory.create("EpisodeOfCare/abc1", null));
    Row inputRow2 = RowFactory
        .create("Encounter/xyz2", RowFactory.create("EpisodeOfCare/abc3", null));
    Row inputRow3 = RowFactory
        .create("Encounter/xyz3", RowFactory.create("EpisodeOfCare/abc2", null));
    Row inputRow4 = RowFactory
        .create("Encounter/xyz4", RowFactory.create("EpisodeOfCare/abc2", null));
    Dataset<Row> inputDataset = spark
        .createDataFrame(Arrays.asList(inputRow1, inputRow2, inputRow3, inputRow4),
            inputStruct);

    // Build a Dataset with several rows in it, and create a mock reader which returns it as the
    // set of all Patient resources.
    ResourceReader mockReader = mock(ResourceReader.class);
    Metadata targetMetadata = new MetadataBuilder().build();
    StructField episodeOfCareIdColumn = new StructField("id", DataTypes.StringType, false,
        targetMetadata);
    StructField episodeOfCareStatusColumn = new StructField("status", DataTypes.StringType, true,
        targetMetadata);
    StructType episodeOfCareStruct = new StructType(
        new StructField[]{episodeOfCareIdColumn, episodeOfCareStatusColumn});

    Row episodeOfCareRow1 = RowFactory.create("EpisodeOfCare/abc1", "planned");
    Row episodeOfCareRow2 = RowFactory.create("EpisodeOfCare/abc2", "waitlist");
    Row episodeOfCareRow3 = RowFactory.create("EpisodeOfCare/abc3", "active");
    Dataset<Row> episodeOfCareDataset = spark
        .createDataFrame(Arrays.asList(episodeOfCareRow1, episodeOfCareRow2, episodeOfCareRow3),
            episodeOfCareStruct);
    when(mockReader.read(ResourceType.EPISODEOFCARE))
        .thenReturn(episodeOfCareDataset);

    // Prepare the inputs to the function.
    ExpressionParserContext parserContext = new ExpressionParserContext();
    parserContext.setResourceReader(mockReader);

    ParsedExpression inputExpression = new ParsedExpression();
    inputExpression.setFhirPath("episodeOfCare");
    inputExpression.setPathTraversal(PathResolver.resolvePath("Encounter.episodeOfCare"));
    inputExpression.setSingular(false);
    inputExpression.setDataset(inputDataset);
    inputExpression.setDatasetColumn("789wxyz");

    FunctionInput resolveInput = new FunctionInput();
    resolveInput.setContext(parserContext);
    resolveInput.setInput(inputExpression);
    resolveInput.setExpression("resolve()");

    // Execute the function.
    ResolveFunction resolveFunction = new ResolveFunction();
    ParsedExpression result = resolveFunction.invoke(resolveInput);

    // Check the result.
    assertThat(result.getFhirPath()).isEqualTo("resolve()");
    assertThat(result.isSingular()).isFalse();
    assertThat(result.isResource()).isTrue();
    assertThat(result.getResourceType()).isEqualTo(ResourceType.EPISODEOFCARE);
    assertThat(result.getPathTraversal()).isInstanceOf(PathTraversal.class);

    // Check the result dataset.
    Dataset<Row> resultDataset = result.getDataset();
    assertThat(resultDataset.columns().length).isEqualTo(2);
    assertThat(resultDataset.columns()[0]).isEqualTo(result.getDatasetColumn() + "_id");
    assertThat(resultDataset.columns()[1]).isEqualTo(result.getDatasetColumn());
    List<Row> resultRows = resultDataset.collectAsList();
    assertThat(resultRows.size()).isEqualTo(4);
    Row resultRow = resultRows.get(0);
    assertThat(resultRow.getString(0)).isEqualTo("Encounter/xyz1");
    assertThat(resultRow.getStruct(1).get(0)).isEqualTo("EpisodeOfCare/abc1");
    assertThat(resultRow.getStruct(1).get(1)).isEqualTo("planned");
    resultRow = resultRows.get(1);
    assertThat(resultRow.getString(0)).isEqualTo("Encounter/xyz2");
    assertThat(resultRow.getStruct(1).get(0)).isEqualTo("EpisodeOfCare/abc3");
    assertThat(resultRow.getStruct(1).get(1)).isEqualTo("active");
    resultRow = resultRows.get(2);
    assertThat(resultRow.getString(0)).isEqualTo("Encounter/xyz3");
    assertThat(resultRow.getStruct(1).get(0)).isEqualTo("EpisodeOfCare/abc2");
    assertThat(resultRow.getStruct(1).get(1)).isEqualTo("waitlist");
    resultRow = resultRows.get(3);
    assertThat(resultRow.getString(0)).isEqualTo("Encounter/xyz4");
    assertThat(resultRow.getStruct(1).get(0)).isEqualTo("EpisodeOfCare/abc2");
    assertThat(resultRow.getStruct(1).get(1)).isEqualTo("waitlist");
  }

  @Test
  public void polymorphicResolve() {
    // Build a dataset which represents the input to the function.
    Metadata inputMetadata = new MetadataBuilder().build();
    StructField reference = new StructField("reference", DataTypes.StringType, true, inputMetadata);
    StructField display = new StructField("display", DataTypes.StringType, true, inputMetadata);
    StructType referenceStruct = new StructType(new StructField[]{reference, display});
    StructField inputId = new StructField("789wxyz_id", DataTypes.StringType, false,
        inputMetadata);
    StructField inputValue = new StructField("789wxyz", referenceStruct, true,
        inputMetadata);
    StructType inputStruct = new StructType(new StructField[]{inputId, inputValue});

    Row inputRow1 = RowFactory.create("Encounter/xyz1", RowFactory.create("Patient/abc1", null));
    Row inputRow2 = RowFactory.create("Encounter/xyz2", RowFactory.create("Patient/abc3", null));
    Row inputRow3 = RowFactory.create("Encounter/xyz3", RowFactory.create("Patient/abc2", null));
    Row inputRow4 = RowFactory.create("Encounter/xyz4", RowFactory.create("Patient/abc2", null));
    Row inputRow5 = RowFactory.create("Encounter/xyz5", RowFactory.create("Group/def1", null));
    Dataset<Row> inputDataset = spark
        .createDataFrame(Arrays.asList(inputRow1, inputRow2, inputRow3, inputRow4, inputRow5),
            inputStruct);

    // Build a Dataset with several rows in it, and create a mock reader which returns it as the
    // set of all Patient resources.
    ResourceReader mockReader = mock(ResourceReader.class);
    Metadata targetMetadata = new MetadataBuilder().build();
    StructField patientIdColumn = new StructField("id", DataTypes.StringType, false,
        targetMetadata);
    StructField patientGenderColumn = new StructField("gender", DataTypes.StringType, true,
        targetMetadata);
    StructField patientActiveColumn = new StructField("active", DataTypes.BooleanType, true,
        targetMetadata);
    StructType patientStruct = new StructType(
        new StructField[]{patientIdColumn, patientGenderColumn, patientActiveColumn});
    StructField groupIdColumn = new StructField("id", DataTypes.StringType, false,
        targetMetadata);
    StructField groupNameColumn = new StructField("name", DataTypes.StringType, true,
        targetMetadata);
    StructField groupActiveColumn = new StructField("active", DataTypes.BooleanType, true,
        targetMetadata);
    StructType groupStruct = new StructType(
        new StructField[]{groupIdColumn, groupNameColumn, groupActiveColumn});

    Row patientRow1 = RowFactory.create("Patient/abc1", "female", true);
    Row patientRow2 = RowFactory.create("Patient/abc2", "female", false);
    Row patientRow3 = RowFactory.create("Patient/abc3", "male", true);
    Dataset<Row> patientDataset = spark
        .createDataFrame(Arrays.asList(patientRow1, patientRow2, patientRow3), patientStruct);
    when(mockReader.read(ResourceType.PATIENT))
        .thenReturn(patientDataset);

    Row groupRow = RowFactory.create("Group/def1", "Some group", true);
    Dataset<Row> groupDataset = spark
        .createDataFrame(Collections.singletonList(groupRow), patientStruct);
    when(mockReader.read(ResourceType.GROUP))
        .thenReturn(groupDataset);

    // Prepare the inputs to the function.
    ExpressionParserContext parserContext = new ExpressionParserContext();
    parserContext.setResourceReader(mockReader);

    ParsedExpression inputExpression = new ParsedExpression();
    inputExpression.setFhirPath("patient");
    inputExpression.setPathTraversal(PathResolver.resolvePath("Encounter.subject"));
    inputExpression.setSingular(true);
    inputExpression.setDataset(inputDataset);
    inputExpression.setDatasetColumn("789wxyz");

    FunctionInput resolveInput = new FunctionInput();
    resolveInput.setContext(parserContext);
    resolveInput.setInput(inputExpression);
    resolveInput.setExpression("resolve()");

    // Execute the function.
    ResolveFunction resolveFunction = new ResolveFunction();
    ParsedExpression result = resolveFunction.invoke(resolveInput);

    // Check the result.
    assertThat(result.getFhirPath()).isEqualTo("resolve()");
    assertThat(result.isSingular()).isTrue();
    assertThat(result.isResource()).isFalse();
    assertThat(result.getResourceType()).isNull();
    assertThat(result.isPolymorphic()).isTrue();

    // Check the result dataset.
    Dataset<Row> resultDataset = result.getDataset();
    assertThat(resultDataset.columns().length).isEqualTo(3);
    assertThat(resultDataset.columns()[0]).isEqualTo(result.getDatasetColumn() + "_id");
    assertThat(resultDataset.columns()[1]).isEqualTo(result.getDatasetColumn() + "_type");
    assertThat(resultDataset.columns()[2]).isEqualTo(result.getDatasetColumn());
    List<Row> resultRows = resultDataset.collectAsList();
    assertThat(resultRows.size()).isEqualTo(5);
    Row resultRow = resultRows.get(0);
    assertThat(resultRow.getString(0)).isEqualTo("Encounter/xyz1");
    assertThat(resultRow.getString(1)).isEqualTo("Patient");
    assertThat(resultRow.getString(2)).isEqualTo("Patient/abc1");
    resultRow = resultRows.get(1);
    assertThat(resultRow.getString(0)).isEqualTo("Encounter/xyz2");
    assertThat(resultRow.getString(1)).isEqualTo("Patient");
    assertThat(resultRow.getString(2)).isEqualTo("Patient/abc3");
    resultRow = resultRows.get(2);
    assertThat(resultRow.getString(0)).isEqualTo("Encounter/xyz3");
    assertThat(resultRow.getString(1)).isEqualTo("Patient");
    assertThat(resultRow.getString(2)).isEqualTo("Patient/abc2");
    resultRow = resultRows.get(3);
    assertThat(resultRow.getString(0)).isEqualTo("Encounter/xyz4");
    assertThat(resultRow.getString(1)).isEqualTo("Patient");
    assertThat(resultRow.getString(2)).isEqualTo("Patient/abc2");
    resultRow = resultRows.get(4);
    assertThat(resultRow.getString(0)).isEqualTo("Encounter/xyz5");
    assertThat(resultRow.getString(1)).isEqualTo("Group");
    assertThat(resultRow.getString(2)).isEqualTo("Group/def1");
  }
}