package au.csiro.clinsight.query.functions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.clinsight.TestUtilities;
import au.csiro.clinsight.query.ResourceReader;
import au.csiro.clinsight.query.parsing.ExpressionParserContext;
import au.csiro.clinsight.query.parsing.ParsedExpression;
import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.RuntimeChildResourceDefinition;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * @author John Grimes
 */
@Category(au.csiro.clinsight.UnitTest.class)
public class ResolveFunctionTest {

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
    Column idColumn = inputDataset.col(inputDataset.columns()[0]);
    Column valueColumn = inputDataset.col(inputDataset.columns()[1]);

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

    BaseRuntimeChildDefinition childDefinition = TestUtilities.getFhirContext()
        .getResourceDefinition("Encounter").getChildByName("episodeOfCare");
    assertThat(childDefinition).isInstanceOf(RuntimeChildResourceDefinition.class);

    // Prepare the inputs to the function.
    ExpressionParserContext parserContext = new ExpressionParserContext();
    parserContext.setResourceReader(mockReader);

    ParsedExpression inputExpression = new ParsedExpression();
    inputExpression.setFhirPath("episodeOfCare");
    inputExpression.setFhirType(FHIRDefinedType.REFERENCE);
    inputExpression.setDefinition(childDefinition);
    inputExpression.setSingular(false);
    inputExpression.setDataset(inputDataset);
    inputExpression.setIdColumn(idColumn);
    inputExpression.setValueColumn(valueColumn);

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
    assertThat(result.isPolymorphic()).isFalse();
    assertThat(result.getIdColumn()).isNotNull();
    assertThat(result.getValueColumn()).isNotNull();
    assertThat(result.getResourceTypeColumn()).isNull();

    // Check the result dataset.
    Dataset<Row> resultDataset = result.getDataset()
        .select(result.getIdColumn(), result.getValueColumn());
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
    Column idColumn = inputDataset.col(inputDataset.columns()[0]);
    Column valueColumn = inputDataset.col(inputDataset.columns()[1]);

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
    StructType patientResource = new StructType(
        new StructField[]{patientIdColumn, patientGenderColumn, patientActiveColumn});
    StructField patientStruct = new StructField("resource", patientResource, false, targetMetadata);
    StructType patientRecord = new StructType(new StructField[]{patientIdColumn, patientStruct});

    StructField groupIdColumn = new StructField("id", DataTypes.StringType, false,
        targetMetadata);
    StructField groupNameColumn = new StructField("name", DataTypes.StringType, true,
        targetMetadata);
    StructField groupActiveColumn = new StructField("active", DataTypes.BooleanType, true,
        targetMetadata);
    StructType groupResource = new StructType(
        new StructField[]{groupIdColumn, groupNameColumn, groupActiveColumn});
    StructField groupStruct = new StructField("resource", groupResource, false, targetMetadata);
    StructType groupRecord = new StructType(new StructField[]{groupIdColumn, groupStruct});

    Row patientRow1 = RowFactory
        .create("Patient/abc1", RowFactory.create("Patient/abc1", "female", true));
    Row patientRow2 = RowFactory
        .create("Patient/abc2", RowFactory.create("Patient/abc2", "female", false));
    Row patientRow3 = RowFactory
        .create("Patient/abc3", RowFactory.create("Patient/abc3", "male", true));
    Dataset<Row> patientDataset = spark
        .createDataFrame(Arrays.asList(patientRow1, patientRow2, patientRow3), patientRecord);
    when(mockReader.read(ResourceType.PATIENT))
        .thenReturn(patientDataset);

    BaseRuntimeChildDefinition childDefinition = TestUtilities.getFhirContext()
        .getResourceDefinition("Encounter").getChildByName("subject");
    assertThat(childDefinition).isInstanceOf(RuntimeChildResourceDefinition.class);

    Row groupRow = RowFactory
        .create("Group/def1", RowFactory.create("Group/def1", "Some group", true));
    Dataset<Row> groupDataset = spark
        .createDataFrame(Collections.singletonList(groupRow), groupRecord);
    when(mockReader.read(ResourceType.GROUP))
        .thenReturn(groupDataset);

    when(mockReader.getAvailableResourceTypes())
        .thenReturn(new HashSet<>(Arrays.asList(ResourceType.PATIENT, ResourceType.GROUP)));

    // Prepare the inputs to the function.
    ExpressionParserContext parserContext = new ExpressionParserContext();
    parserContext.setResourceReader(mockReader);

    ParsedExpression inputExpression = new ParsedExpression();
    inputExpression.setFhirPath("subject");
    inputExpression.setFhirType(FHIRDefinedType.REFERENCE);
    inputExpression.setDefinition(childDefinition);
    inputExpression.setSingular(true);
    inputExpression.setDataset(inputDataset);
    inputExpression.setIdColumn(idColumn);
    inputExpression.setValueColumn(valueColumn);

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
    assertThat(result.getIdColumn()).isNotNull();
    assertThat(result.getValueColumn()).isNotNull();
    assertThat(result.getResourceTypeColumn()).isNotNull();

    // Check the result dataset.
    Dataset<Row> resultDataset = result.getDataset()
        .select(result.getIdColumn(), result.getResourceTypeColumn(), result.getValueColumn());
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