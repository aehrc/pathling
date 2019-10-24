package au.csiro.clinsight.query.functions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.clinsight.query.ResourceReader;
import au.csiro.clinsight.query.parsing.ExpressionParserContext;
import au.csiro.clinsight.query.parsing.ParsedExpression;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * @author John Grimes
 */
@Category(au.csiro.clinsight.UnitTest.class)
public class OfTypeFunctionTest {

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
  public void polymorphicReferenceResolution() {
    // Build a dataset which represents the input to the function.
    Metadata metadata = new MetadataBuilder().build();
    StructField inputId = new StructField("789wxyz_id", DataTypes.StringType, false,
        metadata);
    StructField inputType = new StructField("789wxyz_type", DataTypes.StringType, true,
        metadata);
    StructField inputValue = new StructField("789wxyz", DataTypes.StringType, true,
        metadata);
    StructType inputStruct = new StructType(new StructField[]{inputId, inputType, inputValue});

    Row inputRow1 = RowFactory.create("Encounter/xyz1", "Patient", "Patient/abc1");
    Row inputRow2 = RowFactory.create("Encounter/xyz2", "Patient", "Patient/abc2");
    Row inputRow3 = RowFactory.create("Encounter/xyz3", "Patient", "Patient/abc2");
    Row inputRow4 = RowFactory.create("Encounter/xyz4", "Patient", "Patient/abc2");
    Row inputRow5 = RowFactory.create("Encounter/xyz5", "Group", "Group/def1");
    Dataset<Row> inputDataset = spark
        .createDataFrame(Arrays.asList(inputRow1, inputRow2, inputRow3, inputRow4, inputRow5),
            inputStruct);
    Column idColumn = inputDataset.col(inputDataset.columns()[0]);
    Column typeColumn = inputDataset.col(inputDataset.columns()[1]);
    Column valueColumn = inputDataset.col(inputDataset.columns()[2]);

    // Create a mock reader which will return a mock Patient dataset.
    ResourceReader mockReader = mock(ResourceReader.class);
    StructField patientIdColumn = new StructField("id", DataTypes.StringType, false, metadata);
    StructField patientGenderColumn = new StructField("gender", DataTypes.StringType, true,
        metadata);
    StructField patientActiveColumn = new StructField("active", DataTypes.BooleanType, true,
        metadata);
    StructType patientResource = new StructType(
        new StructField[]{patientIdColumn, patientGenderColumn, patientActiveColumn});
    StructField patientStruct = new StructField("resource", patientResource, false, metadata);
    StructType patientRecord = new StructType(new StructField[]{patientIdColumn, patientStruct});
    Row patientRow1 = RowFactory
        .create("Patient/abc1", RowFactory.create("Patient/abc1", "female", true));
    Row patientRow2 = RowFactory
        .create("Patient/abc2", RowFactory.create("Patient/abc2", "female", false));
    Row patientRow3 = RowFactory
        .create("Patient/abc3", RowFactory.create("Patient/abc3", "male", true));
    Dataset<Row> patientDataset = spark
        .createDataFrame(Arrays.asList(patientRow1, patientRow2, patientRow3), patientRecord);
    Column patientIdCol = patientDataset.col("id");
    Column patientValueCol = patientDataset.col("resource");
    when(mockReader.read(ResourceType.PATIENT))
        .thenReturn(patientDataset);

    // Prepare the inputs to the function.
    ExpressionParserContext parserContext = new ExpressionParserContext();
    parserContext.setResourceReader(mockReader);

    // Prepare the input expression.
    ParsedExpression inputExpression = new ParsedExpression();
    inputExpression.setFhirPath("resolve()");
    inputExpression.setSingular(true);
    inputExpression.setPolymorphic(true);
    inputExpression.setDataset(inputDataset);
    inputExpression.setIdColumn(idColumn);
    inputExpression.setValueColumn(valueColumn);
    inputExpression.setResourceTypeColumn(typeColumn);

    // Prepare the argument expression.
    ParsedExpression argumentExpression = new ParsedExpression();
    argumentExpression.setFhirPath("Patient");
    argumentExpression.setSingular(true);
    argumentExpression.setDataset(patientDataset);
    argumentExpression.setResource(true);
    argumentExpression.setResourceType(ResourceType.PATIENT);
    argumentExpression.setIdColumn(patientIdCol);
    argumentExpression.setValueColumn(patientValueCol);

    FunctionInput ofTypeInput = new FunctionInput();
    ofTypeInput.setExpression("ofType(Patient)");
    ofTypeInput.setContext(parserContext);
    ofTypeInput.setInput(inputExpression);
    ofTypeInput.getArguments().add(argumentExpression);

    // Invoke the function.
    OfTypeFunction ofTypeFunction = new OfTypeFunction();
    ParsedExpression result = ofTypeFunction.invoke(ofTypeInput);

    // Check the result dataset.
    Dataset<Row> resultDataset = result.getDataset()
        .select(result.getIdColumn(), result.getValueColumn());
    List<Row> resultRows = resultDataset.collectAsList();
    assertThat(resultRows.size()).isEqualTo(5);
    Row resultRow = resultRows.get(0);
    assertThat(resultRow.getString(0)).isEqualTo("Encounter/xyz1");
    assertThat(resultRow.getStruct(1).get(0)).isEqualTo("Patient/abc1");
    assertThat(resultRow.getStruct(1).get(1)).isEqualTo("female");
    resultRow = resultRows.get(1);
    assertThat(resultRow.getString(0)).isEqualTo("Encounter/xyz2");
    assertThat(resultRow.getStruct(1).get(0)).isEqualTo("Patient/abc2");
    assertThat(resultRow.getStruct(1).get(1)).isEqualTo("female");
    resultRow = resultRows.get(2);
    assertThat(resultRow.getString(0)).isEqualTo("Encounter/xyz3");
    assertThat(resultRow.getStruct(1).get(0)).isEqualTo("Patient/abc2");
    assertThat(resultRow.getStruct(1).get(1)).isEqualTo("female");
    resultRow = resultRows.get(3);
    assertThat(resultRow.getString(0)).isEqualTo("Encounter/xyz4");
    assertThat(resultRow.getStruct(1).get(0)).isEqualTo("Patient/abc2");
    assertThat(resultRow.getStruct(1).get(1)).isEqualTo("female");
    resultRow = resultRows.get(4);
    assertThat(resultRow.getString(0)).isEqualTo("Encounter/xyz5");
    assertThat(resultRow.getStruct(1)).isNull();
  }
}