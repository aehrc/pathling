package au.csiro.clinsight.query.functions;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.HumanName;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import au.csiro.clinsight.TestUtilities;
import au.csiro.clinsight.query.parsing.ParsedExpression;
import au.csiro.clinsight.query.parsing.ParsedExpression.FhirPathType;
import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import ca.uhn.fhir.context.RuntimeCompositeDatatypeDefinition;

/**
 * @author Piotr Szul
 */
@Category(au.csiro.clinsight.UnitTest.class)
public class FirstFunctionTest {

	private SparkSession spark;

	@Before
	public void setUp() {
		spark = SparkSession.builder().appName("clinsight-test").config("spark.master", "local")
		    .config("spark.driver.host", "localhost").getOrCreate();
	}

	@Ignore
	@Test
	public void testReturnFirstFromMultiResrouceList() {
		// Build a Dataset with several rows in it.
		Metadata metadata = new MetadataBuilder().build();
		StructField genderColumn = new StructField("gender", DataTypes.StringType, true, metadata);
		StructField activeColumn = new StructField("active", DataTypes.BooleanType, true, metadata);
		StructType resourceStruct = new StructType(new StructField[] { genderColumn, activeColumn });
		StructField id = new StructField("123abcd_id", DataTypes.StringType, false, metadata);
		StructField resource = new StructField("123abcd", resourceStruct, false, metadata);
		StructType rowStruct = new StructType(new StructField[] { id, resource });

		Row row1 = RowFactory.create("abc1", RowFactory.create("female", true));
		Row row2 = RowFactory.create("abc2", RowFactory.create("female", false));
		Row row3 = RowFactory.create("abc3", RowFactory.create("male", true));
		Dataset<Row> dataset = spark.createDataFrame(Arrays.asList(row1, row2, row3), rowStruct);
		Column idColumn = dataset.col(dataset.columns()[0]);
		Column valueColumn = dataset.col(dataset.columns()[1]);

		// Build up an input for the function.
		ParsedExpression input = new ParsedExpression();
		input.setFhirPath("%resource");
		input.setResource(true);
		input.setResourceType(ResourceType.PATIENT);
		input.setOrigin(input);
		input.setDataset(dataset);
		input.setIdColumn(idColumn);
		input.setValueColumn(valueColumn);

		FunctionInput firstInput = new FunctionInput();
		firstInput.setInput(input);
		firstInput.setExpression("first()");

		// Execute the count function.
		FirstFunction firstFunction = new FirstFunction();
		ParsedExpression result = firstFunction.invoke(firstInput);

		// Check the result.
		// these should be passed throug
		assertThat(result.getFhirPathType()).isEqualTo(input.getFhirPathType());
		assertThat(result.getFhirType()).isEqualTo(input.getFhirType());
		assertThat(result.getDefinition()).isEqualTo(input.getDefinition());
		assertThat(result.getElementDefinition()).isEqualTo(input.getElementDefinition());
		assertThat(result.isPrimitive()).isFalse();
		assertThat(result.isResource()).isTrue();
		assertThat(result.getResourceType()).isEqualTo(input.getResourceType());

		// these should be set to new values
		assertThat(result.getFhirPath()).isEqualTo("first()");
		assertThat(result.isSingular()).isTrue();
		assertThat(result.getIdColumn()).isNotNull();
		assertThat(result.getValueColumn()).isNotNull();
		assertThat(result.getAggregationColumn()).isNull();

		// Check that running the aggregation against the Dataset results in the correct
		// count.
		Dataset<Row> resultDataset = result.getDataset();
		List<Row> resultRows = resultDataset.select(result.getIdColumn(), result.getValueColumn()).collectAsList();
		assertThat(resultRows).isEqualTo(Arrays.asList(row1, row2, row3));
	}

	@Test
	public void testReturnsFirstFromMultiElementList() {
		// Build a Dataset with several rows in it.
		Metadata metadata = new MetadataBuilder().build();
		StructField id = new StructField("123abcd_id", DataTypes.StringType, false, metadata);
		StructField value = new StructField("123abcd", DataTypes.StringType, true, metadata);
		StructType rowStruct = new StructType(new StructField[] { id, value });

		// Multiple values against the same resource should be counted individually.
		Row row1 = RowFactory.create("abc1", "Jude");
		Row row2 = RowFactory.create("abc1", "Samuel");
		Row row3 = RowFactory.create("abc2", "Thomas");
		Row row4 = RowFactory.create("abc3", null);
		Row row5 = RowFactory.create("abc3", "Adam");
		Dataset<Row> dataset = spark.createDataFrame(Arrays.asList(row1, row2, row3, row4, row5), rowStruct);

		Column idColumn = dataset.col(dataset.columns()[0]);
		Column valueColumn = dataset.col(dataset.columns()[1]);

		BaseRuntimeElementDefinition<?> def = TestUtilities.getFhirContext().getElementDefinition(HumanName.class);
		BaseRuntimeChildDefinition childDef = ((RuntimeCompositeDatatypeDefinition) def).getChildByName("family");

		// Build up an input for the function.
		ParsedExpression input = new ParsedExpression();
		input.setFhirPath("name.family");
		input.setFhirPathType(FhirPathType.STRING);
		input.setFhirType(FHIRDefinedType.STRING);
		input.setPrimitive(true);
		input.setSingular(false);
		input.setOrigin(null);
		input.setPolymorphic(false);
		input.setResource(false);
		input.setDefinition(childDef, "family");
		input.setDataset(dataset);
		input.setIdColumn(idColumn);
		input.setValueColumn(valueColumn);

		FunctionInput firstInput = new FunctionInput();
		firstInput.setInput(input);
		firstInput.setExpression("name.family.first()");

		// Execute the count function.
		FirstFunction firstFunction = new FirstFunction();
		ParsedExpression result = firstFunction.invoke(firstInput);

		// Check the result.
		// these should be passed throug
		assertThat(result.getFhirPathType()).isEqualTo(input.getFhirPathType());
		assertThat(result.getFhirType()).isEqualTo(input.getFhirType());
		assertThat(result.getDefinition()).isEqualTo(input.getDefinition());
		assertThat(result.getElementDefinition()).isEqualTo(input.getElementDefinition());
		assertThat(result.isPrimitive()).isTrue();
		// these should be set to new values
		assertThat(result.getFhirPath()).isEqualTo("name.family.first()");
		assertThat(result.isSingular()).isTrue();
		assertThat(result.getIdColumn()).isNotNull();
		assertThat(result.getValueColumn()).isNotNull();
		assertThat(result.getAggregationColumn()).isNull();

		// Check that running the aggregation against the Dataset results in the correct
		// count.
		Dataset<Row> resultDataset = result.getDataset();
		List<Row> resultRows = resultDataset.select(result.getIdColumn(), result.getValueColumn())
		    .orderBy(result.getIdColumn()).collectAsList();
		assertThat(resultRows).isEqualTo(Arrays.asList(row1, row3, row4));
	}

	@Test
	public void testReturnsEmptyFromEmptyList() {
		// Build a Dataset with several rows in it.
		Metadata metadata = new MetadataBuilder().build();
		StructField id = new StructField("123abcd_id", DataTypes.StringType, false, metadata);
		StructField value = new StructField("123abcd", DataTypes.StringType, true, metadata);
		StructType rowStruct = new StructType(new StructField[] { id, value });

		// Multiple values against the same resource should be counted individually.
		Dataset<Row> dataset = spark.createDataFrame(Arrays.asList(), rowStruct);

		Column idColumn = dataset.col(dataset.columns()[0]);
		Column valueColumn = dataset.col(dataset.columns()[1]);

		BaseRuntimeElementDefinition<?> def = TestUtilities.getFhirContext().getElementDefinition(HumanName.class);
		BaseRuntimeChildDefinition childDef = ((RuntimeCompositeDatatypeDefinition) def).getChildByName("family");

		// Build up an input for the function.
		ParsedExpression input = new ParsedExpression();
		input.setFhirPath("name.family");
		input.setFhirPathType(FhirPathType.STRING);
		input.setFhirType(FHIRDefinedType.STRING);
		input.setPrimitive(true);
		input.setSingular(false);
		input.setOrigin(null);
		input.setPolymorphic(false);
		input.setResource(false);
		input.setDefinition(childDef, "family");
		input.setDataset(dataset);
		input.setIdColumn(idColumn);
		input.setValueColumn(valueColumn);

		FunctionInput firstInput = new FunctionInput();
		firstInput.setInput(input);
		firstInput.setExpression("name.family.first()");

		// Execute the count function.
		FirstFunction firstFunction = new FirstFunction();
		ParsedExpression result = firstFunction.invoke(firstInput);

		// Check the result.
		// these should be passed throug
		assertThat(result.getFhirPathType()).isEqualTo(input.getFhirPathType());
		assertThat(result.getFhirType()).isEqualTo(input.getFhirType());
		assertThat(result.getDefinition()).isEqualTo(input.getDefinition());
		assertThat(result.getElementDefinition()).isEqualTo(input.getElementDefinition());
		assertThat(result.isPrimitive()).isTrue();
		// these should be set to new values
		assertThat(result.getFhirPath()).isEqualTo("name.family.first()");
		assertThat(result.isSingular()).isTrue();
		assertThat(result.getIdColumn()).isNotNull();
		assertThat(result.getValueColumn()).isNotNull();
		assertThat(result.getAggregationColumn()).isNull();

		// Check that running the aggregation against the Dataset results in the correct
		// count.
		Dataset<Row> resultDataset = result.getDataset();
		List<Row> resultRows = resultDataset.select(result.getIdColumn(), result.getValueColumn())
		    .orderBy(result.getIdColumn()).collectAsList();
		assertThat(resultRows).isEmpty();
	}
}