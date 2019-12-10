/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.parsing;

import static au.csiro.clinsight.test.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.HumanName;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import au.csiro.clinsight.TestUtilities;
import au.csiro.clinsight.fhir.FreshFhirContextFactory;
import au.csiro.clinsight.fhir.TerminologyClient;
import au.csiro.clinsight.query.ResourceReader;
import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import ca.uhn.fhir.context.RuntimeCompositeDatatypeDefinition;


/**
 * @author Piotr Szul
 */
@Category(au.csiro.clinsight.UnitTest.class)
public class ExpressionParserTest {

	private SparkSession spark;
	private ResourceReader mockReader;
	private TerminologyClient terminologyClient;
	private String terminologyServiceUrl = "https://r4.ontoserver.csiro.au/fhir";
	private ExpressionParserContext parserContext;
	private ExpressionParser expressionParser;

	@Before
	public void setUp() throws IOException {
		spark = SparkSession.builder().appName("clinsight-test").config("spark.master", "local")
		    .config("spark.driver.host", "localhost").config("spark.sql.shuffle.partitions", "1").getOrCreate();

		terminologyClient = mock(TerminologyClient.class, Mockito.withSettings().serializable());
		when(terminologyClient.getServerBase()).thenReturn(terminologyServiceUrl);

		mockReader = mock(ResourceReader.class);

		// Gather dependencies for the execution of the expression parser.
		parserContext = new ExpressionParserContext();
		parserContext.setFhirContext(TestUtilities.getFhirContext());
		parserContext.setFhirContextFactory(new FreshFhirContextFactory());
		parserContext.setTerminologyClient(terminologyClient);
		parserContext.setSparkSession(spark);
		parserContext.setResourceReader(mockReader);
		parserContext.setSubjectContext(null);
		expressionParser = new ExpressionParser(parserContext);
	}

	@Test
	public void testParseSomeExpression() {

		mockResourceReader(ResourceType.PATIENT);

		ResourceType resourceType = ResourceType.PATIENT;
		Dataset<Row> subject = mockReader.read(resourceType);
		String firstColumn = subject.columns()[0];
		String[] remainingColumns = Arrays.copyOfRange(subject.columns(), 1, subject.columns().length);
		Column idColumn = subject.col("id");
		subject = subject.withColumn("resource", org.apache.spark.sql.functions.struct(firstColumn, remainingColumns));
		Column valueColumn = subject.col("resource");
		subject = subject.select(idColumn, valueColumn);

		BaseRuntimeElementDefinition<?> def = parserContext.getFhirContext().getElementDefinition(HumanName.class);
		System.out.println(def);
		BaseRuntimeChildDefinition childDef = ((RuntimeCompositeDatatypeDefinition) def).getChildByName("family");
		System.out.println(childDef);

		// Build up an input for the function.
		ParsedExpression subjectResource = new ParsedExpression();
		subjectResource.setFhirPath("%resource");
		subjectResource.setResource(true);
		subjectResource.setResourceType(ResourceType.PATIENT);
		subjectResource.setOrigin(subjectResource);
		subjectResource.setDataset(subject);
		subjectResource.setIdColumn(idColumn);
		subjectResource.setSingular(true);
		subjectResource.setValueColumn(valueColumn);

		parserContext.setSubjectContext(subjectResource);
		ParsedExpression result = expressionParser.parse("name.given.first()");

		assertThat(result).aggByIdResult().debugSchema().debugRows();
	}

	private void mockResourceReader(ResourceType... resourceTypes) {
		for (ResourceType resourceType : resourceTypes) {
			URL parquetUrl = Thread.currentThread().getContextClassLoader()
			    .getResource("test-data/parquet/" + resourceType + ".parquet");
			assertThat(parquetUrl).isNotNull();
			Dataset<Row> dataset = spark.read().parquet(parquetUrl.toString());
			when(mockReader.read(resourceType)).thenReturn(dataset);
			when(mockReader.getAvailableResourceTypes()).thenReturn(new HashSet<>(Arrays.asList(resourceTypes)));
		}
	}

	@After
	public void tearDown() {
		spark.close();
	}
}
