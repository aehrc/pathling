/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.clinsight.TestUtilities;
import au.csiro.clinsight.fhir.TerminologyClient;
import au.csiro.clinsight.query.QueryRequest.Aggregation;
import au.csiro.clinsight.query.QueryRequest.Grouping;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Parameters;
import org.json.JSONException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.skyscreamer.jsonassert.JSONAssert;

/**
 * @author John Grimes
 */
public class QueryExecutorTest {

  private QueryExecutor executor;
  private SparkSession spark;
  private TerminologyClient terminologyClient;
  private IParser jsonParser;
  private Path warehouseDirectory;
  private ResourceReader mockReader;

  @Before
  public void setUp() throws IOException {
    spark = SparkSession.builder()
        .appName("clinsight-test")
        .config("spark.master", "local")
        .getOrCreate();
    terminologyClient = mock(TerminologyClient.class);
    jsonParser = FhirContext.forR4().newJsonParser();

    warehouseDirectory = Files.createTempDirectory("clinsight-test-");
    mockReader = mock(ResourceReader.class);
  }

  @Test
  public void queryWithMultipleGroupings() throws IOException, JSONException {
    // Load the subject resource dataset from a test Parquet file, and use it to mock out the
    // ResourceReader.
    URL parquetUrl = Thread.currentThread().getContextClassLoader()
        .getResource("test-data/parquet/Encounter.parquet");
    assertThat(parquetUrl).isNotNull();
    Dataset<Row> dataset = spark.read().parquet(parquetUrl.toString());
    when(mockReader.read("http://hl7.org/fhir/StructureDefinition/Encounter"))
        .thenReturn(dataset);

    // Create and configure a new QueryExecutor.
    QueryExecutorConfiguration config = new QueryExecutorConfiguration(spark, terminologyClient,
        mockReader);
    config.setWarehouseUrl(warehouseDirectory.toString());
    config.setDatabaseName("test");

    TestUtilities.mockDefinitionRetrieval(terminologyClient);
    executor = new QueryExecutor(config);

    // Build a QueryRequest to pass to the executor.
    QueryRequest request = new QueryRequest();
    request.setSubjectResource("http://hl7.org/fhir/StructureDefinition/Encounter");

    Aggregation aggregation = new Aggregation();
    aggregation.setLabel("Number of encounters");
    aggregation.setExpression("%resource.count()");
    request.getAggregations().add(aggregation);

    Grouping grouping1 = new Grouping();
    grouping1.setLabel("Class");
    grouping1.setExpression("%resource.class.code");
    request.getGroupings().add(grouping1);

    Grouping grouping2 = new Grouping();
    grouping2.setLabel("Reason");
    grouping2.setExpression("%resource.reasonCode.coding.display");
    request.getGroupings().add(grouping2);

    // Execute the query.
    QueryResponse response = executor.execute(request);

    // Check the response against an expected response.
    Parameters responseParameters = response.toParameters();
    String actualJson = jsonParser.encodeResourceToString(responseParameters);
    InputStream expectedStream = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream(
            "responses/QueryExecutorTest-queryWithMultipleGroupings.Parameters.json");
    assertThat(expectedStream).isNotNull();
    StringWriter writer = new StringWriter();
    IOUtils.copy(expectedStream, writer, UTF_8);
    String expectedJson = writer.toString();
    JSONAssert.assertEquals(expectedJson, actualJson, false);
  }

  @Test
  public void queryWithFilter() throws IOException, JSONException {
    // Load the subject resource dataset from a test Parquet file, and use it to mock out the
    // ResourceReader.
    URL parquetUrl = Thread.currentThread().getContextClassLoader()
        .getResource("test-data/parquet/Patient.parquet");
    assertThat(parquetUrl).isNotNull();
    Dataset<Row> dataset = spark.read().parquet(parquetUrl.toString());
    when(mockReader.read("http://hl7.org/fhir/StructureDefinition/Patient"))
        .thenReturn(dataset);

    // Create and configure a new QueryExecutor.
    QueryExecutorConfiguration config = new QueryExecutorConfiguration(spark, terminologyClient,
        mockReader);
    config.setWarehouseUrl(warehouseDirectory.toString());
    config.setDatabaseName("test");

    TestUtilities.mockDefinitionRetrieval(terminologyClient);
    executor = new QueryExecutor(config);

    // Build a QueryRequest to pass to the executor.
    QueryRequest request = new QueryRequest();
    request.setSubjectResource("http://hl7.org/fhir/StructureDefinition/Patient");

    Aggregation aggregation = new Aggregation();
    aggregation.setLabel("Number of patients");
    aggregation.setExpression("%resource.count()");
    request.getAggregations().add(aggregation);

    Grouping grouping1 = new Grouping();
    grouping1.setLabel("Gender");
    grouping1.setExpression("%resource.gender");
    request.getGroupings().add(grouping1);

    request.getFilters().add("%resource.gender = 'female'");

    // Execute the query.
    QueryResponse response = executor.execute(request);

    // Check the response against an expected response.
    Parameters responseParameters = response.toParameters();
    String actualJson = jsonParser.encodeResourceToString(responseParameters);
    InputStream expectedStream = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream("responses/QueryExecutorTest-queryWithFilter.Parameters.json");
    assertThat(expectedStream).isNotNull();
    StringWriter writer = new StringWriter();
    IOUtils.copy(expectedStream, writer, UTF_8);
    String expectedJson = writer.toString();
    JSONAssert.assertEquals(expectedJson, actualJson, false);
  }

  @After
  public void tearDown() {
    spark.close();
  }
}
