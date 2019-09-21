/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import au.csiro.clinsight.fhir.TerminologyClient;
import au.csiro.clinsight.query.QueryExecutor;
import au.csiro.clinsight.query.QueryExecutorConfiguration;
import au.csiro.clinsight.query.QueryRequest;
import au.csiro.clinsight.query.QueryRequest.Aggregation;
import au.csiro.clinsight.query.QueryRequest.Grouping;
import au.csiro.clinsight.query.QueryResponse;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author John Grimes
 */
public class QueryExecutorTest {

  private QueryExecutor executor;
  private SparkSession spark;

  @Before
  public void setUp() throws IOException {
    spark = SparkSession.builder()
        .appName("clinsight-test")
        .config("spark.master", "local")
        .getOrCreate();
    TerminologyClient terminologyClient = mock(TerminologyClient.class);
    Path warehouseDirectory = Files.createTempDirectory("clinsight-test");

    QueryExecutorConfiguration config = new QueryExecutorConfiguration(spark, terminologyClient);
    config.setWarehouseUrl(warehouseDirectory.toString());
    config.setDatabaseName("test");

    TestUtilities.mockDefinitionRetrieval(terminologyClient);
    executor = new QueryExecutor(config);
  }

  @Test
  public void simpleQuery() {
    QueryRequest request = new QueryRequest();
    request.setSubjectResource("http://hl7.org/fhir/StructureDefinition/AllergyIntolerance");

    Aggregation aggregation = new Aggregation();
    aggregation.setLabel("Number of allergies");
    aggregation.setExpression("%resource.count()");
    request.getAggregations().add(aggregation);

    Grouping grouping = new Grouping();
    grouping.setLabel("Category");
    grouping.setExpression("%resource.category");
    request.getGroupings().add(grouping);

    QueryResponse response = executor.execute(request);

    assertThat(response.getGroupings().size()).isEqualTo(1);
  }

  @After
  public void tearDown() {
    spark.close();
  }
}
