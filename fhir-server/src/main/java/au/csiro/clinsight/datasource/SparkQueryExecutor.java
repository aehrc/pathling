/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.datasource;

import static au.csiro.clinsight.utilities.Preconditions.checkNotNull;

import au.csiro.clinsight.ExpansionResult;
import au.csiro.clinsight.TerminologyClient;
import au.csiro.clinsight.TerminologyClientConfiguration;
import au.csiro.clinsight.resources.AggregateQuery;
import au.csiro.clinsight.resources.AggregateQuery.AggregationComponent;
import au.csiro.clinsight.resources.AggregateQueryResult;
import au.csiro.clinsight.resources.AggregateQueryResult.DataComponent;
import au.csiro.clinsight.utilities.Configuration;
import ca.uhn.fhir.context.FhirContext;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.dstu3.model.IntegerType;
import org.hl7.fhir.dstu3.model.Reference;
import org.hl7.fhir.dstu3.model.StringType;
import org.hl7.fhir.dstu3.model.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author John Grimes
 */
public class SparkQueryExecutor implements QueryExecutor {

  private static final Logger logger = LoggerFactory.getLogger(SparkQueryExecutor.class);
  private static final Map<DataType, Class> sparkDataTypeToJavaClass = new HashMap<DataType, Class>() {{
    put(DataTypes.LongType, Long.class);
  }};
  private static final Map<DataType, Class> sparkDataTypeToFhirClass = new HashMap<DataType, Class>() {{
    put(DataTypes.LongType, IntegerType.class);
  }};

  private SparkQueryExecutorConfiguration configuration;
  private FhirContext fhirContext;
  private SparkSession spark;
  private TerminologyClient terminologyClient;

  public SparkQueryExecutor(SparkQueryExecutorConfiguration configuration,
      FhirContext fhirContext) {
    checkNotNull(configuration, "Must supply configuration");
    checkNotNull(configuration.getSparkMasterUrl(), "Must supply Spark master URL");
    checkNotNull(configuration.getWarehouseDirectory(), "Must supply warehouse directory");
    checkNotNull(configuration.getMetastoreUrl(), "Must supply metastore connection URL");
    checkNotNull(configuration.getMetastoreUser(), "Must supply metastore user");
    checkNotNull(configuration.getMetastorePassword(), "Must supply metastore password");
    checkNotNull(configuration.getExecutorMemory(), "Must supply executor memory");

    logger.info("Creating new SparkQueryExecutor: " + configuration);
    this.configuration = configuration;
    this.fhirContext = fhirContext;
    initialiseSpark();
    initialiseTerminologyClient();
    spark.sql("USE clinsight");
  }

  private void initialiseSpark() {
    spark = SparkSession.builder()
        .config("spark.master", configuration.getSparkMasterUrl())
        // TODO: Use Maven dependency plugin to copy this into a relative location.
        .config("spark.jars",
            "/Users/gri306/Code/contrib/bunsen/bunsen-shaded/target/bunsen-shaded-0.4.6-SNAPSHOT.jar")
        .config("spark.sql.warehouse.dir", configuration.getWarehouseDirectory())
        .config("javax.jdo.option.ConnectionURL", configuration.getMetastoreUrl())
        .config("javax.jdo.option.ConnectionUserName", configuration.getMetastoreUser())
        .config("javax.jdo.option.ConnectionPassword", configuration.getMetastorePassword())
        .config("spark.executor.memory", configuration.getExecutorMemory())
        .enableHiveSupport()
        .getOrCreate();
  }


  private void initialiseTerminologyClient() {
    TerminologyClientConfiguration clientConfiguration = new TerminologyClientConfiguration();
    Configuration
        .copyStringProps(configuration, clientConfiguration, Arrays.asList("terminologyServerUrl"));
    terminologyClient = new TerminologyClient(clientConfiguration, fhirContext);
  }

  @Override
  public AggregateQueryResult execute(AggregateQuery query) throws IllegalArgumentException {
    List<AggregationComponent> aggregations = query.getAggregation();
    if (aggregations == null || aggregations.isEmpty()) {
      throw new IllegalArgumentException("Missing aggregation component within query");
    }
    if (aggregations.size() > 1) {
      throw new AssertionError("More than one aggregation not yet supported");
    }
    AggregationComponent aggregation = aggregations.get(0);
    String aggExpression = aggregation.getExpression().asStringValue();
    if (aggExpression == null) {
      throw new IllegalArgumentException("Aggregation component must have expression");
    } else {
      AggregationExpressionParser parser = new AggregationExpressionParser();
      SparkQueryPlan queryPlan = parser.parse(aggExpression);
      Dataset<Row> result = spark.sql("SELECT " + queryPlan.getAggregationClause()
          + " FROM " + queryPlan.getFromTables().stream().collect(Collectors.joining(", ")));
      return queryResultFromDataset(result, query, queryPlan);
    }
  }

  /**
   * Build an AggregateQueryResult resource from the supplied Dataset, embedding the original
   * AggregateQuery and honouring the hints within the SparkQueryPlan.
   */
  private static AggregateQueryResult queryResultFromDataset(Dataset<Row> dataset,
      AggregateQuery query, SparkQueryPlan queryPlan) {
    AggregateQueryResult queryResult = new AggregateQueryResult();
    queryResult.setQuery(new Reference(query));
    DataComponent data = new DataComponent();
    if (dataset.columns().length > 1) {
      throw new AssertionError("More than one column in result not yet supported");
    }
    data.setName(new StringType(dataset.columns()[0]));
    @SuppressWarnings("unchecked") List<Type> series = (List<Type>) (Object) dataset.collectAsList()
        .stream()
        .map(row -> {
          try {
            @SuppressWarnings("unchecked") Constructor constructor = sparkDataTypeToFhirClass
                .get(queryPlan.getResultType())
                .getConstructor(sparkDataTypeToJavaClass.get(queryPlan.getResultType()));
            Object value = row.get(0);
            return constructor.newInstance(value);
          } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
            logger.error("Failed to access value from row: " + e.getMessage());
            return null;
          }
        }).collect(Collectors.toList());
    data.setSeries(series);
    queryResult.setData(Arrays.asList(data));
    return queryResult;
  }

  /**
   * Create a Spark DataSet and temporary view from the results of expanding an ECL query using the
   * terminology server.
   */
  private static void createViewFromEclExpansion(SparkSession spark,
      TerminologyClient terminologyClient, String ecl, String viewName) {
    List<ExpansionResult> results = terminologyClient
        .expand("http://snomed.info/sct?fhir_vs=ecl/(" + ecl + ")");
    Dataset<ExpansionResult> dataset = spark
        .createDataset(results, Encoders.bean(ExpansionResult.class));
    dataset.createOrReplaceTempView(viewName);
    dataset.persist();
    dataset.show();
  }

}
