/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query;

import static au.csiro.clinsight.utilities.Preconditions.checkNotNull;

import au.csiro.clinsight.ExpansionResult;
import au.csiro.clinsight.TerminologyClient;
import au.csiro.clinsight.TerminologyClientConfiguration;
import au.csiro.clinsight.resources.AggregateQuery;
import au.csiro.clinsight.resources.AggregateQuery.AggregationComponent;
import au.csiro.clinsight.resources.AggregateQuery.GroupingComponent;
import au.csiro.clinsight.resources.AggregateQueryResult;
import au.csiro.clinsight.resources.AggregateQueryResult.DataComponent;
import au.csiro.clinsight.utilities.Configuration;
import ca.uhn.fhir.context.FhirContext;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    Configuration.copyStringProps(configuration, clientConfiguration,
        Collections.singletonList("terminologyServerUrl"));
    terminologyClient = new TerminologyClient(clientConfiguration, fhirContext);
  }

  @Override
  public AggregateQueryResult execute(AggregateQuery query) throws IllegalArgumentException {
    List<AggregationComponent> aggregations = query.getAggregation();
    List<GroupingComponent> groupings = query.getGrouping();
    if (aggregations == null || aggregations.isEmpty()) {
      throw new IllegalArgumentException("Missing aggregation component within query");
    }

    List<SparkAggregationParseResult> aggregationParseResults = parseAggregation(aggregations);
    List<SparkGroupingParseResult> groupingParseResults = parseGroupings(groupings);
    SparkQueryPlan finalQueryPlan = buildQueryPlan(aggregationParseResults, groupingParseResults);

    String selectClause = "SELECT " + String.join(", ", finalQueryPlan.getAggregationClause());
    String fromClause = "FROM " + String.join(", ", finalQueryPlan.getFromTables());
    String groupByClause = "GROUP BY " + String.join(", ", finalQueryPlan.getGroupingClause());
    Dataset<Row> result = spark.sql(String.join(" ", selectClause, fromClause, groupByClause));

    return queryResultFromDataset(result, query, finalQueryPlan);
  }

  private List<SparkAggregationParseResult> parseAggregation(
      List<AggregationComponent> aggregations) {
    return aggregations.stream()
        .map(aggregation -> {
          // TODO: Support references to pre-defined aggregations.
          String aggExpression = aggregation.getExpression().asStringValue();
          if (aggExpression == null) {
            throw new IllegalArgumentException("Aggregation component must have expression");
          }
          SparkAggregationParser aggregationParser = new SparkAggregationParser();
          return aggregationParser.parse(aggExpression);
        }).collect(Collectors.toList());
  }

  private List<SparkGroupingParseResult> parseGroupings(List<GroupingComponent> groupings) {
    List<SparkGroupingParseResult> groupingParseResults = new ArrayList<>();
    if (groupings != null) {
      groupingParseResults = groupings.stream()
          .map(grouping -> {
            // TODO: Support references to pre-defined dimensions.
            String groupingExpression = grouping.getExpression().asStringValue();
            if (groupingExpression == null) {
              throw new IllegalArgumentException("Grouping component must have expression");
            }
            SparkGroupingParser groupingParser = new SparkGroupingParser();
            return groupingParser.parse(groupingExpression);
          }).collect(Collectors.toList());
    }
    return groupingParseResults;
  }

  private SparkQueryPlan buildQueryPlan(List<SparkAggregationParseResult> aggregationParseResults,
      List<SparkGroupingParseResult> groupingParseResults) {
    SparkQueryPlan finalQueryPlan = new SparkQueryPlan();
    List<String> aggregationClause = aggregationParseResults.stream()
        .map(SparkAggregationParseResult::getExpression)
        .collect(Collectors.toList());
    finalQueryPlan.setAggregationClause(aggregationClause);

    List<DataType> resultTypes = aggregationParseResults.stream()
        .map(SparkAggregationParseResult::getResultType)
        .collect(Collectors.toList());
    finalQueryPlan.setResultTypes(resultTypes);

    List<String> groupingClause = groupingParseResults.stream()
        .map(SparkGroupingParseResult::getExpression)
        .collect(Collectors.toList());
    finalQueryPlan.setGroupingClause(groupingClause);

    Set<String> aggregationFromTables = aggregationParseResults.stream()
        .map(SparkAggregationParseResult::getFromTable)
        .collect(Collectors.toSet());
    Set<String> groupingFromTables = groupingParseResults.stream()
        .map(SparkGroupingParseResult::getFromTable)
        .collect(Collectors.toSet());
    if (!aggregationFromTables.containsAll(groupingFromTables)) {
      Set<String> difference = new HashSet<>(groupingFromTables);
      difference.removeAll(aggregationFromTables);
      throw new IllegalArgumentException(
          "Groupings contain one or more resources that are not the subject of an aggregation: "
              + String.join(", ", difference));
    }
    finalQueryPlan.setFromTables(aggregationFromTables);
    return finalQueryPlan;
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
                .get(queryPlan.getResultTypes())
                .getConstructor(sparkDataTypeToJavaClass.get(queryPlan.getResultTypes()));
            Object value = row.get(0);
            return constructor.newInstance(value);
          } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
            logger.error("Failed to access value from row: " + e.getMessage());
            return null;
          }
        }).collect(Collectors.toList());
    data.setSeries(series);
    queryResult.setData(Collections.singletonList(data));
    data.setName(query.getAggregation().get(0).getLabel());
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
