/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query;

import static au.csiro.clinsight.utilities.Preconditions.checkNotNull;
import static au.csiro.clinsight.utilities.Strings.backTicks;

import au.csiro.clinsight.ExpansionResult;
import au.csiro.clinsight.TerminologyClient;
import au.csiro.clinsight.TerminologyClientConfiguration;
import au.csiro.clinsight.resources.AggregateQuery;
import au.csiro.clinsight.resources.AggregateQuery.AggregationComponent;
import au.csiro.clinsight.resources.AggregateQuery.GroupingComponent;
import au.csiro.clinsight.resources.AggregateQueryResult;
import au.csiro.clinsight.resources.AggregateQueryResult.DataComponent;
import au.csiro.clinsight.resources.AggregateQueryResult.LabelComponent;
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
    SparkQueryPlan queryPlan = buildQueryPlan(aggregationParseResults, groupingParseResults);

    Dataset<Row> result = executeQueryPlan(queryPlan, query);

    return queryResultFromDataset(result, query, queryPlan);
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
    SparkQueryPlan queryPlan = new SparkQueryPlan();
    List<String> aggregations = aggregationParseResults.stream()
        .map(SparkAggregationParseResult::getExpression)
        .collect(Collectors.toList());
    queryPlan.setAggregations(aggregations);

    List<DataType> resultTypes = aggregationParseResults.stream()
        .map(SparkAggregationParseResult::getResultType)
        .collect(Collectors.toList());
    queryPlan.setResultTypes(resultTypes);

    List<String> groupings = groupingParseResults.stream()
        .map(SparkGroupingParseResult::getExpression)
        .collect(Collectors.toList());
    queryPlan.setGroupings(groupings);

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
    queryPlan.setFromTables(aggregationFromTables);
    return queryPlan;
  }

  private Dataset<Row> executeQueryPlan(SparkQueryPlan queryPlan, AggregateQuery query) {
    List<String> selectExpressions = new ArrayList<>();
    if (queryPlan.getGroupings() != null && !queryPlan.getGroupings().isEmpty()) {
      for (int i = 0; i < queryPlan.getGroupings().size(); i++) {
        String grouping = queryPlan.getGroupings().get(i);
        String label = backTicks(query.getGrouping().get(i).getLabel().asStringValue());
        selectExpressions.add(grouping + " AS " + label);
      }
    }
    if (queryPlan.getAggregations() != null && !queryPlan.getAggregations().isEmpty()) {
      for (int i = 0; i < queryPlan.getAggregations().size(); i++) {
        String aggregation = queryPlan.getAggregations().get(i);
        String label = backTicks(query.getAggregation().get(i).getLabel().asStringValue());
        selectExpressions.add(aggregation + " AS " + label);
      }
    }
    String selectClause = "SELECT " + String.join(", ", selectExpressions);
    String fromClause = "FROM " + String.join(", ", queryPlan.getFromTables());
    String groupByClause = "GROUP BY " + String.join(", ", queryPlan.getGroupings());
    String orderByClause = "ORDER BY " + String.join(", ", queryPlan.getGroupings());
    String sql = String.join(" ", selectClause, fromClause, groupByClause, orderByClause);

    logger.info("Executing query: " + sql);
    return spark.sql(sql);
  }

  /**
   * Build an AggregateQueryResult resource from the supplied Dataset, embedding the original
   * AggregateQuery and honouring the hints within the SparkQueryPlan.
   */
  private static AggregateQueryResult queryResultFromDataset(Dataset<Row> dataset,
      AggregateQuery query, SparkQueryPlan queryPlan) {
    List<Row> rows = dataset.collectAsList();
    int numGroupings = query.getGrouping().size();
    int numAggregations = query.getAggregation().size();

    AggregateQueryResult queryResult = new AggregateQueryResult();
    queryResult.setQuery(new Reference(query));

    List<LabelComponent> labelComponents = new ArrayList<>();
    for (int i = 0; i < numGroupings; i++) {
      LabelComponent labelComponent = new LabelComponent();
      StringType label = query.getGrouping().get(i).getLabel();
      labelComponent.setName(label);
      int columnNumber = i;
      List<StringType> series = rows.stream()
          .map(row -> new StringType(row.getString(columnNumber)))
          .collect(Collectors.toList());
      labelComponent.setSeries(series);
      labelComponents.add(labelComponent);
    }
    queryResult.setLabel(labelComponents);

    List<DataComponent> dataComponents = new ArrayList<>();
    for (int i = 0; i < numAggregations; i++) {
      DataComponent dataComponent = new DataComponent();
      StringType label = query.getAggregation().get(i).getLabel();
      dataComponent.setName(label);
      int aggregationNumber = i;
      int columnNumber = i + numGroupings;
      @SuppressWarnings("unchecked") List<Type> series = (List<Type>) (Object) rows.stream()
          .map(row -> {
            try {
              @SuppressWarnings("unchecked") Constructor constructor = sparkDataTypeToFhirClass
                  .get(queryPlan.getResultTypes().get(aggregationNumber))
                  .getConstructor(sparkDataTypeToJavaClass.get(queryPlan.getResultTypes().get(
                      aggregationNumber)));
              Object value = row.get(columnNumber);
              return constructor.newInstance(value);
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
              logger.error("Failed to access value from row: " + e.getMessage());
              return null;
            }
          }).collect(Collectors.toList());
      dataComponent.setSeries(series);
      dataComponents.add(dataComponent);
    }
    queryResult.setData(dataComponents);

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
