/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.spark;

import static au.csiro.clinsight.query.spark.Mappings.getFhirClass;
import static au.csiro.clinsight.utilities.Preconditions.checkNotNull;
import static au.csiro.clinsight.utilities.Strings.backTicks;

import au.csiro.clinsight.TerminologyClient;
import au.csiro.clinsight.fhir.ResourceDefinitions;
import au.csiro.clinsight.query.QueryExecutor;
import au.csiro.clinsight.resources.AggregateQuery;
import au.csiro.clinsight.resources.AggregateQuery.AggregationComponent;
import au.csiro.clinsight.resources.AggregateQuery.GroupingComponent;
import au.csiro.clinsight.resources.AggregateQueryResult;
import au.csiro.clinsight.resources.AggregateQueryResult.LabelComponent;
import au.csiro.clinsight.resources.AggregateQueryResult.ResultComponent;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.dstu3.model.Reference;
import org.hl7.fhir.dstu3.model.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class knows how to take an AggregateQuery and execute it against an Apache Spark data
 * warehouse, returning the result as an AggregateQueryResult resource.
 *
 * @author John Grimes
 */
public class SparkQueryExecutor implements QueryExecutor {

  private static final Logger logger = LoggerFactory.getLogger(SparkQueryExecutor.class);

  private final SparkQueryExecutorConfiguration configuration;
  private final FhirContext fhirContext;
  private SparkSession spark;

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
    initialiseResourceDefinitions();
    spark.sql("USE clinsight");
  }

  private void initialiseResourceDefinitions() {
    TerminologyClient terminologyClient = fhirContext
        .newRestfulClient((TerminologyClient.class), configuration.getTerminologyServerUrl());
    ResourceDefinitions.ensureInitialized(terminologyClient);
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

  @Override
  public AggregateQueryResult execute(AggregateQuery query) throws InvalidRequestException {
    try {
      List<AggregationComponent> aggregations = query.getAggregation();
      List<GroupingComponent> groupings = query.getGrouping();
      if (aggregations == null || aggregations.isEmpty()) {
        throw new InvalidRequestException("Missing aggregation component within query");
      }

      List<ParseResult> aggregationParseResults = parseAggregation(aggregations);
      List<ParseResult> groupingParseResults = parseGroupings(groupings);
      QueryPlan queryPlan = buildQueryPlan(aggregationParseResults, groupingParseResults);

      Dataset<Row> result = executeQueryPlan(queryPlan, query);

      return queryResultFromDataset(result, query, queryPlan);
    } catch (BaseServerResponseException e) {
      throw e;
    } catch (Exception | AssertionError e) {
      logger.error("Exception occurred while executing query", e);
      throw new InternalErrorException("Unexpected error occurred while executing query");
    }
  }

  private List<ParseResult> parseAggregation(List<AggregationComponent> aggregations) {
    return aggregations.stream()
        .map(aggregation -> {
          // TODO: Support references to pre-defined aggregations.
          String aggExpression = aggregation.getExpression().asStringValue();
          if (aggExpression == null) {
            throw new InvalidRequestException("Aggregation component must have expression");
          }
          AggregationParser aggregationParser = new AggregationParser();
          return aggregationParser.parse(aggExpression);
        }).collect(Collectors.toList());
  }

  private List<ParseResult> parseGroupings(List<GroupingComponent> groupings) {
    List<ParseResult> groupingParseResults = new ArrayList<>();
    if (groupings != null) {
      groupingParseResults = groupings.stream()
          .map(grouping -> {
            // TODO: Support references to pre-defined dimensions.
            String groupingExpression = grouping.getExpression().asStringValue();
            if (groupingExpression == null) {
              throw new InvalidRequestException("Grouping component must have expression");
            }
            GroupingParser groupingParser = new GroupingParser();
            return groupingParser.parse(groupingExpression);
          }).collect(Collectors.toList());
    }
    return groupingParseResults;
  }

  private QueryPlan buildQueryPlan(List<ParseResult> aggregationParseResults,
      List<ParseResult> groupingParseResults) {
    QueryPlan queryPlan = new QueryPlan();

    // Get aggregation expressions from the parse results.
    List<String> aggregations = aggregationParseResults.stream()
        .map(ParseResult::getExpression)
        .collect(Collectors.toList());
    queryPlan.setAggregations(aggregations);

    // Get aggregation data types from the parse results.
    List<String> aggregationTypes = aggregationParseResults.stream()
        .map(ParseResult::getResultType)
        .collect(Collectors.toList());
    queryPlan.setAggregationTypes(aggregationTypes);

    // Get grouping expressions from the parse results.
    List<String> groupings = groupingParseResults.stream()
        .map(ParseResult::getExpression)
        .collect(Collectors.toList());
    queryPlan.setGroupings(groupings);

    // Get grouping data types from the parse results.
    List<String> groupingTypes = groupingParseResults.stream()
        .map(ParseResult::getResultType)
        .collect(Collectors.toList());
    queryPlan.setGroupingTypes(groupingTypes);

    // Get from tables from the results of parsing both aggregations and groupings, and compute the
    // union.
    Set<String> aggregationFromTables = aggregationParseResults.stream()
        .map(ParseResult::getFromTable)
        .collect(Collectors.toSet());
    Set<String> groupingFromTables = groupingParseResults.stream()
        .map(ParseResult::getFromTable)
        .collect(Collectors.toSet());
    // Check for from tables within the groupings that were not referenced within at least one
    // aggregation expression.
    if (!aggregationFromTables.containsAll(groupingFromTables)) {
      Set<String> difference = new HashSet<>(groupingFromTables);
      difference.removeAll(aggregationFromTables);
      throw new InvalidRequestException(
          "Groupings contain one or more resources that are not the subject of an aggregation: "
              + String.join(", ", difference));
    }
    queryPlan.setFromTables(aggregationFromTables);

    return queryPlan;
  }

  private Dataset<Row> executeQueryPlan(QueryPlan queryPlan, AggregateQuery query) {
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
   * AggregateQuery and honouring the hints within the QueryPlan.
   */
  private AggregateQueryResult queryResultFromDataset(Dataset<Row> dataset,
      AggregateQuery query, QueryPlan queryPlan) {
    List<Row> rows = dataset.collectAsList();
    int numGroupings = query.getGrouping().size();
    int numAggregations = query.getAggregation().size();

    AggregateQueryResult queryResult = new AggregateQueryResult();
    queryResult.setQuery(new Reference(query));

    List<AggregateQueryResult.GroupingComponent> groupings = rows.stream()
        .map(mapRowToGrouping(queryPlan, numGroupings, numAggregations))
        .collect(Collectors.toList());
    queryResult.setGrouping(groupings);

    return queryResult;
  }

  /**
   * Translate a Dataset Row into a grouping component for inclusion within an
   * AggregateQueryResult.
   */
  private Function<Row, AggregateQueryResult.GroupingComponent> mapRowToGrouping(
      QueryPlan queryPlan, int numGroupings,
      int numAggregations) {
    return row -> {
      AggregateQueryResult.GroupingComponent grouping = new AggregateQueryResult.GroupingComponent();

      List<LabelComponent> labels = new ArrayList<>();
      for (int i = 0; i < numGroupings; i++) {
        LabelComponent label = new LabelComponent();
        String fhirType = queryPlan.getGroupingTypes().get(i);
        Type value = getValueFromRow(row, i, fhirType);
        // Null values are represented as the absence of a value, as per the FHIR spec.
        if (value != null) {
          label.setValue(value);
        }
        labels.add(label);
      }
      grouping.setLabel(labels);

      List<ResultComponent> results = new ArrayList<>();
      for (int i = 0; i < numAggregations; i++) {
        ResultComponent result = new ResultComponent();
        String fhirType = queryPlan.getAggregationTypes().get(i);
        Type value = getValueFromRow(row, i + numGroupings, fhirType);
        // Null values are represented as the absence of a value, as per the FHIR spec.
        if (value != null) {
          result.setValue(value);
        }
        results.add(result);
      }
      grouping.setResult(results);

      return grouping;
    };
  }

  /**
   * Extract a value from the specified column within a row, using the supplied data type. Note that
   * this method may return null where the value is actually null within the Dataset.
   */
  private Type getValueFromRow(Row row, int columnNumber, String fhirType) {
    try {
      Class fhirClass = getFhirClass(fhirType);
      checkNotNull(fhirClass, "Unable to map FHIR type to FHIR class: " + fhirType);
      Class javaClass = Mappings.getJavaClass(fhirType);
      checkNotNull(javaClass, "Unable to map FHIR type to Java class: " + fhirType);
      @SuppressWarnings("unchecked") Constructor constructor = fhirClass.getConstructor(javaClass);
      Object value = row.get(columnNumber);
      return (Type) constructor.newInstance(value);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
      logger.error("Failed to access value from row: " + e.getMessage());
      return null;
    }
  }

}
