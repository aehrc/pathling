/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.spark;

import static au.csiro.clinsight.query.spark.Mappings.getFhirClass;
import static au.csiro.clinsight.utilities.Strings.backTicks;

import au.csiro.clinsight.TerminologyClient;
import au.csiro.clinsight.fhir.ResourceDefinitions;
import au.csiro.clinsight.query.AggregateQuery;
import au.csiro.clinsight.query.AggregateQueryResult;
import au.csiro.clinsight.query.AggregateQueryResult.Grouping;
import au.csiro.clinsight.query.QueryExecutor;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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
  private TerminologyClient terminologyClient;

  public SparkQueryExecutor(SparkQueryExecutorConfiguration configuration,
      FhirContext fhirContext) {
    assert configuration != null : "Must supply configuration";
    assert configuration.getSparkMasterUrl() != null : "Must supply Spark master URL";
    assert configuration.getWarehouseDirectory() != null : "Must supply warehouse directory";
    assert configuration.getMetastoreUrl() != null : "Must supply metastore connection URL";
    assert configuration.getMetastoreUser() != null : "Must supply metastore user";
    assert configuration.getMetastorePassword() != null : "Must supply metastore password";
    assert configuration.getExecutorMemory() != null : "Must supply executor memory";

    logger.info("Creating new SparkQueryExecutor: " + configuration);
    this.configuration = configuration;
    this.fhirContext = fhirContext;
    initialiseSpark();
    initialiseResourceDefinitions();
    spark.sql("USE clinsight");
  }

  private void initialiseResourceDefinitions() {
    if (configuration.getTerminologyClient() != null) {
      terminologyClient = configuration.getTerminologyClient();
    } else {
      terminologyClient = fhirContext
          .newRestfulClient((TerminologyClient.class), configuration.getTerminologyServerUrl());
    }
    ResourceDefinitions.ensureInitialized(terminologyClient);
  }

  private void initialiseSpark() {
    if (configuration.getSparkSession() != null) {
      spark = configuration.getSparkSession();
    } else {
      spark = SparkSession.builder()
          .appName("clinsight-server")
          .config("spark.master", configuration.getSparkMasterUrl())
          .config("spark.sql.warehouse.dir", configuration.getWarehouseDirectory())
          .config("javax.jdo.option.ConnectionURL", configuration.getMetastoreUrl())
          .config("javax.jdo.option.ConnectionUserName", configuration.getMetastoreUser())
          .config("javax.jdo.option.ConnectionPassword", configuration.getMetastorePassword())
          .config("spark.executor.memory", configuration.getExecutorMemory())
          .config("spark.dynamicAllocation.enabled", "true")
          .config("spark.shuffle.service.enabled", "true")
          .config("spark.dynamicAllocation.initialExecutors", "1")
          .config("spark.scheduler.mode", "FAIR")
          .enableHiveSupport()
          .getOrCreate();
    }
  }

  @Override
  public AggregateQueryResult execute(AggregateQuery query) throws InvalidRequestException {
    try {

      SparkQueryPlanner queryPlanner = new SparkQueryPlanner(terminologyClient, spark, query);
      QueryPlan queryPlan = queryPlanner.buildQueryPlan();

      Dataset<Row> result = executeQueryPlan(queryPlan, query);

      return queryResultFromDataset(result, query, queryPlan);

    } catch (BaseServerResponseException e) {
      throw e;
    } catch (Exception | AssertionError e) {
      // All unexpected exceptions get logged and wrapped in a 500 for presenting back to the user.
      logger.error("Exception occurred while executing query", e);
      throw new InternalErrorException("Unexpected error occurred while executing query");
    }
  }

  private Dataset<Row> executeQueryPlan(QueryPlan queryPlan, AggregateQuery query) {
    List<String> selectExpressions = new ArrayList<>();
    if (queryPlan.getGroupings() != null && !queryPlan.getGroupings().isEmpty()) {
      for (int i = 0; i < queryPlan.getGroupings().size(); i++) {
        String grouping = queryPlan.getGroupings().get(i);
        String label = backTicks(query.getGroupings().get(i).getLabel());
        selectExpressions.add(grouping + " AS " + label);
      }
    }
    if (queryPlan.getAggregations() != null && !queryPlan.getAggregations().isEmpty()) {
      for (int i = 0; i < queryPlan.getAggregations().size(); i++) {
        String aggregation = queryPlan.getAggregations().get(i);
        String label = backTicks(query.getAggregations().get(i).getLabel());
        selectExpressions.add(aggregation + " AS " + label);
      }
    }
    LinkedList<String> groupByArgs = new LinkedList<>();
    for (int i = 0; i < queryPlan.getGroupings().size(); i++) {
      groupByArgs.add(Integer.toString(i + 1));
    }
    LinkedList<String> orderByArgs = new LinkedList<>();
    for (int i = 0; i < queryPlan.getGroupings().size() + queryPlan.getAggregations().size(); i++) {
      orderByArgs.add(Integer.toString(i + 1));
    }
    String selectClause = "SELECT " + String.join(", ", selectExpressions);
    String fromClause = "FROM " + String.join(", ", queryPlan.getFromTables());
    String joins = queryPlan.getJoins().stream().map(Join::getExpression).collect(
        Collectors.joining(" "));
    String groupByClause = "GROUP BY " + String.join(", ", groupByArgs);
    String orderByClause = "ORDER BY " + String.join(", ", orderByArgs);
    List<String> clauses = new LinkedList<>(Arrays.asList(selectClause, fromClause));
    if (!joins.isEmpty()) {
      clauses.add(joins);
    }
    if (queryPlan.getGroupings().size() > 0) {
      clauses.add(groupByClause);
    }
    clauses.add(orderByClause);
    String sql = String.join(" ", clauses);

    logger.info("Executing query: " + sql);
    return spark.sql(sql);
  }

  /**
   * Build an AggregateQueryResult resource from the supplied Dataset, embedding the original
   * AggregateQuery and honouring the hints within the QueryPlan.
   */
  private AggregateQueryResult queryResultFromDataset(@Nonnull Dataset<Row> dataset,
      @Nonnull AggregateQuery query, QueryPlan queryPlan) {
    List<Row> rows = dataset.collectAsList();
    int numGroupings = query.getGroupings().size();
    int numAggregations = query.getAggregations().size();

    AggregateQueryResult queryResult = new AggregateQueryResult();

    List<Grouping> groupings = rows.stream()
        .map(mapRowToGrouping(queryPlan, numGroupings, numAggregations))
        .collect(Collectors.toList());
    queryResult.getGroupings().addAll(groupings);

    return queryResult;
  }

  /**
   * Translate a Dataset Row into a grouping component for inclusion within an
   * AggregateQueryResult.
   */
  private Function<Row, AggregateQueryResult.Grouping> mapRowToGrouping(
      @Nonnull QueryPlan queryPlan, int numGroupings,
      int numAggregations) {
    return row -> {
      AggregateQueryResult.Grouping grouping = new AggregateQueryResult.Grouping();

      for (int i = 0; i < numGroupings; i++) {
        String fhirType = queryPlan.getGroupingTypes().get(i);
        Type value = getValueFromRow(row, i, fhirType);
        grouping.getLabels().add(value);
      }

      for (int i = 0; i < numAggregations; i++) {
        String fhirType = queryPlan.getAggregationTypes().get(i);
        Type value = getValueFromRow(row, i + numGroupings, fhirType);
        grouping.getResults().add(value);
      }

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
      assert fhirClass != null : "Unable to map FHIR type to FHIR class: " + fhirType;
      Class javaClass = Mappings.getJavaClass(fhirType);
      assert javaClass != null : "Unable to map FHIR type to Java class: " + fhirType;
      @SuppressWarnings("unchecked") Constructor constructor = fhirClass.getConstructor(javaClass);
      Object value = row.get(columnNumber);
      return value == null ? null : (Type) constructor.newInstance(value);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException | InstantiationException e) {
      logger.error("Failed to access value from row: " + e.getMessage());
      return null;
    }
  }

}
