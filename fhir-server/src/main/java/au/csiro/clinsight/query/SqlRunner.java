/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class wraps the execution of SQL queries within a Spark Session, wrapping them with logging
 * and configurable explain.
 *
 * @author John Grimes
 */
public class SqlRunner {

  private static final Logger logger = LoggerFactory.getLogger(SqlRunner.class);

  private final SparkSession spark;

  private final boolean explain;

  public SqlRunner(SparkSession spark, boolean explain) {
    this.spark = spark;
    this.explain = explain;
  }

  public Dataset<Row> run(String sql) {
    logger.info("Executing query: " + sql);
    if (explain) {
      Dataset<Row> explain = spark.sql("EXPLAIN " + sql);
      String queryPlanText = explain.collectAsList().get(0).getString(0);
      logger.debug("Query plan: " + queryPlanText);
    }
    return spark.sql(sql);
  }
}
