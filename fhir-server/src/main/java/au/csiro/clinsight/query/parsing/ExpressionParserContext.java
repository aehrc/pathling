/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.parsing;

import au.csiro.clinsight.TerminologyClient;
import org.apache.spark.sql.SparkSession;

/**
 * Contains dependencies for the execution of an ExpressionParser.
 *
 * @author John Grimes
 */
public class ExpressionParserContext {

  private TerminologyClient terminologyClient;
  private SparkSession sparkSession;
  private String databaseName;
  private AliasGenerator aliasGenerator;

  public TerminologyClient getTerminologyClient() {
    return terminologyClient;
  }

  public void setTerminologyClient(TerminologyClient terminologyClient) {
    this.terminologyClient = terminologyClient;
  }

  public SparkSession getSparkSession() {
    return sparkSession;
  }

  public void setSparkSession(SparkSession sparkSession) {
    this.sparkSession = sparkSession;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  public AliasGenerator getAliasGenerator() {
    return aliasGenerator;
  }

  public void setAliasGenerator(AliasGenerator aliasGenerator) {
    this.aliasGenerator = aliasGenerator;
  }
}
