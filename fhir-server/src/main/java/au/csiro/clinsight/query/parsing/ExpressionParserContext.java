/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.parsing;

import au.csiro.clinsight.fhir.FhirContextFactory;
import au.csiro.clinsight.fhir.TerminologyClient;
import au.csiro.clinsight.query.ResourceReader;
import ca.uhn.fhir.context.FhirContext;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.SparkSession;

/**
 * Contains dependencies for the execution of an ExpressionParser.
 *
 * @author John Grimes
 */
public class ExpressionParserContext {

  /**
   * A FHIR context that can be used to do FHIR stuff.
   */
  private FhirContext fhirContext;

  /**
   * A factory for creating new FhirContext objects, which is needed within blocks of code that are
   * run in parallel.
   */
  private FhirContextFactory fhirContextFactory;

  /**
   * The terminology client that should be used to resolve terminology queries within this
   * expression.
   */
  private TerminologyClient terminologyClient;

  /**
   * The Spark session that should be used to resolve Spark queries required for this expression.
   */
  private SparkSession sparkSession;

  /**
   * A table resolver for retrieving Datasets for resource references.
   */
  private ResourceReader resourceReader;

  /**
   * A ParseResult representing the subject resource specified within the query, which is then
   * referred to through `%resource` or `%context`.
   */
  private ParsedExpression subjectContext;

  /**
   * A ParseResult representing an item from an input collection currently under evaluation, e.g.
   * within the argument to the `where` function.
   */
  private ParsedExpression thisContext;

  /**
   * Groupings to be applied to this expression at the point of aggregation.
   */
  private final List<ParsedExpression> groupings = new ArrayList<>();

  public ExpressionParserContext() {
  }

  public ExpressionParserContext(ExpressionParserContext context) {
    this.fhirContext = context.fhirContext;
    this.fhirContextFactory = context.fhirContextFactory;
    this.terminologyClient = context.terminologyClient;
    this.sparkSession = context.sparkSession;
    this.resourceReader = context.resourceReader;
    this.subjectContext = context.subjectContext;
    this.thisContext = context.thisContext;
    this.groupings.addAll(context.groupings);
  }

  public FhirContext getFhirContext() {
    return fhirContext;
  }

  public void setFhirContext(FhirContext fhirContext) {
    this.fhirContext = fhirContext;
  }

  public FhirContextFactory getFhirContextFactory() {
    return fhirContextFactory;
  }

  public void setFhirContextFactory(FhirContextFactory fhirContextFactory) {
    this.fhirContextFactory = fhirContextFactory;
  }

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

  public ResourceReader getResourceReader() {
    return resourceReader;
  }

  public void setResourceReader(ResourceReader resourceReader) {
    this.resourceReader = resourceReader;
  }

  public ParsedExpression getSubjectContext() {
    return subjectContext;
  }

  public void setSubjectContext(ParsedExpression subjectContext) {
    this.subjectContext = subjectContext;
  }

  public ParsedExpression getThisContext() {
    return thisContext;
  }

  public void setThisContext(ParsedExpression thisContext) {
    this.thisContext = thisContext;
  }

  public List<ParsedExpression> getGroupings() {
    return groupings;
  }

}
