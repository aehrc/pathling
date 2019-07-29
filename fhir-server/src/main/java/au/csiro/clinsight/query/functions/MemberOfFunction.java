/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import static au.csiro.clinsight.query.parsing.Join.JoinType.LATERAL_VIEW;
import static au.csiro.clinsight.query.parsing.Join.JoinType.MEMBERSHIP_JOIN;
import static au.csiro.clinsight.query.parsing.Join.JoinType.TABLE_JOIN;
import static au.csiro.clinsight.query.parsing.Join.rewriteSqlWithJoinAliases;
import static au.csiro.clinsight.query.parsing.Join.wrapUpstreamJoins;
import static au.csiro.clinsight.query.parsing.ParseResult.ParseResultType.BOOLEAN;
import static au.csiro.clinsight.query.parsing.ParseResult.ParseResultType.STRING;

import au.csiro.clinsight.TerminologyClient;
import au.csiro.clinsight.query.Code;
import au.csiro.clinsight.query.parsing.ExpressionParserContext;
import au.csiro.clinsight.query.parsing.Join;
import au.csiro.clinsight.query.parsing.ParseResult;
import au.csiro.clinsight.utilities.Strings;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.dstu3.model.UriType;
import org.hl7.fhir.dstu3.model.ValueSet;

/**
 * A function that takes a set of Codings as inputs and returns a set of boolean values, based upon
 * whether each Coding is present within the ValueSet identified by the supplied URL.
 *
 * @author John Grimes
 */
public class MemberOfFunction implements ExpressionFunction {

  private ExpressionParserContext context;

  @Nonnull
  @Override
  public ParseResult invoke(@Nonnull String expression, @Nullable ParseResult input,
      @Nonnull List<ParseResult> arguments) {
    validateInput(input);
    ParseResult argument = validateArgument(arguments);

    assert input.getFhirPath() != null;
    assert argument.getFhirPath() != null;
    String unquotedArgument = argument.getFhirPath()
        .substring(1, argument.getFhirPath().length() - 1);
    // Get a shortened hash of the ValueSet URL for use as part of the temporary view name, and also
    // within the alias in the query. This helps us ensure uniqueness of the names we use.
    String argumentHash = Strings.md5(unquotedArgument).substring(0, 7);
    String tableName = "valueSet_" + argumentHash;

    // Create a table containing the results of the expanded ValueSet, if it doesn't already exist.
    ensureExpansionTableExists(unquotedArgument, tableName);

    // Build a SQL expression representing the new subquery that provides the result of the ValueSet
    // membership test.
    String newJoinAlias = context.getAliasGenerator().getAlias();
    String subqueryAlias = context.getAliasGenerator().getAlias();

    // If this is a CodeableConcept, we need to update the input expression to the `coding`
    // member first.
    String inputType = input.getPathTraversal().getElementDefinition().getTypeCode();
    if (inputType.equals("CodeableConcept")) {
      input = new MemberInvocation(context).invoke("coding", input);
    }

    String inputSql = rewriteSqlWithJoinAliases(input.getSql(), input.getJoins());

    Join valueSetJoin = new Join();
    String valueSetJoinSql = "LEFT JOIN " + tableName + " " + newJoinAlias + " ";
    valueSetJoinSql += "ON " + inputSql + ".system = " + newJoinAlias + ".system ";
    valueSetJoinSql += "AND " + inputSql + ".code = " + newJoinAlias + ".code";
    valueSetJoin.setSql(valueSetJoinSql);
    valueSetJoin.setJoinType(TABLE_JOIN);
    valueSetJoin.setTableAlias(newJoinAlias);
    valueSetJoin.setAliasTarget(tableName);
    valueSetJoin.setDependsUpon(input.getJoins().last());

    // Build the candidate set of inner joins.
    SortedSet<Join> innerJoins = new TreeSet<>(input.getJoins());
    // If the input has joins and the last one is a lateral view, we will need to wrap the upstream
    // joins. This is because Spark SQL does not currently allow a table join to follow a lateral
    // view within a query.
    if (!innerJoins.isEmpty() && innerJoins.last().getJoinType() == LATERAL_VIEW) {
      SortedSet<Join> wrappedJoins = wrapUpstreamJoins(innerJoins,
          context.getAliasGenerator().getAlias(), context.getFromTable());
      innerJoins.clear();
      innerJoins.addAll(wrappedJoins);
    }
    innerJoins.add(valueSetJoin);

    String subquery = "LEFT JOIN (";
    subquery += "SELECT " + context.getFromTable() + ".id, ";
    subquery += "MAX(" + newJoinAlias + ".code) IS NULL AS result ";
    subquery += "FROM " + context.getFromTable() + " ";
    subquery += innerJoins.stream().map(Join::getSql).collect(Collectors.joining(" ")) + " ";
    subquery += "GROUP BY 1";
    subquery +=
        ") " + subqueryAlias + " ON " + context.getFromTable() + ".id = " + subqueryAlias + ".id";

    // Create a new Join that represents the join to the new subquery.
    Join newJoin = new Join();
    newJoin.setSql(subquery);
    newJoin.setJoinType(MEMBERSHIP_JOIN);
    newJoin.setTableAlias(subqueryAlias);
    newJoin.setAliasTarget(subqueryAlias);
    newJoin.setTargetElement(input.getPathTraversal().getElementDefinition());

    // Build a new parse result, representing the results of the ValueSet expansion.
    ParseResult result = new ParseResult();
    result.setFhirPath(expression);
    result.setSql(subqueryAlias + ".result");
    result.setResultType(BOOLEAN);
    result.getJoins().add(newJoin);
    result.setPrimitive(true);
    result.setSingular(input.isSingular());
    return result;
  }

  private void validateInput(@Nullable ParseResult input) {
    if (input == null) {
      throw new InvalidRequestException("Must provide an input to memberOf function");
    }
    String typeCode = input.getPathTraversal().getElementDefinition().getTypeCode();
    if (input.isPrimitive() || !(typeCode.equals("Coding") || typeCode.equals("CodeableConcept"))) {
      throw new InvalidRequestException(
          "Input to memberOf function must be Coding or CodeableConcept: " + input.getFhirPath());
    }
  }

  @Nonnull
  private ParseResult validateArgument(@Nonnull List<ParseResult> arguments) {
    if (arguments.size() != 1) {
      throw new InvalidRequestException("Must pass URL argument to memberOf function");
    }
    ParseResult argument = arguments.get(0);
    if (argument.getResultType() != STRING) {
      throw new InvalidRequestException(
          "Argument to memberOf function must be a String: " + argument.getFhirPath());
    }
    return argument;
  }

  /**
   * Expands the specified ValueSet using the terminology server, and saves the result to a
   * temporary view identified by the specified table name.
   */
  private void ensureExpansionTableExists(String valueSetUrl, String tableName) {
    SparkSession spark = context.getSparkSession();
    String databaseName = context.getDatabaseName();
    TerminologyClient terminologyClient = context.getTerminologyClient();

    if (!spark.catalog().tableExists(databaseName, tableName)) {
      ValueSet expansion = terminologyClient.expandValueSet(new UriType(valueSetUrl));
      List<Code> expansionRows = expansion.getExpansion().getContains().stream()
          .map(contains -> new Code(contains.getSystem(), contains.getCode()))
          .collect(Collectors.toList());
      Dataset<Code> expansionDataset = spark
          .createDataset(expansionRows, Encoders.bean(Code.class));
      expansionDataset.createOrReplaceTempView(tableName);
    }
  }

  @Override
  public void setContext(@Nonnull ExpressionParserContext context) {
    this.context = context;
  }

}
