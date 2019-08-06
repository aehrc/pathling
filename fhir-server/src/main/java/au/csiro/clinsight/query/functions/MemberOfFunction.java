/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import static au.csiro.clinsight.query.parsing.Join.JoinType.LEFT_JOIN;
import static au.csiro.clinsight.query.parsing.Join.rewriteSqlWithJoinAliases;
import static au.csiro.clinsight.query.parsing.Join.wrapLateralViews;
import static au.csiro.clinsight.query.parsing.ParseResult.FhirPathType.STRING;

import au.csiro.clinsight.TerminologyClient;
import au.csiro.clinsight.query.Code;
import au.csiro.clinsight.query.parsing.AliasGenerator;
import au.csiro.clinsight.query.parsing.ExpressionParserContext;
import au.csiro.clinsight.query.parsing.Join;
import au.csiro.clinsight.query.parsing.ParseResult;
import au.csiro.clinsight.query.parsing.ParseResult.FhirPathType;
import au.csiro.clinsight.query.parsing.ParseResult.FhirType;
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

  @Nonnull
  @Override
  public ParseResult invoke(@Nonnull ExpressionFunctionInput input) {
    ParseResult inputResult = validateInput(input.getInput());
    ParseResult argument = validateArgument(input.getArguments());
    ExpressionParserContext context = input.getContext();
    AliasGenerator aliasGenerator = context.getAliasGenerator();

    assert inputResult.getFhirPath() != null;
    assert argument.getFhirPath() != null;
    String unquotedArgument = argument.getFhirPath()
        .substring(1, argument.getFhirPath().length() - 1);
    // Get a shortened hash of the ValueSet URL for use as part of the temporary view name, and also
    // within the alias in the query. This helps us ensure uniqueness of the names we use.
    String argumentHash = Strings.md5(unquotedArgument).substring(0, 7);
    String tableName = "valueSet_" + argumentHash;

    // Create a table containing the results of the expanded ValueSet, if it doesn't already exist.
    ensureExpansionTableExists(unquotedArgument, tableName, context);

    // Build a SQL expression representing the new subquery that provides the result of the ValueSet
    // membership test.
    String valueSetJoinAlias = aliasGenerator.getAlias();
    String wrapperJoinAlias = aliasGenerator.getAlias();

    // If this is a CodeableConcept, we need to update the input expression to the `coding`
    // member first.
    String inputType = inputResult.getPathTraversal().getElementDefinition().getTypeCode();
    if (inputType.equals("CodeableConcept")) {
      ExpressionFunctionInput memberInvocationInput = new ExpressionFunctionInput();
      memberInvocationInput.setContext(context);
      memberInvocationInput.setExpression("coding");
      memberInvocationInput.setInput(inputResult);
      inputResult = new MemberInvocation().invoke(memberInvocationInput);
    }

    String inputSql = rewriteSqlWithJoinAliases(inputResult.getSql(), inputResult.getJoins());

    Join valueSetJoin = new Join();
    String valueSetJoinSql = "LEFT JOIN " + tableName + " " + valueSetJoinAlias + " ";
    valueSetJoinSql += "ON " + inputSql + ".system = " + valueSetJoinAlias + ".system ";
    valueSetJoinSql += "AND " + inputSql + ".code = " + valueSetJoinAlias + ".code";
    valueSetJoin.setSql(valueSetJoinSql);
    valueSetJoin.setJoinType(LEFT_JOIN);
    valueSetJoin.setTableAlias(valueSetJoinAlias);
    valueSetJoin.setAliasTarget(tableName);
    valueSetJoin.getDependsUpon().add(inputResult.getJoins().last());

    // Build the candidate set of inner joins.
    SortedSet<Join> innerJoins = new TreeSet<>(inputResult.getJoins());
    // If the input has joins and the last one is a lateral view, we will need to wrap the upstream
    // joins. This is because Spark SQL does not currently allow a table join to follow a lateral
    // view within a query.
    innerJoins.add(valueSetJoin);
    innerJoins = wrapLateralViews(innerJoins, valueSetJoin, aliasGenerator, context.getFromTable());

    // If there is a filter to be applied as part of this invocation, add in its join dependencies.
    if (input.getFilter() != null) {
      innerJoins.addAll(input.getFilterJoins());
    }

    String wrapperJoinSql = "LEFT JOIN (";
    wrapperJoinSql += "SELECT " + context.getFromTable() + ".id, ";
    wrapperJoinSql += "MAX(" + valueSetJoinAlias + ".code) IS NULL AS result ";
    wrapperJoinSql += "FROM " + context.getFromTable() + " ";
    wrapperJoinSql += innerJoins.stream().map(Join::getSql).collect(Collectors.joining(" ")) + " ";

    // If there is a filter to be applied as part of this invocation, add a where clause in here.
    if (input.getFilter() != null) {
      String filter = rewriteSqlWithJoinAliases(input.getFilter(), innerJoins);
      wrapperJoinSql += "WHERE " + filter + " ";
    }

    wrapperJoinSql += "GROUP BY 1";
    wrapperJoinSql +=
        ") " + wrapperJoinAlias + " ON " + context.getFromTable() + ".id = " + wrapperJoinAlias
            + ".id";

    // Create a new Join that represents the join to the new subquery.
    Join wrapperJoin = new Join();
    wrapperJoin.setSql(wrapperJoinSql);
    wrapperJoin.setJoinType(LEFT_JOIN);
    wrapperJoin.setTableAlias(wrapperJoinAlias);
    wrapperJoin.setAliasTarget(wrapperJoinAlias);
    wrapperJoin.setTargetElement(inputResult.getPathTraversal().getElementDefinition());

    // Build a new parse result, representing the results of the ValueSet expansion.
    ParseResult result = new ParseResult();
    result.setFunction(this);
    result.setFunctionInput(input);
    result.setFhirPath(input.getExpression());
    result.setSql(wrapperJoinAlias + ".result");
    result.setFhirPathType(FhirPathType.BOOLEAN);
    result.setFhirType(FhirType.BOOLEAN);
    result.getJoins().add(wrapperJoin);
    result.setPrimitive(true);
    result.setSingular(inputResult.isSingular());
    return result;
  }

  private ParseResult validateInput(@Nullable ParseResult input) {
    if (input == null) {
      throw new InvalidRequestException("Must provide an input to memberOf function");
    }
    String typeCode = input.getPathTraversal().getElementDefinition().getTypeCode();
    if (input.isPrimitive() || !(typeCode.equals("Coding") || typeCode.equals("CodeableConcept"))) {
      throw new InvalidRequestException(
          "Input to memberOf function must be Coding or CodeableConcept: " + input.getFhirPath());
    }
    return input;
  }

  @Nonnull
  private ParseResult validateArgument(@Nonnull List<ParseResult> arguments) {
    if (arguments.size() != 1) {
      throw new InvalidRequestException("Must pass URL argument to memberOf function");
    }
    ParseResult argument = arguments.get(0);
    if (argument.getFhirPathType() != STRING) {
      throw new InvalidRequestException(
          "Argument to memberOf function must be a String: " + argument.getFhirPath());
    }
    return argument;
  }

  /**
   * Expands the specified ValueSet using the terminology server, and saves the result to a
   * temporary view identified by the specified table name.
   */
  private void ensureExpansionTableExists(String valueSetUrl, String tableName,
      ExpressionParserContext context) {
    SparkSession spark = context.getSparkSession();
    String databaseName = context.getDatabaseName();
    TerminologyClient terminologyClient = context.getTerminologyClient();

    if (!spark.catalog().tableExists(databaseName, tableName)) {
      ValueSet expansion = terminologyClient.expandValueSet(new UriType(valueSetUrl));
      List<Code> expansionRows = expansion.getExpansion().getContains().stream()
          .map(contains -> {
            Code code = new Code();
            code.setSystem(contains.getSystem());
            code.setCode(contains.getCode());
            return code;
          })
          .collect(Collectors.toList());
      Dataset<Code> expansionDataset = spark
          .createDataset(expansionRows, Encoders.bean(Code.class));
      expansionDataset.createOrReplaceTempView(tableName);
    }
  }

}
