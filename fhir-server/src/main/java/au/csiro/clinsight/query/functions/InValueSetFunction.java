/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import static au.csiro.clinsight.fhir.definitions.ElementResolver.resolveElement;
import static au.csiro.clinsight.fhir.definitions.ResolvedElement.ResolvedElementType.COMPLEX;
import static au.csiro.clinsight.fhir.definitions.ResolvedElement.ResolvedElementType.PRIMITIVE;
import static au.csiro.clinsight.query.QueryWrangling.convertUpstreamLateralViewsToInlineQueries;
import static au.csiro.clinsight.query.parsing.Join.JoinType.EXISTS_JOIN;
import static au.csiro.clinsight.query.parsing.Join.JoinType.TABLE_JOIN;
import static au.csiro.clinsight.query.parsing.ParseResult.ParseResultType.COLLECTION;
import static au.csiro.clinsight.query.parsing.ParseResult.ParseResultType.STRING;
import static au.csiro.clinsight.utilities.Strings.pathToLowerCamelCase;
import static au.csiro.clinsight.utilities.Strings.tokenizePath;

import au.csiro.clinsight.TerminologyClient;
import au.csiro.clinsight.fhir.definitions.ResolvedElement;
import au.csiro.clinsight.query.Code;
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
public class InValueSetFunction implements ExpressionFunction {

  private TerminologyClient terminologyClient;
  private SparkSession spark;

  @Nonnull
  @Override
  public ParseResult invoke(@Nullable ParseResult input, @Nonnull List<ParseResult> arguments) {
    ParseResult argument = validateArgument(input, arguments);
    @SuppressWarnings("ConstantConditions") ResolvedElement element = resolveElement(
        input.getExpression());
    assert element.getType() == COMPLEX;
    assert element.getTypeCode() != null;
    assert element.getTypeCode().equals("Coding");

    List<String> pathComponents = tokenizePath(element.getPath());
    assert argument.getExpression() != null;
    String unquotedArgument = argument.getExpression()
        .substring(1, argument.getExpression().length() - 1);
    // Get a shortened hash of the ValueSet URL for use as part of the temporary view name, and also
    // within the alias in the query. This helps us ensure uniqueness of the names we use.
    String argumentHash = Strings.md5(unquotedArgument).substring(0, 7);
    String joinAlias = input.getJoins().isEmpty()
        ? pathToLowerCamelCase(pathComponents) + "ValueSet" + argumentHash
        : input.getJoins().last().getTableAlias() + "ValueSet" + argumentHash;
    String rootExpression = "valueSet_" + argumentHash;
    String quotedRootExpression = "`" + rootExpression + "`";

    // Create a table containing the results of the expanded ValueSet, if it doesn't already exist.
    ensureExpansionTableExists(unquotedArgument, rootExpression);

    // Build an expression which joins to the new ValueSet expansion table.
    String lastJoinAlias = input.getJoins().last().getTableAlias();
    String valueSetJoinExpression = "LEFT JOIN " + quotedRootExpression + " " + joinAlias + " ";
    valueSetJoinExpression += "ON " + lastJoinAlias + ".system = " + joinAlias + ".system ";
    valueSetJoinExpression += "AND " + lastJoinAlias + ".code = " + joinAlias + ".code";
    Join valueSetJoin = new Join(valueSetJoinExpression, rootExpression, TABLE_JOIN, joinAlias);
    if (!input.getJoins().isEmpty()) {
      valueSetJoin.setDependsUpon(input.getJoins().last());
    }

    // Build a select expression which tests whether there is a code on the right-hand side of the
    // left join, returning a boolean.
    String resourceTable = (String) input.getFromTables().toArray()[0];
    String selectExpression = "SELECT " + resourceTable + ".id, CASE WHEN MAX(" + joinAlias
        + ".code) IS NULL THEN FALSE ELSE TRUE END AS codeExists";

    // Add the new join to the joins from the input, and convert any lateral views to inline
    // queries.
    SortedSet<Join> subqueryJoins = new TreeSet<>(input.getJoins());
    subqueryJoins.add(valueSetJoin);
    subqueryJoins = convertUpstreamLateralViewsToInlineQueries(subqueryJoins);

    // Convert the set of views into an inline query. This is necessary due to the fact that we have
    // two levels of aggregation, one to aggregate possible multiple codes into a single exists or
    // not boolean expression, and the second to perform the requested aggregations across any
    // groupings (e.g. counting).
    String joinExpressions = subqueryJoins.stream().map(Join::getExpression)
        .collect(Collectors.joining(" "));
    String existsJoinAlias = joinAlias + "Aggregated";
    String existsJoinExpression =
        "LEFT JOIN (" + selectExpression + " FROM " + resourceTable + " " + joinExpressions
            + " GROUP BY 1) " + existsJoinAlias + " ON " + resourceTable + ".id = "
            + existsJoinAlias + ".id";
    String existsSelect = existsJoinAlias + ".codeExists";

    // Clear the old joins out of the input and replace them with the new join to the inline query.
    Join existsJoin = new Join(existsJoinExpression, rootExpression, EXISTS_JOIN, existsJoinAlias);
    input.getJoins().clear();
    input.getJoins().add(existsJoin);
    input.setResultType(COLLECTION);
    input.setElementType(PRIMITIVE);
    input.setElementTypeCode("boolean");
    input.setSqlExpression(existsSelect);
    return input;
  }

  private void validateInput(@Nullable ParseResult input) {
    if (input == null) {
      throw new InvalidRequestException("Missing input expression for resolve function");
    }
    assert input.getElementTypeCode() != null;
    if (!input.getElementTypeCode().equals("Coding")) {
      throw new InvalidRequestException(
          "Input to resolve function must be a Coding: " + input.getExpression() + " ("
              + input.getElementTypeCode() + ")");
    }
  }

  @Nonnull
  private ParseResult validateArgument(@Nullable ParseResult input,
      @Nonnull List<ParseResult> arguments) {
    validateInput(input);
    if (arguments.size() != 1) {
      throw new InvalidRequestException("Must pass URL argument to inValueSet function");
    }
    ParseResult argument = arguments.get(0);
    if (argument.getResultType() != STRING) {
      throw new InvalidRequestException(
          "Argument to inValueSet function must be a string: " + argument.getExpression());
    }
    return argument;
  }

  /**
   * Expands the specified ValueSet using the terminology server, and saves the result to a
   * temporary view identified by the specified table name.
   */
  private void ensureExpansionTableExists(String valueSetUrl, String tableName) {
    if (!spark.catalog().tableExists("clinsight", tableName)) {
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
  public void setTerminologyClient(@Nonnull TerminologyClient terminologyClient) {
    this.terminologyClient = terminologyClient;
  }

  @Override
  public void setSparkSession(@Nonnull SparkSession spark) {
    this.spark = spark;
  }

}
