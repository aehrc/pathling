/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import static au.csiro.clinsight.fhir.definitions.ElementResolver.resolveElement;
import static au.csiro.clinsight.utilities.Strings.pathToLowerCamelCase;
import static au.csiro.clinsight.utilities.Strings.tokenizePath;

import au.csiro.clinsight.TerminologyClient;
import au.csiro.clinsight.fhir.definitions.ResolvedElement;
import au.csiro.clinsight.fhir.definitions.ResolvedElement.ResolvedElementType;
import au.csiro.clinsight.query.Code;
import au.csiro.clinsight.query.parsing.Join;
import au.csiro.clinsight.query.parsing.Join.JoinType;
import au.csiro.clinsight.query.parsing.ParseResult;
import au.csiro.clinsight.query.parsing.ParseResult.ParseResultType;
import au.csiro.clinsight.utilities.Strings;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.List;
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
    ParseResult argument = validateArguments(input, arguments);
    @SuppressWarnings("ConstantConditions") ResolvedElement element = resolveElement(
        input.getExpression());
    assert element.getType() == ResolvedElementType.COMPLEX;
    assert element.getTypeCode() != null;
    assert element.getTypeCode().equals("Coding");

    List<String> pathComponents = tokenizePath(element.getPath());
    String joinAlias = input.getJoins().isEmpty()
        ? pathToLowerCamelCase(pathComponents) + "ValueSet"
        : input.getJoins().last().getTableAlias() + "ValueSet";
    assert argument.getExpression() != null;
    String unquotedArgument = argument.getExpression()
        .substring(1, argument.getExpression().length() - 1);
    String rootExpression = "valueSet_" + Strings.md5(unquotedArgument);
    String quotedRootExpression = "`" + rootExpression + "`";

    ensureExpansionTableExists(unquotedArgument, rootExpression);

    String lastJoinAlias = input.getJoins().last().getTableAlias();
    String joinExpression = "LEFT OUTER JOIN " + quotedRootExpression + " " + joinAlias + " ";
    joinExpression += "ON " + lastJoinAlias + ".system = " + joinAlias + ".system ";
    joinExpression += "AND " + lastJoinAlias + ".code = " + joinAlias + ".code";
    String sqlExpression = "/* MAPJOIN(" + joinAlias + ") */ CASE WHEN " + joinAlias
        + ".code IS NULL THEN FALSE ELSE TRUE END";
    Join join = new Join(joinExpression, rootExpression, JoinType.TABLE_JOIN, joinAlias);
    if (!input.getJoins().isEmpty()) {
      join.setDependsUpon(input.getJoins().last());
    }
    input.setResultType(ParseResultType.ELEMENT_PATH);
    input.setElementType(ResolvedElementType.PRIMITIVE);
    input.setElementTypeCode("boolean");
    input.setSqlExpression(sqlExpression);
    input.getJoins().add(join);
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
  private ParseResult validateArguments(@Nullable ParseResult input,
      @Nonnull List<ParseResult> arguments) {
    validateInput(input);
    if (arguments.size() != 1) {
      throw new InvalidRequestException("Must pass URL argument to inValueSet function");
    }
    ParseResult argument = arguments.get(0);
    if (argument.getResultType() != ParseResultType.STRING_LITERAL) {
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
