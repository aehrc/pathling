/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.functions;

import static au.csiro.clinsight.query.parsing.Join.JoinType.LEFT_JOIN;
import static au.csiro.clinsight.query.parsing.Join.rewriteSqlWithJoinAliases;
import static au.csiro.clinsight.query.parsing.Join.wrapLateralViews;
import static au.csiro.clinsight.query.parsing.ParseResult.FhirPathType.CODING;
import static au.csiro.clinsight.utilities.Strings.singleQuote;

import au.csiro.clinsight.TerminologyClient;
import au.csiro.clinsight.query.Mapping;
import au.csiro.clinsight.query.parsing.AliasGenerator;
import au.csiro.clinsight.query.parsing.ExpressionParserContext;
import au.csiro.clinsight.query.parsing.Join;
import au.csiro.clinsight.query.parsing.Join.JoinType;
import au.csiro.clinsight.query.parsing.ParseResult;
import au.csiro.clinsight.query.parsing.ParseResult.FhirPathType;
import au.csiro.clinsight.query.parsing.ParseResult.FhirType;
import au.csiro.clinsight.utilities.Strings;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.dstu3.model.Coding;
import org.hl7.fhir.dstu3.model.ConceptMap;
import org.hl7.fhir.dstu3.model.ConceptMap.ConceptMapGroupComponent;
import org.hl7.fhir.dstu3.model.ConceptMap.SourceElementComponent;
import org.hl7.fhir.dstu3.model.ConceptMap.TargetElementComponent;
import org.hl7.fhir.dstu3.model.StringType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Describes a function which returns a boolean value based upon whether any of the input set of
 * Codings or CodeableConcepts subsume one or more Codings or CodeableConcepts in the target set.
 *
 * @author John Grimes
 */
public class SubsumesFunction implements ExpressionFunction {

  private static final Logger logger = LoggerFactory.getLogger(SubsumesFunction.class);
  private boolean inverted = false;
  private String functionName = "subsumes";

  public SubsumesFunction() {
  }

  public SubsumesFunction(boolean inverted) {
    this.inverted = inverted;
    if (inverted) {
      this.functionName = "subsumedBy";
    }
  }

  @Nonnull
  @Override
  public ParseResult invoke(@Nonnull ExpressionFunctionInput input) {
    if (input.getArguments().size() != 1) {
      throw new InvalidRequestException(
          "One argument must be passed to " + functionName + " function");
    }

    ParseResult inputResult = inverted ? validateArgument(input) : validateInput(input);
    ParseResult argument = inverted ? validateInput(input) : validateArgument(input);
    ExpressionParserContext context = input.getContext();
    AliasGenerator aliasGenerator = context.getAliasGenerator();

    String closureTableName = ensureClosureTableExists(input, inputResult, argument);

    String closureJoinAlias = input.getContext().getAliasGenerator().getAlias();
    String wrapperJoinAlias = input.getContext().getAliasGenerator().getAlias();

    // Build a new join across to the closure table.
    Join closureJoin = new Join();
    String closureJoinSql = "LEFT JOIN " + closureTableName + " " + closureJoinAlias + " ";

    if (inputResult.getLiteralValue() != null) {
      // If the input is a Coding literal, inject literal values into the SQL.
      Coding coding = (Coding) inputResult.getLiteralValue();
      closureJoinSql +=
          "ON " + singleQuote(coding.getSystem()) + " = " + closureJoinAlias + ".targetSystem ";
      closureJoinSql +=
          "AND " + singleQuote(coding.getCode()) + " = " + closureJoinAlias + ".targetCode ";
    } else {
      // Otherwise, the values are queried using the SQL expressions within the parse result.
      closureJoinSql +=
          "ON " + inputResult.getSql() + ".system = " + closureJoinAlias + ".targetSystem ";
      closureJoinSql +=
          "AND " + inputResult.getSql() + ".code = " + closureJoinAlias + ".targetCode ";
    }

    if (argument.getLiteralValue() != null) {
      // If the argument is a Coding literal, inject literal values into the SQL.
      Coding coding = (Coding) argument.getLiteralValue();
      closureJoinSql +=
          "AND " + singleQuote(coding.getSystem()) + " = " + closureJoinAlias + ".sourceSystem ";
      closureJoinSql +=
          "AND " + singleQuote(coding.getCode()) + " = " + closureJoinAlias + ".sourceCode";
    } else {
      // Otherwise, the values are queried using the SQL expressions within the parse result.
      closureJoinSql +=
          "AND " + argument.getSql() + ".system = " + closureJoinAlias + ".sourceSystem ";
      closureJoinSql += "AND " + argument.getSql() + ".code = " + closureJoinAlias + ".sourceCode";
    }

    closureJoin.setSql(closureJoinSql);
    closureJoin.setJoinType(JoinType.LEFT_JOIN);
    closureJoin.setTableAlias(closureJoinAlias);
    closureJoin.setAliasTarget(closureTableName);
    if (!inputResult.getJoins().isEmpty()) {
      closureJoin.getDependsUpon().add(inputResult.getJoins().last());
    }
    if (!argument.getJoins().isEmpty()) {
      closureJoin.getDependsUpon().add(argument.getJoins().last());
    }

    // Build the candidate set of inner joins.
    SortedSet<Join> innerJoins = new TreeSet<>(inputResult.getJoins());
    innerJoins.addAll(argument.getJoins());

    // If there is a filter to be applied as part of this invocation, add in its join dependencies.
    if (input.getFilter() != null) {
      innerJoins.addAll(input.getFilterJoins());
    }

    closureJoin.setSql(rewriteSqlWithJoinAliases(closureJoin.getSql(), innerJoins));
    innerJoins.add(closureJoin);

    // Wrap any upstream dependencies of our new join which are lateral views.
    innerJoins = wrapLateralViews(innerJoins, closureJoin, aliasGenerator, context.getFromTable());

    String wrapperJoinSql = "LEFT JOIN (";
    wrapperJoinSql += "SELECT " + context.getFromTable() + ".id, ";
    wrapperJoinSql +=
        "MAX(" + closureJoinAlias + ".equivalence IN ('subsumes', 'equal')) AS result ";
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
    if (inputResult.getPathTraversal() != null) {
      wrapperJoin.setTargetElement(inputResult.getPathTraversal().getElementDefinition());
    }

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

  /**
   * Executes a closure operation including the codes from the input and argument to this function,
   * then store the result in a Spark Dataset accessible via a temporary view.
   */
  private String ensureClosureTableExists(ExpressionFunctionInput input, ParseResult inputResult,
      ParseResult argument) {
    String resourceTable = input.getContext().getFromTable();
    SparkSession spark = input.getContext().getSparkSession();
    List<Coding> codings = new ArrayList<>();

    // Get the set of codes from both the input and the argument.
    String inputQuery = null;
    String argumentQuery = null;
    // If the input is a literal, harvest the code - otherwise we need to query for the codes.
    if (inputResult.getLiteralValue() != null) {
      codings.add((Coding) inputResult.getLiteralValue());
    } else {
      inputQuery = buildCodeQuery(inputResult, input);
    }
    // If the argument is a literal, harvest the code - otherwise we need to query for the codes.
    if (argument.getLiteralValue() != null) {
      codings.add((Coding) argument.getLiteralValue());
    } else {
      argumentQuery = buildCodeQuery(argument, input);
    }
    // The query will be a union if we need to query for both the input and argument codes.
    String query =
        inputQuery != null && argumentQuery != null
            ? inputQuery + " UNION " + argumentQuery
            : inputQuery != null
                ? inputQuery : argumentQuery;

    // The hash used for the closure table name is based upon the concatenation of the FHIRPath
    // expressions for input and argument.
    String closureHash = Strings.md5(inputResult.getFhirPath() + argument.getFhirPath())
        .substring(0, 7);
    String closureName = "closure_" + closureHash;

    // Skip the rest if the table already exists.
    if (spark.catalog().tableExists(input.getContext().getDatabaseName(), closureName)) {
      return closureName;
    }

    // If needed, execute the query to get the codes, and collect them into a list of Coding
    // objects.
    if (inputQuery != null || argumentQuery != null) {
      List<Row> codeResults = spark.sql(query).collectAsList();
      codings.addAll(codeResults.stream()
          .map(row -> new Coding(row.getString(0), row.getString(1), null))
          .collect(Collectors.toList()));
    }

    // Execute a closure operation using the set of Codings.
    TerminologyClient terminologyClient = input.getContext().getTerminologyClient();
    terminologyClient.closure(new StringType(closureName), null, null);
    ConceptMap closure = terminologyClient.closure(new StringType(closureName), codings, null);

    // Extract the mappings from the closure result into a set of Mapping objects.
    List<ConceptMapGroupComponent> groups = closure.getGroup();
    List<Mapping> mappings = new ArrayList<>();
    if (groups.size() == 1) {
      ConceptMapGroupComponent group = groups.get(0);
      String sourceSystem = group.getSource();
      String targetSystem = group.getTarget();
      List<SourceElementComponent> elements = group.getElement();
      for (SourceElementComponent element : elements) {
        for (TargetElementComponent target : element.getTarget()) {
          Mapping mapping = new Mapping();
          mapping.setSourceSystem(sourceSystem);
          mapping.setSourceCode(element.getCode());
          mapping.setTargetSystem(targetSystem);
          mapping.setTargetCode(target.getCode());
          mappings.add(mapping);
        }
      }
    } else if (groups.size() > 1) {
      logger.warn("Encountered closure response with more than one group");
    }

    // Create a Spark Dataset containing the mappings, and make it the subject of a temporary view.
    Dataset<Mapping> mappingDataset = spark.createDataset(mappings, Encoders.bean(Mapping.class));
    mappingDataset.createOrReplaceTempView(closureName);

    return closureName;
  }

  private ParseResult validateInput(ExpressionFunctionInput input) {
    ParseResult inputResult = input.getInput();
    String inputFhirPath = inputResult.getFhirPath();
    if (inputResult.getFhirPathType() == CODING) {
      return inputResult;
    }
    String typeCode = inputResult.getPathTraversal().getElementDefinition().getTypeCode();
    if (!typeCode.equals("CodeableConcept")) {
      throw new InvalidRequestException(
          "Argument to " + functionName + " function must be Coding or CodeableConcept: "
              + inputResult
              .getFhirPath());
    } else {
      // If this is a CodeableConcept, we need to traverse to the `coding` member first.
      ExpressionFunctionInput memberInvocationInput = new ExpressionFunctionInput();
      memberInvocationInput.setContext(input.getContext());
      memberInvocationInput.setExpression("coding");
      memberInvocationInput.setInput(inputResult);
      inputResult = new MemberInvocation().invoke(memberInvocationInput);
      inputResult.setFhirPath(inputFhirPath + ".coding");
      return inputResult;
    }
  }

  private ParseResult validateArgument(ExpressionFunctionInput input) {
    ParseResult argument = input.getArguments().get(0);
    String argumentFhirPath = argument.getFhirPath();
    if (argument.getFhirPathType() == CODING) {
      return argument;
    }
    String typeCode = argument.getPathTraversal().getElementDefinition().getTypeCode();
    if (!typeCode.equals("CodeableConcept")) {
      throw new InvalidRequestException(
          "Argument to " + functionName + " function must be Coding or CodeableConcept: " + argument
              .getFhirPath());
    } else {
      // If this is a CodeableConcept, we need to traverse to the `coding` member first.
      ExpressionFunctionInput memberInvocationInput = new ExpressionFunctionInput();
      memberInvocationInput.setContext(input.getContext());
      memberInvocationInput.setExpression("coding");
      memberInvocationInput.setInput(argument);
      argument = new MemberInvocation().invoke(memberInvocationInput);
      argument.setFhirPath(argumentFhirPath + ".coding");
      return argument;
    }
  }

  @Nonnull
  private String buildCodeQuery(ParseResult result, ExpressionFunctionInput input) {
    SortedSet<Join> joins = new TreeSet<>(result.getJoins());

    // If there is a filter to be applied as part of this invocation, add in its join dependencies.
    if (input.getFilter() != null) {
      joins.addAll(input.getFilterJoins());
    }

    String query = "SELECT DISTINCT " + result.getSql() + ".system, " + result.getSql() + ".code ";
    query = rewriteSqlWithJoinAliases(query, result.getJoins());
    query += "FROM " + input.getContext().getFromTable() + " ";
    query += result.getJoins().stream().map(Join::getSql).collect(Collectors.joining(" ")) + " ";
    String nullFilter = "WHERE " + result.getSql() + ".system IS NOT NULL AND " + result.getSql()
        + ".code IS NOT NULL";
    nullFilter = rewriteSqlWithJoinAliases(nullFilter, result.getJoins());
    query += nullFilter;

    return query;
  }

}
