/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.spark;

import static au.csiro.clinsight.query.spark.Mappings.getFunction;
import static au.csiro.clinsight.utilities.Strings.pathToLowerCamelCase;
import static au.csiro.clinsight.utilities.Strings.tokenizePath;
import static au.csiro.clinsight.utilities.Strings.untokenizePath;

import au.csiro.clinsight.TerminologyClient;
import au.csiro.clinsight.fhir.ElementNotKnownException;
import au.csiro.clinsight.fhir.ElementResolver;
import au.csiro.clinsight.fhir.FhirPathBaseVisitor;
import au.csiro.clinsight.fhir.FhirPathLexer;
import au.csiro.clinsight.fhir.FhirPathParser;
import au.csiro.clinsight.fhir.FhirPathParser.AdditiveExpressionContext;
import au.csiro.clinsight.fhir.FhirPathParser.AndExpressionContext;
import au.csiro.clinsight.fhir.FhirPathParser.BooleanLiteralContext;
import au.csiro.clinsight.fhir.FhirPathParser.DateTimeLiteralContext;
import au.csiro.clinsight.fhir.FhirPathParser.EqualityExpressionContext;
import au.csiro.clinsight.fhir.FhirPathParser.ExternalConstantTermContext;
import au.csiro.clinsight.fhir.FhirPathParser.FunctionInvocationContext;
import au.csiro.clinsight.fhir.FhirPathParser.ImpliesExpressionContext;
import au.csiro.clinsight.fhir.FhirPathParser.IndexerExpressionContext;
import au.csiro.clinsight.fhir.FhirPathParser.InequalityExpressionContext;
import au.csiro.clinsight.fhir.FhirPathParser.InvocationExpressionContext;
import au.csiro.clinsight.fhir.FhirPathParser.InvocationTermContext;
import au.csiro.clinsight.fhir.FhirPathParser.LiteralTermContext;
import au.csiro.clinsight.fhir.FhirPathParser.MemberInvocationContext;
import au.csiro.clinsight.fhir.FhirPathParser.MembershipExpressionContext;
import au.csiro.clinsight.fhir.FhirPathParser.MultiplicativeExpressionContext;
import au.csiro.clinsight.fhir.FhirPathParser.NullLiteralContext;
import au.csiro.clinsight.fhir.FhirPathParser.NumberLiteralContext;
import au.csiro.clinsight.fhir.FhirPathParser.OrExpressionContext;
import au.csiro.clinsight.fhir.FhirPathParser.ParamListContext;
import au.csiro.clinsight.fhir.FhirPathParser.ParenthesizedTermContext;
import au.csiro.clinsight.fhir.FhirPathParser.PolarityExpressionContext;
import au.csiro.clinsight.fhir.FhirPathParser.QuantityLiteralContext;
import au.csiro.clinsight.fhir.FhirPathParser.StringLiteralContext;
import au.csiro.clinsight.fhir.FhirPathParser.TermExpressionContext;
import au.csiro.clinsight.fhir.FhirPathParser.ThisInvocationContext;
import au.csiro.clinsight.fhir.FhirPathParser.TimeLiteralContext;
import au.csiro.clinsight.fhir.FhirPathParser.TypeExpressionContext;
import au.csiro.clinsight.fhir.FhirPathParser.UnionExpressionContext;
import au.csiro.clinsight.fhir.MultiValueTraversal;
import au.csiro.clinsight.fhir.ResolvedElement;
import au.csiro.clinsight.fhir.ResolvedElement.ResolvedElementType;
import au.csiro.clinsight.fhir.ResourceNotKnownException;
import au.csiro.clinsight.query.spark.Join.JoinType;
import au.csiro.clinsight.query.spark.ParseResult.ParseResultType;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.spark.sql.SparkSession;

/**
 * @author John Grimes
 */
class ExpressionParser {

  private final TerminologyClient terminologyClient;
  private final SparkSession spark;

  ExpressionParser(TerminologyClient terminologyClient, SparkSession spark) {
    this.terminologyClient = terminologyClient;
    this.spark = spark;
  }

  ParseResult parse(String expression) {
    FhirPathLexer lexer = new FhirPathLexer(CharStreams.fromString(expression));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    FhirPathParser parser = new FhirPathParser(tokens);

    ExpressionVisitor expressionVisitor = new ExpressionVisitor(terminologyClient, spark);
    return expressionVisitor.visit(parser.expression());
  }

  private static class ExpressionVisitor extends FhirPathBaseVisitor<ParseResult> {

    private final TerminologyClient terminologyClient;
    private final SparkSession spark;

    ExpressionVisitor(TerminologyClient terminologyClient,
        SparkSession spark) {
      this.terminologyClient = terminologyClient;
      this.spark = spark;
    }

    @Override
    public ParseResult visitTermExpression(TermExpressionContext ctx) {
      return ctx.term().accept(new TermVisitor(terminologyClient, spark));
    }

    @Override
    public ParseResult visitInvocationExpression(InvocationExpressionContext ctx) {
      ParseResult expressionResult = new ExpressionVisitor(terminologyClient, spark)
          .visit(ctx.expression());
      return ctx.invocation()
          .accept(new InvocationVisitor(terminologyClient, spark, expressionResult));
    }

    @Override
    public ParseResult visitIndexerExpression(IndexerExpressionContext ctx) {
      throw new InvalidRequestException("Indexer operation is not supported");
    }

    @Override
    public ParseResult visitPolarityExpression(PolarityExpressionContext ctx) {
      throw new InvalidRequestException("Polarity operator is not supported");
    }

    @Override
    public ParseResult visitMultiplicativeExpression(MultiplicativeExpressionContext ctx) {
      throw new InvalidRequestException("Multiplicative expressions are not supported");
    }

    @Override
    public ParseResult visitAdditiveExpression(AdditiveExpressionContext ctx) {
      throw new InvalidRequestException("Additive expressions are not supported");
    }

    @Override
    public ParseResult visitUnionExpression(UnionExpressionContext ctx) {
      throw new InvalidRequestException("Union expressions are not supported");
    }

    @Override
    public ParseResult visitInequalityExpression(InequalityExpressionContext ctx) {
      throw new InvalidRequestException("Inequality expressions are not supported");
    }

    @Override
    public ParseResult visitTypeExpression(TypeExpressionContext ctx) {
      throw new InvalidRequestException("Type expressions are not supported");
    }

    @Override
    public ParseResult visitEqualityExpression(EqualityExpressionContext ctx) {
      throw new InvalidRequestException("Equality expressions are not supported");
    }

    @Override
    public ParseResult visitMembershipExpression(MembershipExpressionContext ctx) {
      throw new InvalidRequestException("Membership expressions are not supported");
    }

    @Override
    public ParseResult visitAndExpression(AndExpressionContext ctx) {
      throw new InvalidRequestException("And expressions are not supported");
    }

    @Override
    public ParseResult visitOrExpression(OrExpressionContext ctx) {
      throw new InvalidRequestException("Or expressions are not supported");
    }

    @Override
    public ParseResult visitImpliesExpression(ImpliesExpressionContext ctx) {
      throw new InvalidRequestException("Implies expressions are not supported");
    }

  }

  private static class TermVisitor extends FhirPathBaseVisitor<ParseResult> {

    private final TerminologyClient terminologyClient;
    private final SparkSession spark;

    TermVisitor(TerminologyClient terminologyClient, SparkSession spark) {
      this.terminologyClient = terminologyClient;
      this.spark = spark;
    }

    @Override
    public ParseResult visitInvocationTerm(InvocationTermContext ctx) {
      return new InvocationVisitor(terminologyClient, spark).visit(ctx.invocation());
    }

    @Override
    public ParseResult visitLiteralTerm(LiteralTermContext ctx) {
      return new LiteralTermVisitor().visit(ctx.literal());
    }

    @Override
    public ParseResult visitExternalConstantTerm(ExternalConstantTermContext ctx) {
      throw new InvalidRequestException("Environment variables are not supported");
    }

    @Override
    public ParseResult visitParenthesizedTerm(ParenthesizedTermContext ctx) {
      return new ExpressionVisitor(terminologyClient, spark).visit(ctx.expression());
    }

  }

  private static class InvocationVisitor extends FhirPathBaseVisitor<ParseResult> {

    TerminologyClient terminologyClient;
    SparkSession spark;
    ParseResult invoker;

    InvocationVisitor(TerminologyClient terminologyClient,
        SparkSession spark) {
      this.terminologyClient = terminologyClient;
      this.spark = spark;
    }

    InvocationVisitor(TerminologyClient terminologyClient,
        SparkSession spark, ParseResult invoker) {
      this.terminologyClient = terminologyClient;
      this.spark = spark;
      this.invoker = invoker;
    }

    static void populateJoinsFromElement(ParseResult result, ResolvedElement element) {
      // Process multi-value traversals, adding statements which explode the multiple values into
      // multiple rows and then join across to those rows.
      Join previousJoin = result.getJoins().isEmpty()
          ? null
          : result.getJoins().last();
      if (!element.getMultiValueTraversals().isEmpty()) {
        populateJoinFromMultiValueTraversal(result, previousJoin,
            element.getMultiValueTraversals().getLast());
      }

      // Rewrite the main expression (SELECT) of the parse result to make use of the table aliases
      // that were created when we processed the joins.
      if (!result.getJoins().isEmpty()) {
        Join finalJoin = result.getJoins().last();
        assert element.getType() != null;
        String updatedExpression;
        if (finalJoin.getJoinType() == JoinType.LATERAL_VIEW) {
          assert finalJoin.getUdtfExpression() != null;
          updatedExpression = result.getSqlExpression().replace(finalJoin.getUdtfExpression(),
              finalJoin.getTableAlias());
        } else {
          updatedExpression = result.getSqlExpression().replace(finalJoin.getRootExpression(),
              finalJoin.getTableAlias());
        }
        result.setSqlExpression(updatedExpression);
      }
    }

    private static void populateJoinFromMultiValueTraversal(ParseResult result, Join previousJoin,
        MultiValueTraversal multiValueTraversal) {
      // Construct an alias that can be used to refer to the generated table elsewhere in the query.
      LinkedList<String> pathComponents = tokenizePath(multiValueTraversal.getPath());
      String tableAlias = pathToLowerCamelCase(pathComponents);

      // Construct a join expression.
      pathComponents.push(pathComponents.pop().toLowerCase());
      String rootExpression = untokenizePath(pathComponents);
      String udtfExpression = rootExpression;
      String traversalType = multiValueTraversal.getTypeCode();

      // If this is not the first join, record a dependency between this join and the previous one.
      // The expression needs to be rewritten to refer to the alias of the target join.
      if (previousJoin != null) {
        udtfExpression = udtfExpression
            .replace(previousJoin.getRootExpression(), previousJoin.getTableAlias());
        tableAlias = pathToLowerCamelCase(tokenizePath(udtfExpression));
      }

      String joinExpression =
          "LATERAL VIEW explode(" + udtfExpression + ") " + tableAlias + " AS " + tableAlias;
      Join join = new Join(joinExpression, rootExpression, JoinType.LATERAL_VIEW, tableAlias);
      join.setUdtfExpression(udtfExpression);
      join.setTraversalType(traversalType);
      if (previousJoin != null) {
        join.setDependsUpon(previousJoin);
      }

      result.getJoins().add(join);
    }

    @Override
    public ParseResult visitMemberInvocation(MemberInvocationContext ctx) {
      if (invoker != null && invoker.getElementType() == ResolvedElementType.PRIMITIVE) {
        throw new InvalidRequestException("Attempt to invoke member on primitive type");
      }
      ResolvedElement element;
      String fhirPathExpression = invoker == null
          ? ctx.getText()
          : invoker.getExpression() + "." + ctx.getText();
      SortedSet<Join> joins = invoker == null
          ? new TreeSet<>()
          : invoker.getJoins();
      try {
        element = ElementResolver.resolveElement(fhirPathExpression);
      } catch (ResourceNotKnownException | ElementNotKnownException e) {
        throw new InvalidRequestException(e.getMessage());
      }
      ParseResult result = new ParseResult();
      if (element.getType() == ResolvedElementType.RESOURCE) {
        String sqlExpression = fhirPathExpression.toLowerCase();
        result.setSqlExpression(sqlExpression);
        result.setExpression(fhirPathExpression);
        result.getFromTable().add(sqlExpression);
      } else {
        assert invoker != null;
        String sqlExpression = invoker.getSqlExpression() + "." + ctx.getText();
        result.setSqlExpression(sqlExpression);
        result.setExpression(fhirPathExpression);
        result.getFromTable().addAll(invoker.getFromTable());
      }
      result.setResultType(ParseResultType.ELEMENT_PATH);
      result.setElementType(element.getType());
      result.setElementTypeCode(element.getTypeCode());
      result.getJoins().addAll(joins);
      populateJoinsFromElement(result, element);
      assert result.getElementTypeCode() != null;
      return result;
    }

    @Override
    public ParseResult visitFunctionInvocation(FunctionInvocationContext ctx) {
      // Get the function that corresponds to the function identifier.
      String functionIdentifier = ctx.functn().identifier().getText();
      ExpressionFunction function = getFunction(functionIdentifier);
      if (function == null) {
        throw new InvalidRequestException("Unrecognised function: " + functionIdentifier);
      }

      // Get the parse results for each of the expressions that make up the functions arguments.
      List<ParseResult> arguments;
      ParamListContext paramList = ctx.functn().paramList();
      arguments = paramList == null
          ? new ArrayList<>()
          : paramList.expression().stream()
              .map(expression -> new ExpressionVisitor(terminologyClient, spark).visit(expression))
              .collect(Collectors.toList());

      // Invoke the function and return the result.
      function.setTerminologyClient(terminologyClient);
      function.setSparkSession(spark);
      return function.invoke(invoker, arguments);
    }

    @Override
    public ParseResult visitThisInvocation(ThisInvocationContext ctx) {
      throw new InvalidRequestException("$this is not supported");
    }

  }

  private static class LiteralTermVisitor extends FhirPathBaseVisitor<ParseResult> {

    @Override
    public ParseResult visitNullLiteral(NullLiteralContext ctx) {
      throw new InvalidRequestException("Null literals are not supported");
    }

    @Override
    public ParseResult visitBooleanLiteral(BooleanLiteralContext ctx) {
      throw new InvalidRequestException("Boolean literals are not supported");
    }

    @Override
    public ParseResult visitStringLiteral(StringLiteralContext ctx) {
      ParseResult result = new ParseResult();
      result.setResultType(ParseResultType.STRING_LITERAL);
      result.setExpression(ctx.getText());
      return result;
    }

    @Override
    public ParseResult visitNumberLiteral(NumberLiteralContext ctx) {
      throw new InvalidRequestException("Numeric literals are not supported");
    }

    @Override
    public ParseResult visitDateTimeLiteral(DateTimeLiteralContext ctx) {
      throw new InvalidRequestException("Date/time literals are not supported");
    }

    @Override
    public ParseResult visitTimeLiteral(TimeLiteralContext ctx) {
      throw new InvalidRequestException("Time literals are not supported");
    }

    @Override
    public ParseResult visitQuantityLiteral(QuantityLiteralContext ctx) {
      throw new InvalidRequestException("Quantity literals are not supported");
    }

  }

}
