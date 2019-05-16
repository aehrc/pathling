/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.parsing;

import static au.csiro.clinsight.fhir.definitions.ResolvedElement.ResolvedElementType.PRIMITIVE;
import static au.csiro.clinsight.fhir.definitions.ResolvedElement.ResolvedElementType.RESOURCE;
import static au.csiro.clinsight.query.Mappings.getFunction;
import static au.csiro.clinsight.query.parsing.Join.JoinType.LATERAL_VIEW;
import static au.csiro.clinsight.query.parsing.ParseResult.ParseResultType.BOOLEAN;
import static au.csiro.clinsight.query.parsing.ParseResult.ParseResultType.COLLECTION;
import static au.csiro.clinsight.query.parsing.ParseResult.ParseResultType.STRING;
import static au.csiro.clinsight.utilities.Strings.pathToLowerCamelCase;
import static au.csiro.clinsight.utilities.Strings.tokenizePath;
import static au.csiro.clinsight.utilities.Strings.untokenizePath;

import au.csiro.clinsight.TerminologyClient;
import au.csiro.clinsight.fhir.FhirPathBaseVisitor;
import au.csiro.clinsight.fhir.FhirPathLexer;
import au.csiro.clinsight.fhir.FhirPathParser;
import au.csiro.clinsight.fhir.FhirPathParser.*;
import au.csiro.clinsight.fhir.definitions.*;
import au.csiro.clinsight.query.functions.ExpressionFunction;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.*;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.spark.sql.SparkSession;

/**
 * This is an ANTLR-based parser for processing a FHIRPath expression, and aggregating the results
 * into a ParseResult object.
 *
 * @author John Grimes
 */
public class ExpressionParser {

  private final TerminologyClient terminologyClient;
  private final SparkSession spark;

  public ExpressionParser(TerminologyClient terminologyClient, SparkSession spark) {
    this.terminologyClient = terminologyClient;
    this.spark = spark;
  }

  public ParseResult parse(String expression) {
    FhirPathLexer lexer = new FhirPathLexer(CharStreams.fromString(expression));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    FhirPathParser parser = new FhirPathParser(tokens);

    ExpressionVisitor expressionVisitor = new ExpressionVisitor(terminologyClient, spark);
    return expressionVisitor.visit(parser.expression());
  }

  /**
   * This class processes all types of expressions, and delegates the special handling of supported
   * types to the more specific visitor classes.
   */
  private static class ExpressionVisitor extends FhirPathBaseVisitor<ParseResult> {

    private final TerminologyClient terminologyClient;
    private final SparkSession spark;

    ExpressionVisitor(TerminologyClient terminologyClient,
        SparkSession spark) {
      this.terminologyClient = terminologyClient;
      this.spark = spark;
    }

    /**
     * A term is typically a standalone literal or function invocation.
     */
    @Override
    public ParseResult visitTermExpression(TermExpressionContext ctx) {
      return ctx.term().accept(new TermVisitor(terminologyClient, spark));
    }

    /**
     * An invocation expression is one expression invoking another using the dot notation.
     */
    @Override
    public ParseResult visitInvocationExpression(InvocationExpressionContext ctx) {
      ParseResult expressionResult = new ExpressionVisitor(terminologyClient, spark)
          .visit(ctx.expression());
      // The invoking expression is passed through to the invocation visitor's constructor - this
      // will provide it with extra context required to do things like merging in joins from the
      // upstream path.
      return ctx.invocation()
          .accept(new InvocationVisitor(terminologyClient, spark, expressionResult));
    }

    // All other FHIRPath constructs are currently unsupported.

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
      ParseResult leftExpression = new ExpressionVisitor(terminologyClient, spark)
          .visit(ctx.expression(0));
      ParseResult rightExpression = new ExpressionVisitor(terminologyClient, spark)
          .visit(ctx.expression(1));
      leftExpression.setExpression(ctx.getText());
      leftExpression.setSqlExpression(
          leftExpression.getSqlExpression() + " = " + rightExpression.getSqlExpression());
      leftExpression.setResultType(BOOLEAN);
      leftExpression.getJoins().addAll(rightExpression.getJoins());
      leftExpression.getFromTables().addAll(rightExpression.getFromTables());
      return leftExpression;
    }

    @Override
    public ParseResult visitMembershipExpression(MembershipExpressionContext ctx) {
      throw new InvalidRequestException("Membership expressions are not supported");
    }

    @Override
    public ParseResult visitAndExpression(AndExpressionContext ctx) {
      ParseResult leftExpression = new ExpressionVisitor(terminologyClient, spark)
          .visit(ctx.expression(0));
      ParseResult rightExpression = new ExpressionVisitor(terminologyClient, spark)
          .visit(ctx.expression(1));
      leftExpression.setExpression(ctx.getText());
      leftExpression.setSqlExpression(
          leftExpression.getSqlExpression() + " AND " + rightExpression.getSqlExpression());
      leftExpression.setResultType(BOOLEAN);
      leftExpression.getJoins().addAll(rightExpression.getJoins());
      leftExpression.getFromTables().addAll(rightExpression.getFromTables());
      return leftExpression;
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

  /**
   * A term is typically a standalone literal or function invocation.
   */
  private static class TermVisitor extends FhirPathBaseVisitor<ParseResult> {

    private final TerminologyClient terminologyClient;
    private final SparkSession spark;

    TermVisitor(TerminologyClient terminologyClient, SparkSession spark) {
      this.terminologyClient = terminologyClient;
      this.spark = spark;
    }

    /**
     * This passes a standalone function invocation along to the invocation visitor. Note that most
     * functions will require an input, and will fail validation later on in this instance.
     */
    @Override
    public ParseResult visitInvocationTerm(InvocationTermContext ctx) {
      return new InvocationVisitor(terminologyClient, spark).visit(ctx.invocation());
    }

    /**
     * We pass literals as is through to the literal visitor.
     */
    @Override
    public ParseResult visitLiteralTerm(LiteralTermContext ctx) {
      return new LiteralTermVisitor().visit(ctx.literal());
    }

    @Override
    public ParseResult visitExternalConstantTerm(ExternalConstantTermContext ctx) {
      throw new InvalidRequestException("Environment variables are not supported");
    }

    /**
     * Parentheses are ignored in the standalone term case.
     */
    @Override
    public ParseResult visitParenthesizedTerm(ParenthesizedTermContext ctx) {
      return new ExpressionVisitor(terminologyClient, spark).visit(ctx.expression());
    }

  }

  /**
   * This class is invoked on the right-hand side of the invocation expression, and can optionally
   * be constructed with an invoker expression to allow it to operate with knowledge of this
   * context.
   */
  private static class InvocationVisitor extends FhirPathBaseVisitor<ParseResult> {

    final TerminologyClient terminologyClient;
    final SparkSession spark;
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

    /**
     * Processes multi-value traversals within an element definition, adding joins to the
     * ParseResult along the way.
     */
    static void populateJoinsFromElement(ParseResult result, ResolvedElement element) {
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
        if (finalJoin.getJoinType() == LATERAL_VIEW) {
          assert finalJoin.getUdtfExpression() != null;
          assert result.getSqlExpression() != null;
          updatedExpression = result.getSqlExpression().replace(finalJoin.getUdtfExpression(),
              finalJoin.getTableAlias());
        } else {
          assert result.getSqlExpression() != null;
          updatedExpression = result.getSqlExpression().replace(finalJoin.getRootExpression(),
              finalJoin.getTableAlias());
        }
        result.setSqlExpression(updatedExpression);
      }
    }

    /**
     * Populate an individual join into a ParseResult, based on an individual MultiValueTraversal.
     */
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
          "LATERAL VIEW OUTER explode(" + udtfExpression + ") " + tableAlias + " AS " + tableAlias;
      Join join = new Join(joinExpression, rootExpression, LATERAL_VIEW, tableAlias);
      join.setUdtfExpression(udtfExpression);
      join.setTraversalType(traversalType);
      if (previousJoin != null) {
        join.setDependsUpon(previousJoin);
      }

      result.getJoins().add(join);
    }

    /**
     * This method gets called when an element is on the right-hand side of the invocation
     * expression.
     */
    @Override
    public ParseResult visitMemberInvocation(MemberInvocationContext ctx) {
      if (invoker != null && invoker.getElementType() == PRIMITIVE) {
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
      if (element.getType() == RESOURCE) {
        String sqlExpression = fhirPathExpression.toLowerCase();
        result.setSqlExpression(sqlExpression);
        result.setExpression(fhirPathExpression);
        result.getFromTables().add(sqlExpression);
      } else {
        assert invoker != null;
        String sqlExpression = invoker.getSqlExpression() + "." + ctx.getText();
        result.setSqlExpression(sqlExpression);
        result.setExpression(fhirPathExpression);
        result.getFromTables().addAll(invoker.getFromTables());
      }
      result.setResultType(COLLECTION);
      result.setElementType(element.getType());
      result.setElementTypeCode(element.getTypeCode());
      result.getJoins().addAll(joins);
      populateJoinsFromElement(result, element);
      assert result.getElementTypeCode() != null;
      return result;
    }

    /**
     * This method gets called when a function call is on the right-hand side of an invocation
     * expression. It basically just checks that the function is known, and invokes it to get a new
     * ParseResult.
     */
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

    /**
     * String literals are supported, so that they can be used in arguments, e.g. the `inValueSet`
     * function.
     */
    @Override
    public ParseResult visitStringLiteral(StringLiteralContext ctx) {
      ParseResult result = new ParseResult();
      result.setResultType(STRING);
      result.setExpression(ctx.getText());
      result.setSqlExpression(ctx.getText());
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
