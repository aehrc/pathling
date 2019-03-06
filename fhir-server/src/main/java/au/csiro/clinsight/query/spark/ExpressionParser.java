/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.spark;

import static au.csiro.clinsight.fhir.ResourceDefinitions.isPrimitive;
import static au.csiro.clinsight.query.spark.Mappings.getFunction;
import static au.csiro.clinsight.utilities.Strings.tokenizePath;
import static au.csiro.clinsight.utilities.Strings.untokenizePath;

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
import au.csiro.clinsight.utilities.Strings;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

/**
 * @author John Grimes
 */
public class ExpressionParser {

  ParseResult parse(String expression) {
    FhirPathLexer lexer = new FhirPathLexer(CharStreams.fromString(expression));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    FhirPathParser parser = new FhirPathParser(tokens);

    ExpressionVisitor expressionVisitor = new ExpressionVisitor();
    return expressionVisitor.visit(parser.expression());
  }

  private static class ExpressionVisitor extends FhirPathBaseVisitor<ParseResult> {

    @Override
    public ParseResult visitTermExpression(TermExpressionContext ctx) {
      return ctx.term().accept(new TermVisitor());
    }

    @Override
    public ParseResult visitInvocationExpression(InvocationExpressionContext ctx) {
      ParseResult expressionResult = new ExpressionVisitor().visit(ctx.expression());
      return ctx.invocation().accept(new InvocationVisitor(expressionResult));
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

    @Override
    public ParseResult visitInvocationTerm(InvocationTermContext ctx) {
      return new InvocationVisitor().visit(ctx.invocation());
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
      return new ExpressionVisitor().visit(ctx.expression());
    }

  }

  private static class InvocationVisitor extends FhirPathBaseVisitor<ParseResult> {

    ParseResult invoker;

    InvocationVisitor() {
    }

    InvocationVisitor(ParseResult invoker) {
      this.invoker = invoker;
    }

    static void populateJoinsFromElement(ParseResult result, ResolvedElement element) {
      Join previousJoin = null;
      for (MultiValueTraversal multiValueTraversal : element.getMultiValueTraversals()) {
        // Get the components of the path within the traversal.
        LinkedList<String> pathComponents = tokenizePath(multiValueTraversal.getPath());
        // Make the first component all lowercase.
        pathComponents.push(pathComponents.pop().toLowerCase());

        // Construct an alias that can be used to refer to the generated table elsewhere in the query.
        List<String> aliasComponents = pathComponents.subList(1, pathComponents.size());
        List<String> aliasTail = aliasComponents.subList(1, aliasComponents.size()).stream()
            .map(Strings::capitalize).collect(Collectors.toCollection(LinkedList::new));
        String tableAlias = String.join("", aliasComponents.get(0), String.join("", aliasTail));

        // Construct a join expression.
        String udtfExpression = untokenizePath(pathComponents);
        String traversalType = multiValueTraversal.getTypeCode();
        String joinExpression;
        String columnAlias;
        // If the element is primitive, we will need explode. If it is a complex type, we will need
        // inline.
        if (isPrimitive(traversalType)) {
          columnAlias = pathComponents.getLast();
          joinExpression =
              "LATERAL VIEW OUTER explode(" + udtfExpression + ") " + tableAlias + " AS "
                  + columnAlias;
        } else {
          columnAlias = String.join(", ", multiValueTraversal.getChildren());
          joinExpression =
              "LATERAL VIEW OUTER inline(" + udtfExpression + ") " + tableAlias + " AS "
                  + columnAlias;
        }
        Join join = new Join(joinExpression, tableAlias, columnAlias);
        join.setUdtfExpression(udtfExpression);
        join.setTypeCode(traversalType);

        // If this is not the first join, record a dependency between this join and the previous one.
        // The expression needs to be rewritten to refer to the alias of the target join.
        if (previousJoin != null) {
          join.setDependsUpon(previousJoin);
          assert previousJoin.getUdtfExpression() != null;
          String updatedExpression = join.getExpression()
              .replace(previousJoin.getUdtfExpression(), previousJoin.getTableAlias());
          String updatedUdtfExpression = join.getUdtfExpression()
              .replace(previousJoin.getUdtfExpression(), previousJoin.getTableAlias());
          join.setExpression(updatedExpression);
          join.setUdtfExpression(updatedUdtfExpression);
        }

        result.getJoins().add(join);
        previousJoin = join;
      }

      // Rewrite the main expression (SELECT) of the parse result to make use of the table aliases
      // that were created when we processed the joins.
      if (!result.getJoins().isEmpty()) {
        Join finalJoin = result.getJoins().last();
        String updatedExpression;
        assert finalJoin.getUdtfExpression() != null;
        assert element.getType() != null;
        if (element.getType() == ResolvedElementType.PRIMITIVE) {
          updatedExpression = result.getSqlExpression().replace(finalJoin.getUdtfExpression(),
              finalJoin.getTableAlias() + "." + finalJoin.getColumnAlias());
        } else {
          updatedExpression = result.getSqlExpression()
              .replace(finalJoin.getUdtfExpression(), finalJoin.getTableAlias());
        }
        result.setSqlExpression(updatedExpression);
      }
    }

    @Override
    public ParseResult visitMemberInvocation(MemberInvocationContext ctx) {
      if (invoker != null && invoker.getResultType() == ResolvedElementType.PRIMITIVE) {
        throw new InvalidRequestException("Attempt to invoke member on primitive type");
      }
      ResolvedElement element;
      String fhirPathExpression = invoker == null
          ? ctx.getText()
          : invoker.getFhirPathExpression() + "." + ctx.getText();
      try {
        element = ElementResolver.resolveElement(fhirPathExpression);
      } catch (ResourceNotKnownException | ElementNotKnownException e) {
        throw new InvalidRequestException(e.getMessage());
      }
      ParseResult result = new ParseResult();
      if (element.getType() == ResolvedElementType.RESOURCE) {
        String sqlExpression = fhirPathExpression.toLowerCase();
        result.setSqlExpression(sqlExpression);
        result.setFhirPathExpression(fhirPathExpression);
        result.getFromTable().add(sqlExpression);
      } else {
        assert invoker != null;
        String sqlExpression = invoker.getSqlExpression() + "." + ctx.getText();
        result.setSqlExpression(sqlExpression);
        result.setFhirPathExpression(fhirPathExpression);
        result.getFromTable().addAll(invoker.getFromTable());
      }
      result.setResultType(element.getType());
      result.setResultTypeCode(element.getTypeCode());
      populateJoinsFromElement(result, element);
      assert result.getResultTypeCode() != null;
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
              .map(expression -> new ExpressionVisitor().visit(expression))
              .collect(Collectors.toList());

      // Invoke the function and return the result.
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
      throw new InvalidRequestException("String literals are not supported");
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
