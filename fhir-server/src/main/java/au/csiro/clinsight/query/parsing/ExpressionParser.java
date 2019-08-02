/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.parsing;

import static au.csiro.clinsight.query.functions.ExpressionFunction.getFunction;
import static au.csiro.clinsight.query.parsing.ParseResult.FhirPathType.CODING;

import au.csiro.clinsight.fhir.FhirPathBaseVisitor;
import au.csiro.clinsight.fhir.FhirPathLexer;
import au.csiro.clinsight.fhir.FhirPathParser;
import au.csiro.clinsight.fhir.FhirPathParser.*;
import au.csiro.clinsight.fhir.definitions.PathResolver;
import au.csiro.clinsight.fhir.definitions.exceptions.ElementNotKnownException;
import au.csiro.clinsight.fhir.definitions.exceptions.ResourceNotKnownException;
import au.csiro.clinsight.query.functions.ExpressionFunction;
import au.csiro.clinsight.query.functions.ExpressionFunctionInput;
import au.csiro.clinsight.query.functions.MemberInvocation;
import au.csiro.clinsight.query.functions.MembershipExpression;
import au.csiro.clinsight.query.parsing.ParseResult.FhirPathType;
import au.csiro.clinsight.query.parsing.ParseResult.FhirType;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.hl7.fhir.dstu3.model.Coding;

/**
 * This is an ANTLR-based parser for processing a FHIRPath expression, and aggregating the results
 * into a ParseResult object.
 *
 * @author John Grimes
 */
public class ExpressionParser {

  private final ExpressionParserContext context;

  public ExpressionParser(ExpressionParserContext context) {
    this.context = context;
  }

  public ParseResult parse(String expression) {
    FhirPathLexer lexer = new FhirPathLexer(CharStreams.fromString(expression));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    FhirPathParser parser = new FhirPathParser(tokens);

    ExpressionVisitor expressionVisitor = new ExpressionVisitor(context);
    return expressionVisitor.visit(parser.expression());
  }

  /**
   * This class processes all types of expressions, and delegates the special handling of supported
   * types to the more specific visitor classes.
   */
  private static class ExpressionVisitor extends FhirPathBaseVisitor<ParseResult> {

    final ExpressionParserContext context;

    ExpressionVisitor(ExpressionParserContext context) {
      this.context = context;
    }

    /**
     * A term is typically a standalone literal or function invocation.
     */
    @Override
    public ParseResult visitTermExpression(TermExpressionContext ctx) {
      return ctx.term().accept(new TermVisitor(context));
    }

    /**
     * An invocation expression is one expression invoking another using the dot notation.
     */
    @Override
    public ParseResult visitInvocationExpression(InvocationExpressionContext ctx) {
      ParseResult expressionResult = new ExpressionVisitor(context)
          .visit(ctx.expression());
      // The invoking expression is passed through to the invocation visitor's constructor - this
      // will provide it with extra context required to do things like merging in joins from the
      // upstream path.
      ParseResult invocationResult = ctx.invocation()
          .accept(new InvocationVisitor(context, expressionResult));
      invocationResult
          .setFhirPath(expressionResult.getFhirPath() + "." + invocationResult.getFhirPath());
      return invocationResult;
    }

    @Nonnull
    private ParseResult parseBooleanExpression(ExpressionContext ctx,
        ExpressionContext leftExpression, ExpressionContext rightExpression,
        String operatorString) {
      ParseResult leftResult = new ExpressionVisitor(context)
          .visit(leftExpression);
      ParseResult rightResult = new ExpressionVisitor(context)
          .visit(rightExpression);
      ParseResult result = new ParseResult();
      result.setFhirPath(
          leftResult.getFhirPath() + " " + operatorString + " " + rightResult.getFhirPath());
      result.setSql(
          leftResult.getSql() + " " + operatorString + " " + rightResult
              .getSql());
      result.setFhirPathType(FhirPathType.BOOLEAN);
      result.setFhirType(FhirType.BOOLEAN);
      result.getJoins().addAll(leftResult.getJoins());
      result.getJoins().addAll(rightResult.getJoins());
      result.setPrimitive(true);
      return result;
    }

    @Override
    public ParseResult visitEqualityExpression(EqualityExpressionContext ctx) {
      ParseResult leftResult = new ExpressionVisitor(context)
          .visit(ctx.expression(0));
      ParseResult rightResult = new ExpressionVisitor(context)
          .visit(ctx.expression(1));

      // Check that both operands are singular.
      // if (!leftResult.isSingular()) {
      //   throw new InvalidRequestException(
      //       "Equality operator does not support operand that is not singular: " + leftResult
      //           .getFhirPath());
      // } else if (!rightResult.isSingular()) {
      //   throw new InvalidRequestException(
      //       "Equality operator does not support operand that is not singular: " + rightResult
      //           .getFhirPath());
      // }

      // Check that both operands are primitive.
      // if (!leftResult.isPrimitive()) {
      //   throw new InvalidRequestException(
      //       "Equality operator does not support operand that is not primitive: " + leftResult
      //           .getFhirPath());
      // } else if (!rightResult.isPrimitive()) {
      //   throw new InvalidRequestException(
      //       "Equality operator does not support operand that is not primitive: " + rightResult
      //           .getFhirPath());
      // }

      ParseResult result = new ParseResult();
      result.setFhirPath(leftResult.getFhirPath() + " = " + rightResult.getFhirPath());
      result.setSql(
          leftResult.getSql() + " = " + rightResult
              .getSql());
      result.setFhirPathType(FhirPathType.BOOLEAN);
      result.setFhirType(FhirType.BOOLEAN);
      result.getJoins().addAll(leftResult.getJoins());
      result.getJoins().addAll(rightResult.getJoins());
      result.setPrimitive(true);
      return result;
    }

    @Override
    public ParseResult visitInequalityExpression(InequalityExpressionContext ctx) {
      return parseBooleanExpression(ctx, ctx.expression(0), ctx.expression(1),
          ctx.children.get(1).toString());
    }

    @Override
    public ParseResult visitAndExpression(AndExpressionContext ctx) {
      return parseBooleanExpression(ctx, ctx.expression(0), ctx.expression(1), "AND");
    }

    @Override
    public ParseResult visitOrExpression(OrExpressionContext ctx) {
      return parseBooleanExpression(ctx, ctx.expression(0), ctx.expression(1), "OR");
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
    public ParseResult visitTypeExpression(TypeExpressionContext ctx) {
      throw new InvalidRequestException("Type expressions are not supported");
    }

    @Override
    public ParseResult visitMembershipExpression(MembershipExpressionContext ctx) {
      String operator = ctx.children.get(1).getText();
      MembershipExpression membershipExpression = new MembershipExpression();
      ParseResult leftResult = new ExpressionVisitor(context)
          .visit(ctx.expression(operator.equals("in") ? 0 : 1));
      ParseResult rightResult = new ExpressionVisitor(context)
          .visit(ctx.expression(operator.equals("in") ? 1 : 0));
      ExpressionFunctionInput membershipExpressionInput = new ExpressionFunctionInput();
      membershipExpressionInput.setContext(context);
      if (operator.equals("in")) {
        membershipExpressionInput
            .setExpression(leftResult.getFhirPath() + " in " + rightResult.getFhirPath());
      } else {
        membershipExpressionInput
            .setExpression(rightResult.getFhirPath() + " contains " + leftResult.getFhirPath());
      }
      membershipExpressionInput.setInput(leftResult);
      membershipExpressionInput.getArguments().add(rightResult);
      return membershipExpression.invoke(membershipExpressionInput);
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

    final ExpressionParserContext context;

    TermVisitor(ExpressionParserContext context) {
      this.context = context;
    }

    /**
     * This passes a standalone function invocation along to the invocation visitor. Note that most
     * functions will require an input, and will fail validation later on in this instance.
     */
    @Override
    public ParseResult visitInvocationTerm(InvocationTermContext ctx) {
      return new InvocationVisitor(context).visit(ctx.invocation());
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
      if (ctx.getText().equals("%resource") || ctx.getText().equals("%context")) {
        ParseResult result = context.getSubjectResource();
        result.setFhirPath(ctx.getText());
        return result;
      } else {
        throw new InvalidRequestException("Unrecognised environment variable: " + ctx.getText());
      }
    }

    /**
     * Parentheses are ignored in the standalone term case.
     */
    @Override
    public ParseResult visitParenthesizedTerm(ParenthesizedTermContext ctx) {
      final ParseResult result = new ExpressionVisitor(context)
          .visit(ctx.expression());
      result.setFhirPath("(" + result.getSql() + ")");
      result.setSql("(" + result.getSql() + ")");
      return result;
    }

  }

  /**
   * This class is invoked on the right-hand side of the invocation expression, and can optionally
   * be constructed with an invoker expression to allow it to operate with knowledge of this
   * context.
   */
  private static class InvocationVisitor extends FhirPathBaseVisitor<ParseResult> {

    final ExpressionParserContext context;
    ParseResult invoker;

    InvocationVisitor(ExpressionParserContext context) {
      this.context = context;
    }

    InvocationVisitor(ExpressionParserContext context,
        ParseResult invoker) {
      this.context = context;
      this.invoker = invoker;
    }

    /**
     * This method gets called when an element is on the right-hand side of the invocation
     * expression, or when an identifier is referred to as a term (e.g. Encounter).
     */
    @Override
    public ParseResult visitMemberInvocation(MemberInvocationContext ctx) {
      if (invoker == null) {
        // If there is no invoker, we assume that this is a resource. If we can't resolve it, an
        // error will be thrown.
        ParseResult result = new ParseResult();
        result.setFhirPath(ctx.getText());
        try {
          result.setPathTraversal(PathResolver.resolvePath(result.getFhirPath()));
        } catch (ResourceNotKnownException | ElementNotKnownException e) {
          throw new InvalidRequestException(e.getMessage());
        }
        result.setSql(ctx.getText().toLowerCase());
        return result;

      } else {
        // If there is an invoker, this must be a path expression to an element, with a resource or
        // parent element as the input. We have a class called `MemberInvocation` to encapsulate
        // the logic required for these traversals.
        ExpressionFunctionInput memberInvocationInput = new ExpressionFunctionInput();
        memberInvocationInput.setContext(context);
        memberInvocationInput.setExpression(ctx.getText());
        memberInvocationInput.setInput(invoker);
        return new MemberInvocation().invoke(memberInvocationInput);
      }
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
      ExpressionFunctionInput functionInput = new ExpressionFunctionInput();
      if (function == null) {
        throw new InvalidRequestException("Unrecognised function: " + functionIdentifier);
      }

      // Get the parse results for each of the expressions that make up the functions arguments.
      List<ParseResult> arguments;
      ParamListContext paramList = ctx.functn().paramList();
      if (paramList == null) {
        arguments = new ArrayList<>();
      } else {
        // Create a new ExpressionParserContext, which includes information about how to evaluate
        // the `$this` expression.
        ExpressionParserContext argumentContext = new ExpressionParserContext(context);
        if (invoker != null) {
          ParseResult thisResult = new ParseResult(invoker);
          thisResult.setFhirPath("$this");
          argumentContext.setThisExpression(thisResult);
        }
        // Parse each of the expressions passed as arguments to the function.
        arguments = paramList.expression().stream()
            .map(expression -> new ExpressionVisitor(argumentContext).visit(expression))
            .collect(Collectors.toList());
      }

      // Invoke the function and return the result.
      functionInput.setContext(context);
      String fhirPath =
          functionIdentifier + "(" + arguments.stream().map(ParseResult::getFhirPath).collect(
              Collectors.joining(", ")) + ")";
      functionInput.setExpression(fhirPath);
      functionInput.setInput(invoker);
      functionInput.getArguments().addAll(arguments);

      ParseResult result = function.invoke(functionInput);
      result.setFhirPath(fhirPath);
      return result;
    }

    @Override
    public ParseResult visitThisInvocation(ThisInvocationContext ctx) {
      if (context.getThisExpression() == null) {
        throw new InvalidRequestException(
            "$this can only be used within the context of arguments to a function");
      }
      return context.getThisExpression();
    }

  }

  private static class LiteralTermVisitor extends FhirPathBaseVisitor<ParseResult> {

    @Override
    public ParseResult visitCodingLiteral(CodingLiteralContext ctx) {
      ParseResult result = new ParseResult();
      result.setFhirPathType(CODING);
      result.setFhirPath(ctx.getText());
      LinkedList<String> codingTokens = new LinkedList<>(Arrays.asList(ctx.getText().split("\\|")));
      Coding literalValue;
      if (codingTokens.size() == 2) {
        literalValue = new Coding(codingTokens.get(0), codingTokens.get(1), null);
      } else if (codingTokens.size() == 3) {
        literalValue = new Coding(codingTokens.get(0), codingTokens.get(2), null);
        literalValue.setVersion(codingTokens.get(1));
      } else {
        throw new InvalidRequestException(
            "Coding literal must be of form [system]|[code] or [system]|[version]|[code]");
      }
      result.setLiteralValue(literalValue);
      result.setSingular(true);
      return result;
    }

    @Override
    public ParseResult visitStringLiteral(StringLiteralContext ctx) {
      ParseResult result = new ParseResult();
      result.setFhirPathType(FhirPathType.STRING);
      result.setFhirType(FhirType.STRING);
      result.setFhirPath(ctx.getText());
      result.setSql(ctx.getText());
      result.setPrimitive(true);
      result.setSingular(true);
      return result;
    }

    @Override
    public ParseResult visitDateTimeLiteral(DateTimeLiteralContext ctx) {
      ParseResult result = new ParseResult();
      result.setFhirPathType(FhirPathType.DATE_TIME);
      result.setFhirType(FhirType.DATE_TIME);
      result.setFhirPath(ctx.getText());
      result.setSql("'" + ctx.getText().replace("@", "") + "'");
      result.setPrimitive(true);
      result.setSingular(true);
      return result;
    }

    @Override
    public ParseResult visitNumberLiteral(NumberLiteralContext ctx) {
      ParseResult result = new ParseResult();
      result.setFhirPathType(FhirPathType.INTEGER);
      result.setFhirType(FhirType.INTEGER);
      result.setFhirPath(ctx.getText());
      result.setSql(ctx.getText());
      result.setPrimitive(true);
      result.setSingular(true);
      return result;
    }

    @Override
    public ParseResult visitBooleanLiteral(BooleanLiteralContext ctx) {
      ParseResult result = new ParseResult();
      result.setFhirPathType(FhirPathType.BOOLEAN);
      result.setFhirType(FhirType.BOOLEAN);
      result.setFhirPath(ctx.getText());
      result.setSql(ctx.getText().toUpperCase());
      result.setPrimitive(true);
      result.setSingular(true);
      return result;
    }

    @Override
    public ParseResult visitNullLiteral(NullLiteralContext ctx) {
      throw new InvalidRequestException("Null literals are not supported");
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
