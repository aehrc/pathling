/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query;

import au.csiro.clinsight.fhir.FhirPathBaseVisitor;
import au.csiro.clinsight.fhir.FhirPathLexer;
import au.csiro.clinsight.fhir.FhirPathParser;
import au.csiro.clinsight.fhir.FhirPathParser.FunctionInvocationContext;
import au.csiro.clinsight.fhir.FhirPathParser.InvocationExpressionContext;
import au.csiro.clinsight.fhir.FhirPathParser.MemberInvocationContext;
import au.csiro.clinsight.fhir.FhirPathParser.TermExpressionContext;
import au.csiro.clinsight.fhir.FhirPathParser.ThisInvocationContext;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

/**
 * @author John Grimes
 */
public class SparkGroupingParser {

  public SparkGroupingParseResult parse(String expression) {
    FhirPathLexer lexer = new FhirPathLexer(CharStreams.fromString(expression));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    FhirPathParser parser = new FhirPathParser(tokens);

    ExpressionVisitor expressionVisitor = new ExpressionVisitor();
    ExpressionVisitor.ParseResult parseResult = expressionVisitor.visit(parser.expression());
    SparkGroupingParseResult result = new SparkGroupingParseResult();
    result.setExpression(parseResult.expression);
    result.setFromTable(parseResult.fromTable);
    return result;
  }

  private static class ExpressionVisitor extends
      FhirPathBaseVisitor<ExpressionVisitor.ParseResult> {

    @Override
    public ParseResult visitInvocationExpression(InvocationExpressionContext ctx) {
      ParseResult expressionResult = ctx.expression().accept(new ExpressionVisitor());
      ParseResult invocationResult = ctx.invocation().accept(new ExpressionVisitor());
      ParseResult result = new ParseResult();
      result.expression = expressionResult.expression + "." + invocationResult.expression;
      result.fromTable = expressionResult.fromTable;
      return result;
    }

    @Override
    public ParseResult visitTermExpression(TermExpressionContext ctx) {
      ParseResult result = new ParseResult(ctx.getText().toLowerCase());
      result.fromTable = result.expression;
      return result;
    }

    @Override
    public ParseResult visitMemberInvocation(MemberInvocationContext ctx) {
      return new ParseResult(ctx.getText());
    }

    @Override
    public ParseResult visitFunctionInvocation(FunctionInvocationContext ctx) {
      return new ParseResult(ctx.getText());
    }

    @Override
    public ParseResult visitThisInvocation(ThisInvocationContext ctx) {
      return new ParseResult(ctx.getText());
    }

    private static class ParseResult {

      private String expression;
      private String fromTable;

      ParseResult() {
      }

      ParseResult(String expression) {
        this.expression = expression;
      }
    }

  }

}
