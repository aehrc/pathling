/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.spark;

import static au.csiro.clinsight.fhir.ResourceDefinitions.getBaseResource;

import au.csiro.clinsight.fhir.FhirPathBaseVisitor;
import au.csiro.clinsight.fhir.FhirPathLexer;
import au.csiro.clinsight.fhir.FhirPathParser;
import au.csiro.clinsight.fhir.FhirPathParser.ExpressionContext;
import au.csiro.clinsight.fhir.FhirPathParser.FunctionInvocationContext;
import au.csiro.clinsight.fhir.FhirPathParser.InvocationExpressionContext;
import au.csiro.clinsight.fhir.FhirPathParser.MemberInvocationContext;
import au.csiro.clinsight.fhir.FhirPathParser.ParamListContext;
import au.csiro.clinsight.fhir.FhirPathParser.TermExpressionContext;
import au.csiro.clinsight.fhir.FhirPathParser.ThisInvocationContext;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.HashMap;
import java.util.Map;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author John Grimes
 */
class AggregationParser {

  private static final Logger logger = LoggerFactory.getLogger(AggregationParser.class);

  AggregationParseResult parse(String expression) {
    FhirPathLexer lexer = new FhirPathLexer(CharStreams.fromString(expression));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    FhirPathParser parser = new FhirPathParser(tokens);

    ExpressionVisitor expressionVisitor = new ExpressionVisitor();
    return expressionVisitor.visit(parser.expression());
  }

  private static class ExpressionVisitor extends FhirPathBaseVisitor<AggregationParseResult> {

    private static final Map<String, String> fpFuncToSpark = new HashMap<String, String>() {{
      put("count", "COUNT");
    }};

    private static final Map<String, DataType> fpFuncToDataType = new HashMap<String, DataType>() {{
      put("count", DataTypes.LongType);
    }};

    @Override
    public AggregationParseResult visitFunctionInvocation(FunctionInvocationContext ctx) {
      String functionIdentifier = ctx.functn().identifier().getText();
      String translatedIdentifier = fpFuncToSpark.get(functionIdentifier);
      if (translatedIdentifier == null) {
        logger.warn("Unrecognised function identifier encountered: " + functionIdentifier);
        return null;
      } else {
        AggregationParseResult result = new AggregationParseResult();
        ParamListVisitor.ParseResult paramListResult = ctx.functn().paramList()
            .accept(new ParamListVisitor());
        result.setExpression(translatedIdentifier + "(" + paramListResult.expression + ")");
        result.setResultType(fpFuncToDataType.get(functionIdentifier));
        if (result.getResultType() == null) {
          throw new AssertionError(
              "Data type not found for FHIRPath aggregation function: " + functionIdentifier);
        }
        result.setFromTable(paramListResult.fromTable);
        return result;
      }
    }

  }

  private static class ParamListVisitor extends FhirPathBaseVisitor<ParamListVisitor.ParseResult> {

    @Override
    public ParseResult visitParamList(ParamListContext ctx) {
      ParseResult parseResult = new ParseResult("");
      for (ExpressionContext expression : ctx.expression()) {
        ParseResult expressionResult = expression.accept(new ParamListVisitor());
        if (parseResult.expression.length() > 0 && expressionResult.expression.length() > 0) {
          parseResult.expression += ", " + expressionResult.expression;
        } else {
          parseResult.expression = expressionResult.expression;
        }
        if (parseResult.fromTable != null &&
            !parseResult.fromTable.equals(expressionResult.fromTable)) {
          throw new IllegalArgumentException(
              "Arguments to aggregate function are from different resources");
        }
        parseResult.fromTable = expressionResult.fromTable;
      }
      return parseResult;
    }

    @Override
    public ParseResult visitInvocationExpression(InvocationExpressionContext ctx) {
      ParseResult expressionResult = ctx.expression().accept(new ParamListVisitor());
      ParseResult invocationResult = ctx.invocation().accept(new ParamListVisitor());
      ParseResult result = new ParseResult();
      result.expression = expressionResult.expression + "." + invocationResult.expression;
      result.fromTable = expressionResult.fromTable;
      return result;
    }

    @Override
    public ParseResult visitTermExpression(TermExpressionContext ctx) {
      String resourceIdentifier = ctx.getText();
      if (getBaseResource(resourceIdentifier) == null) {
        throw new InvalidRequestException("Resource identifier not known: " + resourceIdentifier);
      }
      ParseResult result = new ParseResult(resourceIdentifier.toLowerCase());
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
