/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.datasource;

import au.csiro.clinsight.fhir.FhirPathBaseVisitor;
import au.csiro.clinsight.fhir.FhirPathLexer;
import au.csiro.clinsight.fhir.FhirPathParser;
import au.csiro.clinsight.fhir.FhirPathParser.FunctionInvocationContext;
import au.csiro.clinsight.fhir.FhirPathParser.InvocationExpressionContext;
import au.csiro.clinsight.fhir.FhirPathParser.MemberInvocationContext;
import au.csiro.clinsight.fhir.FhirPathParser.ParamListContext;
import au.csiro.clinsight.fhir.FhirPathParser.TermExpressionContext;
import au.csiro.clinsight.fhir.FhirPathParser.ThisInvocationContext;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collector;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * @author John Grimes
 */
public class AggregationExpressionParser {

  private static final Collector<SparkQueryPlan, SparkQueryPlan, SparkQueryPlan> queryPlanMerger =
      Collector.of(
          SparkQueryPlan::new,
          (a, b) -> {
            a.setAggregationClause(a.getAggregationClause() + b.getAggregationClause());
            a.getFromTables().addAll(b.getFromTables());
            if (b.getResultType() != null) {
              a.setResultType(b.getResultType());
            }
          },
          (a, b) -> {
            SparkQueryPlan result = new SparkQueryPlan();
            result.setAggregationClause(a.getAggregationClause() + b.getAggregationClause());
            Set<String> fromTables = new HashSet<>();
            fromTables.addAll(a.getFromTables());
            fromTables.addAll(b.getFromTables());
            result.setFromTables(fromTables);
            result.setResultType(a.getResultType());
            if (b.getResultType() != null) {
              result.setResultType(b.getResultType());
            }
            return result;
          });

  public SparkQueryPlan parse(String expression) {
    FhirPathLexer lexer = new FhirPathLexer(CharStreams.fromString(expression));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    FhirPathParser parser = new FhirPathParser(tokens);

    FunctionVisitor functionVisitor = new FunctionVisitor();
    return functionVisitor.visit(parser.expression());
  }

  private static class FunctionVisitor extends FhirPathBaseVisitor<SparkQueryPlan> {

    private static final Map<String, String> fpFuncToSpark = new HashMap<String, String>() {{
      put("count", "COUNT");
    }};

    private static final Map<String, DataType> fpFuncToDataType = new HashMap<String, DataType>() {{
      put("count", DataTypes.LongType);
    }};

    @Override
    public SparkQueryPlan visitFunctionInvocation(FunctionInvocationContext ctx) {
      String functionIdentifier = ctx.functn().identifier().getText();
      String translatedIdentifier = fpFuncToSpark.get(functionIdentifier);
      if (translatedIdentifier == null) {
        return null;
      } else {
        SparkQueryPlan queryPlan = ctx.functn().paramList().accept(new ParamListVisitor());
        queryPlan.setAggregationClause(
            translatedIdentifier + "(" + queryPlan.getAggregationClause() + ")");
        queryPlan.setResultType(fpFuncToDataType.get(functionIdentifier));
        if (queryPlan.getResultType() == null) {
          throw new AssertionError(
              "Data type not found for FHIRPath aggregation function: " + functionIdentifier);
        }
        return queryPlan;
      }
    }

  }

  private static class ParamListVisitor extends FhirPathBaseVisitor<SparkQueryPlan> {

    @Override
    public SparkQueryPlan visitParamList(ParamListContext ctx) {
      return ctx.expression().stream()
          .map(expr -> expr.accept(new ParamListVisitor()))
          .collect(queryPlanMerger);
    }

    @Override
    public SparkQueryPlan visitInvocationExpression(InvocationExpressionContext ctx) {
      SparkQueryPlan expressionQueryPlan = ctx.expression().accept(new ParamListVisitor());
      SparkQueryPlan invocationQueryPlan = ctx.invocation().accept(new ParamListVisitor());
      SparkQueryPlan result = new SparkQueryPlan();
      result.setAggregationClause(
          expressionQueryPlan.getAggregationClause() + "." + invocationQueryPlan
              .getAggregationClause());
      Set<String> fromTables = new HashSet<>();
      fromTables.addAll(expressionQueryPlan.getFromTables());
      fromTables.addAll(invocationQueryPlan.getFromTables());
      result.setFromTables(fromTables);
      return result;
    }

    @Override
    public SparkQueryPlan visitTermExpression(TermExpressionContext ctx) {
      SparkQueryPlan queryPlan = new SparkQueryPlan();
      queryPlan.setAggregationClause(ctx.getText().toLowerCase());
      queryPlan.setFromTables(new HashSet<>(Arrays.asList(queryPlan.getAggregationClause())));
      return queryPlan;
    }

    @Override
    public SparkQueryPlan visitMemberInvocation(MemberInvocationContext ctx) {
      SparkQueryPlan queryPlan = new SparkQueryPlan();
      queryPlan.setAggregationClause(ctx.getText());
      return queryPlan;
    }

    @Override
    public SparkQueryPlan visitFunctionInvocation(FunctionInvocationContext ctx) {
      SparkQueryPlan queryPlan = new SparkQueryPlan();
      queryPlan.setAggregationClause(ctx.getText());
      return queryPlan;
    }

    @Override
    public SparkQueryPlan visitThisInvocation(ThisInvocationContext ctx) {
      SparkQueryPlan queryPlan = new SparkQueryPlan();
      queryPlan.setAggregationClause(ctx.getText());
      return queryPlan;
    }
  }

}
