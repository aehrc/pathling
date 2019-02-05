/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.spark;

import au.csiro.clinsight.fhir.FhirPathLexer;
import au.csiro.clinsight.fhir.FhirPathParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

/**
 * Parses a FHIRPath grouping expression, and returns an object which contains a Spark SQL grouping
 * expression, and the names of the tables that will need to be included within the FROM clause.
 *
 * @author John Grimes
 */
class GroupingParser {

  ParseResult parse(String expression) {
    FhirPathLexer lexer = new FhirPathLexer(CharStreams.fromString(expression));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    FhirPathParser parser = new FhirPathParser(tokens);

    ValidatingInvocationParser invocationParser = new ValidatingInvocationParser();
    ParseResult expressionResult = invocationParser.visit(parser.expression());
    ParseResult result = new ParseResult(expressionResult.getExpression());
    result.setFromTable(expressionResult.getFromTable());
    return result;
  }

}
