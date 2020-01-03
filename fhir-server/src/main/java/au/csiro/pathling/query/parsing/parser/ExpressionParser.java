/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.parsing.parser;

import au.csiro.pathling.fhir.FhirPathLexer;
import au.csiro.pathling.fhir.FhirPathParser;
import au.csiro.pathling.query.parsing.ParsedExpression;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

/**
 * This is an ANTLR-based parser for processing a FHIRPath expression, and aggregating the results
 * into a ParseResult object. It delegates processing to a number of visitor classes, which contain
 * the logic for parsing specific parts of the grammar.
 *
 * @author John Grimes
 */
public class ExpressionParser {

  private final ExpressionParserContext context;

  public ExpressionParser(ExpressionParserContext context) {
    this.context = context;
  }

  public ParsedExpression parse(String expression) {
    FhirPathLexer lexer = new FhirPathLexer(CharStreams.fromString(expression));
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    FhirPathParser parser = new FhirPathParser(tokens);

    ExpressionVisitor expressionVisitor = new ExpressionVisitor(context);
    return expressionVisitor.visit(parser.expression());
  }

}
