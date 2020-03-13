/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
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

    // Remove the default console error reporter, and add a listener that wraps each parse error in
    // an invalid request exception.
    parser.removeErrorListeners();
    parser.addErrorListener(new ExpressionParserErrorListener());

    ExpressionVisitor expressionVisitor = new ExpressionVisitor(context);
    return expressionVisitor.visit(parser.expression());
  }

}
