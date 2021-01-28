/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.parser;

import au.csiro.pathling.fhir.FhirPathLexer;
import au.csiro.pathling.fhir.FhirPathParser;
import au.csiro.pathling.fhirpath.FhirPath;
import javax.annotation.Nonnull;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

/**
 * This is an ANTLR-based parser for processing a FHIRPath expression, and aggregating the results
 * into a FhirPath object. It delegates processing to a number of visitor classes, which contain the
 * logic for parsing specific parts of the grammar.
 *
 * @author John Grimes
 */
public class Parser {

  @Nonnull
  private final ParserContext context;

  /**
   * @param context The {@link ParserContext} that this parser should use when parsing expressions
   */
  public Parser(@Nonnull final ParserContext context) {
    this.context = context;
  }

  /**
   * Parses a FHIRPath expression.
   *
   * @param expression The String representation of the FHIRPath expression
   * @return a new {@link FhirPath} object
   */
  @Nonnull
  public FhirPath parse(@Nonnull final String expression) {
    final FhirPathLexer lexer = new FhirPathLexer(CharStreams.fromString(expression));
    final CommonTokenStream tokens = new CommonTokenStream(lexer);
    final FhirPathParser parser = new FhirPathParser(tokens);

    // Remove the default console error reporter, and add a listener that wraps each parse error in
    // an invalid request exception.
    parser.removeErrorListeners();
    parser.addErrorListener(new ParserErrorListener());

    final Visitor visitor = new Visitor(context);
    return visitor.visit(parser.expression()).getFhirPath();
  }

}
