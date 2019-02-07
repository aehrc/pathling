/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.spark;

import static au.csiro.clinsight.fhir.ResourceDefinitions.getElementDefinition;
import static au.csiro.clinsight.utilities.Preconditions.checkNotNull;

import au.csiro.clinsight.fhir.FhirPathLexer;
import au.csiro.clinsight.fhir.FhirPathParser;
import au.csiro.clinsight.fhir.ResourceDefinitions;
import au.csiro.clinsight.fhir.ResourceDefinitions.ElementNotKnownException;
import au.csiro.clinsight.fhir.ResourceDefinitions.ResolvedElement;
import au.csiro.clinsight.fhir.ResourceDefinitions.ResourceNotKnownException;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
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
    ResolvedElement element;
    try {
      element = getElementDefinition(expression);
    } catch (ResourceNotKnownException | ElementNotKnownException e) {
      throw new InvalidRequestException(e.getMessage());
    }
    checkNotNull(element);
    if (!ResourceDefinitions.isSupportedPrimitive(element.getTypeCode())) {
      throw new InvalidRequestException(
          "Grouping expression is not of a supported primitive type: " + expression);
    }
    String resultType = element.getTypeCode();
    checkNotNull(resultType);
    result.setResultType(resultType);
    return result;
  }

}
