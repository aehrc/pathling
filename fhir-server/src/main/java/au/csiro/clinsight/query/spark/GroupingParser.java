/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.query.spark;

import static au.csiro.clinsight.fhir.ElementResolver.resolveElement;
import static au.csiro.clinsight.fhir.ResourceDefinitions.isSupportedPrimitive;
import static au.csiro.clinsight.utilities.Strings.tokenizePath;
import static au.csiro.clinsight.utilities.Strings.untokenizePath;

import au.csiro.clinsight.fhir.ElementResolver.ElementNotKnownException;
import au.csiro.clinsight.fhir.ElementResolver.MultiValueTraversal;
import au.csiro.clinsight.fhir.ElementResolver.ResolvedElement;
import au.csiro.clinsight.fhir.ElementResolver.ResourceNotKnownException;
import au.csiro.clinsight.fhir.FhirPathLexer;
import au.csiro.clinsight.fhir.FhirPathParser;
import au.csiro.clinsight.utilities.Strings;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
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
      element = resolveElement(expression);
    } catch (ResourceNotKnownException | ElementNotKnownException e) {
      throw new InvalidRequestException(e.getMessage());
    }
    assert element != null;
    if (!isSupportedPrimitive(element.getTypeCode())) {
      throw new InvalidRequestException(
          "Grouping expression is not of a supported primitive type: " + expression);
    }
    String resultType = element.getTypeCode();
    assert resultType != null;
    result.setResultType(resultType);
    populateJoinsFromElement(result, element);
    return result;
  }

  private void populateJoinsFromElement(ParseResult result, ResolvedElement element) {
    Join previousJoin = null;
    for (MultiValueTraversal multiValueTraversal : element.getMultiValueTraversals()) {
      LinkedList<String> pathComponents = tokenizePath(multiValueTraversal.getPath());
      pathComponents.push(pathComponents.pop().toLowerCase());
      List<String> aliasComponents = pathComponents.subList(1, pathComponents.size());
      List<String> aliasTail = aliasComponents.subList(1, aliasComponents.size()).stream()
          .map(Strings::capitalize).collect(Collectors.toCollection(LinkedList::new));
      String alias = String.join("", aliasComponents.get(0), String.join("", aliasTail));
      String inlineExpression = untokenizePath(pathComponents);
      String joinExpression =
          "LATERAL VIEW inline(" + inlineExpression + ") " + alias + " AS " + String
              .join(", ", multiValueTraversal.getChildren());
      Join join = new Join(joinExpression, alias);
      if (previousJoin != null) {
        join.setDependsUpon(previousJoin);
        assert previousJoin.getInlineExpression() != null;
        String updatedExpression = join.getExpression()
            .replace(previousJoin.getInlineExpression(), previousJoin.getAlias());
        join.setExpression(updatedExpression);
      }
      join.setInlineExpression(inlineExpression);
      result.getJoins().add(join);
      previousJoin = join;
    }
    if (!result.getJoins().isEmpty()) {
      Join finalJoin = result.getJoins().last();
      assert finalJoin.getInlineExpression() != null;
      String updatedExpression = result.getExpression()
          .replace(finalJoin.getInlineExpression(), finalJoin.getAlias());
      result.setExpression(updatedExpression);
    }
  }

}
