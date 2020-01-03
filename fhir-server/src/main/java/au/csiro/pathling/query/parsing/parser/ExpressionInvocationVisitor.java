/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.query.parsing.parser;

import static au.csiro.pathling.utilities.Strings.md5Short;

import au.csiro.pathling.fhir.FhirPathBaseVisitor;
import au.csiro.pathling.fhir.FhirPathParser.FunctionInvocationContext;
import au.csiro.pathling.fhir.FhirPathParser.MemberInvocationContext;
import au.csiro.pathling.fhir.FhirPathParser.ParamListContext;
import au.csiro.pathling.fhir.FhirPathParser.ThisInvocationContext;
import au.csiro.pathling.query.functions.Function;
import au.csiro.pathling.query.functions.FunctionInput;
import au.csiro.pathling.query.operators.PathTraversalInput;
import au.csiro.pathling.query.operators.PathTraversalOperator;
import au.csiro.pathling.query.parsing.ParsedExpression;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.model.Enumerations;
import org.hl7.fhir.r4.model.ResourceType;

/**
 * This class is invoked on the right-hand side of the invocation expression, and can optionally be
 * constructed with an invoker expression to allow it to operate with knowledge of this context.
 *
 * @author John Grimes
 */
class ExpressionInvocationVisitor extends FhirPathBaseVisitor<ParsedExpression> {

  final ExpressionParserContext context;
  ParsedExpression invoker;

  ExpressionInvocationVisitor(ExpressionParserContext context) {
    this.context = context;
    invoker = context.getSubjectContext();
  }

  ExpressionInvocationVisitor(ExpressionParserContext context,
      ParsedExpression invoker) {
    this.context = context;
    this.invoker = invoker;
  }

  /**
   * This method gets called when an element is on the right-hand side of the invocation expression,
   * or when an identifier is referred to as a term (e.g. Encounter).
   */
  @Override
  public ParsedExpression visitMemberInvocation(MemberInvocationContext ctx) {
    String fhirPath = ctx.getText();

    // If there is no invoker, we assume that this is a base resource. If we can't resolve it,
    // an error will be thrown.
    String hash = md5Short(fhirPath);
    try {
      //noinspection ResultOfMethodCallIgnored
      ResourceType.fromCode(fhirPath);
    } catch (FHIRException e) {
      // If the expression is not a base resource type, treat it as a path traversal from the
      // subject resource.
      PathTraversalInput pathTraversalInput = new PathTraversalInput();
      pathTraversalInput.setLeft(invoker);
      pathTraversalInput.setRight(fhirPath);
      pathTraversalInput.setExpression(fhirPath);
      pathTraversalInput.setContext(context);
      return new PathTraversalOperator().invoke(pathTraversalInput);
    }

    // Build a new parse result to represent the resource.
    ParsedExpression result = new ParsedExpression();
    result.setFhirPath(fhirPath);
    result.setResource(true);
    result.setResourceType(Enumerations.ResourceType.fromCode(fhirPath));
    result.setOrigin(result);

    // Add a dataset to the parse result representing the nominated resource.
    Dataset<Row> dataset = context.getResourceReader().read(result.getResourceType());
    String firstColumn = dataset.columns()[0];
    String[] remainingColumns = Arrays
        .copyOfRange(dataset.columns(), 1, dataset.columns().length);
    dataset = dataset
        .withColumn(hash, functions.struct(firstColumn, remainingColumns));
    Column idColumn = dataset.col("id");
    Column valueColumn = dataset.col(hash);
    dataset = dataset.select(idColumn, valueColumn);
    result.setDataset(dataset);
    result.setHashedValue(idColumn, valueColumn);

    return result;
  }

  /**
   * This method gets called when a function call is on the right-hand side of an invocation
   * expression. It basically just checks that the function is known, and invokes it to get a new
   * ParseResult.
   */
  @Override
  public ParsedExpression visitFunctionInvocation(FunctionInvocationContext ctx) {
    // First, check if the function is supported.
    String functionIdentifier = ctx.functn().identifier().getText();
    Function function = Function.getFunction(functionIdentifier);
    if (function == null) {
      throw new InvalidRequestException("Unrecognised function: " + functionIdentifier);
    }
    return invokeFunction(ctx, function);
  }

  @Nonnull
  private ParsedExpression invokeFunction(FunctionInvocationContext ctx, Function function) {
    // Get the parse results for each of the expressions that make up the functions arguments.
    List<ParsedExpression> arguments;
    ParamListContext paramList = ctx.functn().paramList();
    if (paramList == null) {
      arguments = new ArrayList<>();
    } else {
      // Create a new ExpressionParserContext, which includes information about how to evaluate
      // the `$this` expression.
      ExpressionParserContext argumentContext = new ExpressionParserContext(context);
      ParsedExpression thisResult = new ParsedExpression(invoker);
      thisResult.setFhirPath("$this");
      argumentContext.setThisContext(thisResult);
      // Parse each of the expressions passed as arguments to the function.
      arguments = paramList.expression().stream()
          .map(expression -> new ExpressionVisitor(argumentContext).visit(expression))
          .collect(Collectors.toList());
    }

    // Build a function input that passes the arguments as ParseResults.
    FunctionInput functionInput = new FunctionInput();
    functionInput.setContext(context);
    functionInput.setExpression(ctx.getText());
    functionInput.setInput(invoker);
    functionInput.getArguments().addAll(arguments);

    // Invoke the function and return the result.
    return function.invoke(functionInput);
  }

  @Override
  public ParsedExpression visitThisInvocation(ThisInvocationContext ctx) {
    if (context.getThisContext() == null) {
      throw new InvalidRequestException(
          "$this can only be used within the context of arguments to a function");
    }
    return context.getThisContext();
  }

}
