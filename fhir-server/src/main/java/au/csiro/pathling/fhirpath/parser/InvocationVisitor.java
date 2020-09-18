/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.parser;

import static au.csiro.pathling.utilities.Preconditions.checkNotNull;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.fhir.FhirPathBaseVisitor;
import au.csiro.pathling.fhir.FhirPathParser.FunctionInvocationContext;
import au.csiro.pathling.fhir.FhirPathParser.MemberInvocationContext;
import au.csiro.pathling.fhir.FhirPathParser.ParamListContext;
import au.csiro.pathling.fhir.FhirPathParser.ThisInvocationContext;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.function.NamedFunction;
import au.csiro.pathling.fhirpath.function.NamedFunctionInput;
import au.csiro.pathling.fhirpath.operator.PathTraversalInput;
import au.csiro.pathling.fhirpath.operator.PathTraversalOperator;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;

/**
 * This class is invoked on the right-hand side of the invocation expression, and can optionally be
 * constructed with an invoker expression to allow it to operate with knowledge of this context.
 *
 * @author John Grimes
 */
class InvocationVisitor extends FhirPathBaseVisitor<FhirPath> {

  @Nonnull
  private final ParserContext context;

  @Nullable
  private final FhirPath invoker;

  /**
   * This constructor is used when there is no explicit invoker, i.e. an invocation is made without
   * an expression on the left hand side of the dot notation. In this case, the invoker is taken to
   * be either the root node, or the `$this` node in the context of functions that support it.
   *
   * @param context The {@link ParserContext} to use when parsing the invocation
   */
  InvocationVisitor(@Nonnull final ParserContext context) {
    this.context = context;
    this.invoker = null;
  }

  /**
   * This constructor is used when there is an explicit invoker on the left hand side of the dot
   * notation.
   *
   * @param context The {@link ParserContext} to use when parsing the invocation
   * @param invoker A {@link FhirPath} representing the invoking expression
   */
  InvocationVisitor(@Nonnull final ParserContext context, @Nonnull final FhirPath invoker) {
    this.context = context;
    this.invoker = invoker;
  }

  /**
   * This method gets called when an element is on the right-hand side of the invocation expression,
   * or when an identifier is referred to as a term (e.g. "Encounter" or "type").
   *
   * @param ctx The {@link MemberInvocationContext}
   * @return A {@link FhirPath} expression
   */
  @Override
  @Nonnull
  public FhirPath visitMemberInvocation(@Nonnull final MemberInvocationContext ctx) {
    @Nullable final String fhirPath = ctx.getText();
    checkNotNull(fhirPath);

    if (invoker == null) {
      // If there is no invoker, this must be either (1) a reference to a resource type, or; 
      // (2) a path traversal from the input context.
      //
      // According to the spec, references to resource types at the root are only allowed when they 
      // are equal to  (or a supertype of) the input context. We don't currently support the use of 
      // abstract resource types, so it must be exactly equal.
      //
      // See https://hl7.org/fhirpath/2018Sep/index.html#path-selection.
      if (!context.getThisContext().isPresent() && fhirPath
          .equals(context.getInputContext().getExpression())) {
        return context.getInputContext();
      } else {
        try {
          final ResourceType resourceType = ResourceType.fromCode(fhirPath);
          return ResourcePath
              .build(context.getFhirContext(), context.getResourceReader(), resourceType, fhirPath,
                  false);
        } catch (final FHIRException e) {
          // If the expression is not a resource reference, treat it as a path traversal from the 
          // input context.
          final PathTraversalInput pathTraversalInput = new PathTraversalInput(context,
              context.getInputContext(), fhirPath);
          return new PathTraversalOperator().invoke(pathTraversalInput);
        }
      }
    } else {
      // If there is an invoker, we treat this as a path traversal from the invoker.
      final PathTraversalInput pathTraversalInput = new PathTraversalInput(context, invoker,
          fhirPath);
      return new PathTraversalOperator().invoke(pathTraversalInput);
    }
  }

  /**
   * This method gets called when a function call is on the right-hand side of an invocation
   * expression.
   *
   * @param ctx The {@link FunctionInvocationContext}
   * @return A {@link FhirPath} expression
   */
  @Override
  @Nonnull
  public FhirPath visitFunctionInvocation(@Nonnull final FunctionInvocationContext ctx) {
    @Nullable final String functionIdentifier = ctx.functn().identifier().getText();
    checkNotNull(functionIdentifier);
    final NamedFunction function = NamedFunction.getInstance(functionIdentifier);

    final FhirPath input = invoker == null
                           ? context.getInputContext()
                           : invoker;
    @Nullable final ParamListContext paramList = ctx.functn().paramList();

    final List<FhirPath> arguments = new ArrayList<>();
    if (paramList != null) {
      // The `$this` path will be the same as the input, but with a different expression and it will
      // be singular as it represents the current item from the collection.
      final FhirPath thisPath = input
          .copy(NamedFunction.THIS, input.getDataset(), input.getIdColumn(), input.getValueColumn(),
              true);
     
      // Create a new ParserContext, which includes information about how to evaluate the `$this` 
      // expression.
      final ParserContext argumentContext = new ParserContext(context.getInputContext(),
          Optional.of(thisPath), context.getFhirContext(), context.getSparkSession(),
          context.getResourceReader(), context.getTerminologyClient(),
          context.getTerminologyClientFactory());

      // Parse each of the expressions passed as arguments to the function.
      arguments.addAll(
          paramList.expression().stream()
              .map(expression -> new Visitor(argumentContext).visit(expression))
              .collect(Collectors.toList())
      );
    }

    final NamedFunctionInput functionInput = new NamedFunctionInput(context, input, arguments);
    return function.invoke(functionInput);
  }

  @Override
  @Nonnull
  public FhirPath visitThisInvocation(@Nonnull final ThisInvocationContext ctx) {
    checkUserInput(context.getThisContext().isPresent(),
        "$this can only be used within the context of arguments to a function");
    return context.getThisContext().get();
  }

}
