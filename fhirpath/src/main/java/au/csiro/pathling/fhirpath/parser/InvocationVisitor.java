/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.fhirpath.parser;

import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static java.util.Objects.requireNonNull;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.NonLiteralPath;
import au.csiro.pathling.fhirpath.ResourcePath;
import au.csiro.pathling.fhirpath.function.NamedFunction;
import au.csiro.pathling.fhirpath.function.NamedFunctionInput;
import au.csiro.pathling.fhirpath.operator.PathTraversalInput;
import au.csiro.pathling.fhirpath.operator.PathTraversalOperator;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathBaseVisitor;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.FunctionInvocationContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.IndexInvocationContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.MemberInvocationContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.ParamListContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.ThisInvocationContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.TotalInvocationContext;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Column;
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
   * an expression on the left-hand side of the dot notation. In this case, the invoker is taken to
   * be either the root node, or the `$this` node in the context of functions that support it.
   *
   * @param context The {@link ParserContext} to use when parsing the invocation
   */
  InvocationVisitor(@Nonnull final ParserContext context) {
    this.context = context;
    this.invoker = null;
  }

  /**
   * This constructor is used when there is an explicit invoker on the left-hand side of the dot
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
  public FhirPath visitMemberInvocation(@Nullable final MemberInvocationContext ctx) {
    @Nullable final String fhirPath = requireNonNull(ctx).getText();
    requireNonNull(fhirPath);

    if (invoker != null) {
      // If there is an invoker, we treat this as a path traversal from the invoker.
      final PathTraversalInput pathTraversalInput = new PathTraversalInput(context, invoker,
          fhirPath);
      return new PathTraversalOperator().invoke(pathTraversalInput);

    } else {
      // If there is no invoker, we need to interpret what the expression means, based on its
      // content and context.

      if (context.getThisContext().isEmpty()) {
        // If we're at the root of the expression, this could be:
        // (1) a path traversal from the input context; or
        // (2) a reference to the subject resource.

        // The only type of resource reference that is allowed at the root a reference to the
        // subject resource.
        // See https://hl7.org/fhirpath/2018Sep/index.html#path-selection.
        if (fhirPath.equals(context.getInputContext().getExpression())) {
          return context.getInputContext();

        } else {
          // If the expression is not a reference to the subject resource, treat it as a path
          // traversal from the input context.
          final PathTraversalInput pathTraversalInput = new PathTraversalInput(context,
              context.getInputContext(), fhirPath);
          return new PathTraversalOperator().invoke(pathTraversalInput);
        }
      } else {
        // If we're in the context of a function's arguments, there are two valid things this
        // could be:
        // (1) a path traversal from the input context;
        // (2) a reference to a resource type.

        // Check if the expression is a reference to a known resource type.
        final ResourceType resourceType;
        try {
          resourceType = ResourceType.fromCode(fhirPath);
        } catch (final FHIRException e) {
          // If the expression is not a resource reference, treat it as a path traversal from the
          // input context.
          final PathTraversalInput pathTraversalInput = new PathTraversalInput(context,
              context.getThisContext().get(), fhirPath);
          return new PathTraversalOperator().invoke(pathTraversalInput);
        }

        // If the expression is a resource reference, we build a ResourcePath for it - we call this
        // the current resource reference.
        return ResourcePath
            .build(context.getFhirContext(), context.getDataSource(), resourceType, fhirPath, true);
      }
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
  public FhirPath visitFunctionInvocation(@Nullable final FunctionInvocationContext ctx) {
    @Nullable final String functionIdentifier = requireNonNull(ctx).function().identifier()
        .getText();
    requireNonNull(functionIdentifier);
    final NamedFunction function = NamedFunction.getInstance(functionIdentifier);

    // If there is no invoker, we use either the input context or the this context, depending on
    // whether we are in the context of function arguments.
    final FhirPath input = invoker == null
                           ? context.getThisContext().orElse(context.getInputContext())
                           : invoker;

    // A literal cannot be used as a function input.
    checkUserInput(input instanceof NonLiteralPath,
        "Literal expression cannot be used as input to a function invocation: " + input
            .getExpression());
    final NonLiteralPath nonLiteral = (NonLiteralPath) input;

    @Nullable final ParamListContext paramList = ctx.function().paramList();

    final List<FhirPath> arguments = new ArrayList<>();
    if (paramList != null) {
      // The `$this` path will be the same as the input, but with a different expression, and it 
      // will be singular as it represents a single item.
      // NOTE: This works because for $this the context for aggregation grouping on elements
      // includes `id` and `this` columns.

      // Create and alias the $this column.
      final NonLiteralPath thisPath = nonLiteral.toThisPath();

      // If the this context has an element ID, we need to add this to the grouping columns so that
      // aggregations that occur within the arguments are in the context of an element. Otherwise,
      // we add the resource ID column to the groupings.
      final List<Column> argumentGroupings = new ArrayList<>(context.getGroupingColumns());
      thisPath.getEidColumn().ifPresentOrElse(argumentGroupings::add,
          () -> argumentGroupings.add(thisPath.getIdColumn()));

      // Create a new ParserContext, which includes information about how to evaluate the `$this`
      // expression.
      final ParserContext argumentContext = new ParserContext(context.getInputContext(),
          context.getFhirContext(), context.getSparkSession(), context.getDataSource(),
          context.getTerminologyServiceFactory(), argumentGroupings, context.getNodeIdColumns());
      argumentContext.setThisContext(thisPath);

      // Parse each of the expressions passed as arguments to the function.
      arguments.addAll(
          paramList.expression().stream()
              .map(expression -> new Visitor(argumentContext).visit(expression))
              .collect(Collectors.toList())
      );
    }

    final NamedFunctionInput functionInput = new NamedFunctionInput(context, nonLiteral, arguments);
    return function.invoke(functionInput);
  }

  @Override
  @Nonnull
  public FhirPath visitThisInvocation(@Nullable final ThisInvocationContext ctx) {
    checkUserInput(context.getThisContext().isPresent(),
        "$this can only be used within the context of arguments to a function");
    return context.getThisContext().get();
  }

  @Override
  @Nonnull
  public FhirPath visitIndexInvocation(@Nullable final IndexInvocationContext ctx) {
    throw new InvalidUserInputError("$index is not supported");
  }

  @Override
  @Nonnull
  public FhirPath visitTotalInvocation(@Nullable final TotalInvocationContext ctx) {
    throw new InvalidUserInputError("$total is not supported");
  }

}
