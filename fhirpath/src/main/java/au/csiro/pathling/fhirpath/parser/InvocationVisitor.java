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
import static java.util.stream.Collectors.toList;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.FhirPathType;
import au.csiro.pathling.fhirpath.FunctionInput;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.function.NamedFunction;
import au.csiro.pathling.fhirpath.function.registry.FunctionRegistry.NoSuchFunctionException;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathBaseVisitor;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.FunctionInvocationContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.IndexInvocationContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.MemberInvocationContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.ParamListContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.ThisInvocationContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.TotalInvocationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.functions;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * This class is invoked on the right-hand side of the invocation expression, and can optionally be
 * constructed with an invoker expression to allow it to operate with knowledge of this context.
 *
 * @author John Grimes
 */
class InvocationVisitor extends FhirPathBaseVisitor<FhirPath<Collection, Collection>> {

  @Nonnull
  private final ParserContext context;

  /**
   * This constructor is used when there is no explicit invoker, i.e. an invocation is made without
   * an expression on the left-hand side of the dot notation. In this case, the invoker is taken to
   * be either the root node, or the `$this` node in the context of functions that support it.
   *
   * @param context The {@link ParserContext} to use when parsing the invocation
   */
  InvocationVisitor(@Nonnull final ParserContext context) {
    this.context = context;
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
  public FhirPath<Collection, Collection> visitMemberInvocation(
      @Nullable final MemberInvocationContext ctx) {
    @Nullable final String fhirPath = requireNonNull(ctx).getText();
    requireNonNull(fhirPath);

    // TODO: refactor to an expression
    return (input, c) -> {
      try {
        // Attempt path traversal.
        final Optional<Collection> result = input.traverse(fhirPath);
        checkUserInput(result.isPresent(), "No such child: " + fhirPath);
        return result.get();

      } catch (final InvalidUserInputError e) {
        try {
          // TODO: what is this about?
          // If it is not a valid path traversal, see if it is a valid type specifier.
          final FHIRDefinedType fhirType = FHIRDefinedType.fromCode(fhirPath);
          return Collection.build(functions.lit(null), Optional.of(FhirPathType.TYPE_SPECIFIER),
              Optional.of(fhirType), Optional.empty());

        } catch (final FHIRException e2) {
          throw new InvalidUserInputError(
              "Invocation is not a valid path or type specifier: " + fhirPath);
        }
      }
    };
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
  public FhirPath<Collection, Collection> visitFunctionInvocation(
      @Nullable final FunctionInvocationContext ctx) {

    final String functionIdentifier = requireNonNull(ctx).function().identifier().getText();

    @Nullable final ParamListContext paramList = ctx.function().paramList();
    final Visitor paramListVisitor = new Visitor(context);
    final List<FhirPath<Collection, Collection>> arguments = Optional.ofNullable(paramList)
        .map(ParamListContext::expression)
        .map(p -> p.stream()
            .map(paramListVisitor::visit)
            .collect(toList())
        ).orElse(new ArrayList<>());

    // TODO: refactor to an expression
    return (input, c) -> {
      final NamedFunction function;
      try {
        function = c.getFunctionRegistry().getInstance(functionIdentifier);
      } catch (final NoSuchFunctionException e) {
        throw new InvalidUserInputError(e.getMessage());
      }
      final FunctionInput functionInput = new FunctionInput(c, input, arguments);
      return function.invoke(functionInput);
    };
  }

  @Override
  @Nonnull
  public FhirPath<Collection, Collection> visitThisInvocation(
      @Nullable final ThisInvocationContext ctx) {
    return (input, c) -> input;
  }

  @Override
  @Nonnull
  public FhirPath<Collection, Collection> visitIndexInvocation(
      @Nullable final IndexInvocationContext ctx) {
    throw new InvalidUserInputError("$index is not supported");
  }

  @Override
  @Nonnull
  public FhirPath<Collection, Collection> visitTotalInvocation(
      @Nullable final TotalInvocationContext ctx) {
    throw new InvalidUserInputError("$total is not supported");
  }

}
