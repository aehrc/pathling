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

import static java.util.Objects.requireNonNull;

import au.csiro.pathling.errors.UnsupportedFhirPathFeatureError;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathBaseVisitor;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.FunctionInvocationContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.IndexInvocationContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.MemberInvocationContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.ParamListContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.ThisInvocationContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.TotalInvocationContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathVisitor;
import au.csiro.pathling.fhirpath.path.Paths;
import au.csiro.pathling.fhirpath.path.Paths.EvalFunction;
import au.csiro.pathling.fhirpath.path.Paths.Resource;
import au.csiro.pathling.fhirpath.path.Paths.Traversal;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * This class is invoked on the right-hand side of the invocation expression, and can optionally be
 * constructed with an invoker expression to allow it to operate with knowledge of this context.
 *
 * @author John Grimes
 */
class InvocationVisitor extends FhirPathBaseVisitor<FhirPath> {

  private static boolean canBeResourceType(@Nonnull final CharSequence identifier) {
    // Check if the identifier starts with a capital letter.
    return !identifier.isEmpty() && Character.isUpperCase(identifier.charAt(0));
  }


  private final boolean isRoot;

  public InvocationVisitor(final boolean isRoot) {
    this.isRoot = isRoot;
  }

  public InvocationVisitor() {
    this(false);
  }

  private static boolean isTypeSpecifierFunction(@Nonnull final String functionName) {
    return "ofType".equals(functionName) || "getReferenceKey".equals(functionName);
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
  public FhirPath visitMemberInvocation(
      @Nullable final MemberInvocationContext ctx) {
    final String fhirPath = requireNonNull(ctx).getText();
    if (isRoot && canBeResourceType(fhirPath)) {
      return new Resource(fhirPath);
    } else {
      return new Traversal(fhirPath);
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
  public FhirPath visitFunctionInvocation(
      @Nullable final FunctionInvocationContext ctx) {

    final String functionIdentifier = requireNonNull(ctx).function().identifier().getText();
    @Nullable final ParamListContext paramList = ctx.function().paramList();

    // NOTE: Here we assume that a function is either a type specifier function 
    // (and all the arguments are type specifiers) or regular function 
    // (none of the arguments are type specifiers).
    final FhirPathVisitor<FhirPath> paramListVisitor =
        isTypeSpecifierFunction(functionIdentifier)
        ? new TypeSpecifierVisitor()
        : new Visitor();

    final List<FhirPath> arguments = Optional.ofNullable(paramList)
        .map(ParamListContext::expression)
        .map(p -> p.stream()
            .map(paramListVisitor::visit)
            .toList()
        ).orElse(Collections.emptyList());

    return new EvalFunction(functionIdentifier, arguments);
  }

  @Override
  @Nonnull
  public FhirPath visitThisInvocation(
      @Nullable final ThisInvocationContext ctx) {
    return Paths.thisPath();
  }

  @Override
  @Nonnull
  public FhirPath visitIndexInvocation(
      @Nullable final IndexInvocationContext ctx) {
    throw new UnsupportedFhirPathFeatureError("$index is not supported");
  }

  @Override
  @Nonnull
  public FhirPath visitTotalInvocation(
      @Nullable final TotalInvocationContext ctx) {
    throw new UnsupportedFhirPathFeatureError("$total is not supported");
  }

}
