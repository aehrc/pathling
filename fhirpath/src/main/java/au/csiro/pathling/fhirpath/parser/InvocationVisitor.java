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

import static java.util.stream.Collectors.toList;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.expression.FunctionCall;
import au.csiro.pathling.fhirpath.expression.This;
import au.csiro.pathling.fhirpath.expression.Traversal;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathBaseVisitor;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.FunctionInvocationContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.IndexInvocationContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.MemberInvocationContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.ParamListContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.ThisInvocationContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.TotalInvocationContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathVisitor;
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

  private final Optional<FhirPath> invoker;

  public InvocationVisitor() {
    this.invoker = Optional.empty();
  }

  public InvocationVisitor(final FhirPath invoker) {
    this.invoker = Optional.of(invoker);
  }

  /**
   * This method gets called when an element is on the right-hand side of the invocation expression,
   * or when an identifier is referred to as a term (e.g. "Encounter" or "type").
   *
   * @param ctx The {@link MemberInvocationContext}
   * @return A {@link FhirPath} expression
   */
  @Override
  public FhirPath visitMemberInvocation(final MemberInvocationContext ctx) {
    return new Traversal(invoker, ctx.identifier().getText());
  }

  /**
   * This method gets called when a function call is on the right-hand side of an invocation
   * expression.
   *
   * @param ctx The {@link FunctionInvocationContext}
   * @return A {@link FhirPath} expression
   */
  @Override
  public FhirPath visitFunctionInvocation(final FunctionInvocationContext ctx) {
    final String name = ctx.function().identifier().getText();
    final ParamListContext paramList = ctx.function().paramList();
    final FhirPathVisitor<FhirPath> paramListVisitor = new Visitor();

    final List<FhirPath> arguments = Optional.ofNullable(paramList)
        .map(ParamListContext::expression)
        .map(p -> p.stream()
            .map(paramListVisitor::visit)
            .collect(toList())
        ).orElse(Collections.emptyList());

    return new FunctionCall(name, arguments);
  }

  @Override
  public FhirPath visitThisInvocation(final ThisInvocationContext ctx) {
    return new This();
  }

  @Override
  public FhirPath visitIndexInvocation(final IndexInvocationContext ctx) {
    throw new UnsupportedExpressionError("$index is not supported");
  }

  @Override
  public FhirPath visitTotalInvocation(final TotalInvocationContext ctx) {
    throw new UnsupportedExpressionError("$total is not supported");
  }

}
