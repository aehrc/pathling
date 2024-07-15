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

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.expression.ExternalConstant;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathBaseVisitor;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.ExternalConstantContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.ExternalConstantTermContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.InvocationTermContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.LiteralTermContext;
import au.csiro.pathling.fhirpath.parser.generated.FhirPathParser.ParenthesizedTermContext;
import java.util.Optional;
import org.antlr.v4.runtime.tree.ParseTree;

/**
 * A term is typically a standalone literal or function invocation.
 *
 * @author John Grimes
 */
class TermVisitor extends FhirPathBaseVisitor<FhirPath> {

  @Override
  public FhirPath visitInvocationTerm(final InvocationTermContext ctx) {
    return new InvocationVisitor(Optional.empty()).visit(ctx.invocation());
  }

  @Override
  public FhirPath visitLiteralTerm(final LiteralTermContext ctx) {
    return new LiteralTermVisitor().visit(ctx.literal());
  }

  @Override
  public FhirPath visitExternalConstantTerm(final ExternalConstantTermContext ctx) {
    final ExternalConstantContext constantContext = ctx.externalConstant();
    final String term = Optional.ofNullable((ParseTree) constantContext.identifier())
        .orElse(constantContext.STRING()).getText();
    return new ExternalConstant(term);
  }

  @Override
  public FhirPath visitParenthesizedTerm(final ParenthesizedTermContext ctx) {
    return new Visitor().visit(ctx.expression());
  }

}
