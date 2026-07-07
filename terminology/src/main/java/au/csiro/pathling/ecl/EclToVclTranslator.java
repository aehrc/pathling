/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.ecl;

import au.csiro.pathling.ecl.generated.EclLexer;
import au.csiro.pathling.ecl.generated.EclParser;
import au.csiro.pathling.ecl.generated.EclParser.AndExprContext;
import au.csiro.pathling.ecl.generated.EclParser.AttributeValueContext;
import au.csiro.pathling.ecl.generated.EclParser.BoundExprContext;
import au.csiro.pathling.ecl.generated.EclParser.ConstraintContext;
import au.csiro.pathling.ecl.generated.EclParser.ConstraintOperatorContext;
import au.csiro.pathling.ecl.generated.EclParser.DottedExprContext;
import au.csiro.pathling.ecl.generated.EclParser.EclAttributeContext;
import au.csiro.pathling.ecl.generated.EclParser.EclAttributeNameContext;
import au.csiro.pathling.ecl.generated.EclParser.EclAttributeSetContext;
import au.csiro.pathling.ecl.generated.EclParser.ExprContext;
import au.csiro.pathling.ecl.generated.EclParser.FocusContext;
import au.csiro.pathling.ecl.generated.EclParser.MemberOfContext;
import au.csiro.pathling.ecl.generated.EclParser.MinusExprContext;
import au.csiro.pathling.ecl.generated.EclParser.OrExprContext;
import au.csiro.pathling.ecl.generated.EclParser.RefinedExprContext;
import au.csiro.pathling.ecl.generated.EclParser.RefinementContext;
import au.csiro.pathling.ecl.generated.EclParser.SubAttributeContext;
import au.csiro.pathling.ecl.generated.EclParser.SubRefinementContext;
import au.csiro.pathling.vcl.VclCode;
import au.csiro.pathling.vcl.VclCodeValue;
import au.csiro.pathling.vcl.VclConjunction;
import au.csiro.pathling.vcl.VclDisjunction;
import au.csiro.pathling.vcl.VclExclusion;
import au.csiro.pathling.vcl.VclExpression;
import au.csiro.pathling.vcl.VclFilter;
import au.csiro.pathling.vcl.VclFilterListValue;
import au.csiro.pathling.vcl.VclFilterOperator;
import au.csiro.pathling.vcl.VclNavigation;
import au.csiro.pathling.vcl.VclRefsetMembership;
import au.csiro.pathling.vcl.VclWildcard;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

/**
 * Translates the supported subset of SNOMED CT Expression Constraint Language into the {@link
 * VclExpression} model, so ECL implicit value sets are evaluated by the same engine as VCL. The
 * grammar recognises the whole ECL construct space; this translator maps the supported subset and
 * rejects anything outside it with an {@link UnsupportedEclConstructError} that names the
 * construct.
 *
 * @author John Grimes
 */
public final class EclToVclTranslator {

  static final String PROPERTY_CONCEPT = "concept";
  static final String PROPERTY_PARENT = "parent";

  private EclToVclTranslator() {
    // Utility class.
  }

  /**
   * Translates an ECL expression to a VCL expression.
   *
   * @param ecl the ECL expression
   * @return the equivalent VCL expression
   * @throws EclParseException if the expression is malformed
   * @throws UnsupportedEclConstructError if the expression uses an unsupported construct
   */
  @Nonnull
  public static VclExpression translate(@Nonnull final String ecl) {
    final BaseErrorListener errorListener =
        new BaseErrorListener() {
          @Override
          public void syntaxError(
              final Recognizer<?, ?> recognizer,
              final Object offendingSymbol,
              final int line,
              final int charPositionInLine,
              final String message,
              final RecognitionException e) {
            throw new EclParseException(charPositionInLine + 1, message);
          }
        };
    final EclLexer lexer = new EclLexer(CharStreams.fromString(ecl));
    lexer.removeErrorListeners();
    lexer.addErrorListener(errorListener);
    final EclParser parser = new EclParser(new CommonTokenStream(lexer));
    parser.removeErrorListeners();
    parser.addErrorListener(errorListener);
    return buildExpr(parser.expressionConstraint().expr());
  }

  @Nonnull
  private static VclExpression buildExpr(@Nonnull final ExprContext ctx) {
    return buildOr(ctx.orExpr());
  }

  @Nonnull
  private static VclExpression buildOr(@Nonnull final OrExprContext ctx) {
    final List<AndExprContext> operands = ctx.andExpr();
    if (operands.size() == 1) {
      return buildAnd(operands.get(0));
    }
    final List<VclExpression> parts = new ArrayList<>();
    operands.forEach(operand -> parts.add(buildAnd(operand)));
    return new VclDisjunction(parts);
  }

  @Nonnull
  private static VclExpression buildAnd(@Nonnull final AndExprContext ctx) {
    final List<MinusExprContext> operands = ctx.minusExpr();
    if (operands.size() == 1) {
      return buildMinus(operands.get(0));
    }
    final List<VclExpression> parts = new ArrayList<>();
    operands.forEach(operand -> parts.add(buildMinus(operand)));
    return new VclConjunction(parts);
  }

  @Nonnull
  private static VclExpression buildMinus(@Nonnull final MinusExprContext ctx) {
    final List<RefinedExprContext> operands = ctx.refinedExpr();
    VclExpression result = buildRefined(operands.get(0));
    for (int i = 1; i < operands.size(); i++) {
      result = new VclExclusion(result, buildRefined(operands.get(i)));
    }
    return result;
  }

  @Nonnull
  private static VclExpression buildRefined(@Nonnull final RefinedExprContext ctx) {
    if (!ctx.braceConstraint().isEmpty()) {
      final String text = ctx.braceConstraint(0).getText();
      final String body = text.substring(2).trim();
      throw new UnsupportedEclConstructError(
          body.startsWith("+")
              ? "history supplement ({{ + ... }})"
              : "term, definition status, or member filter ({{ ... }})");
    }
    final VclExpression base = buildConstraint(ctx.subExpr().constraint());
    if (ctx.refinement() != null) {
      return new VclConjunction(List.of(base, buildRefinement(ctx.refinement())));
    }
    return base;
  }

  @Nonnull
  private static VclExpression buildConstraint(@Nonnull final ConstraintContext ctx) {
    if (ctx.dottedExpr() != null) {
      return buildDotted(ctx.dottedExpr());
    }
    return buildBound(ctx.boundExpr());
  }

  @Nonnull
  private static VclExpression buildDotted(@Nonnull final DottedExprContext ctx) {
    VclExpression result = buildBound(ctx.boundExpr());
    for (final EclAttributeNameContext name : ctx.eclAttributeName()) {
      final String attribute = attributeName(name);
      result = new VclNavigation(new VclFilterListValue(List.of(result)), attribute);
    }
    return result;
  }

  @Nonnull
  private static VclExpression buildBound(@Nonnull final BoundExprContext ctx) {
    final FocusContext focus = ctx.focus();
    final ConstraintOperatorContext operator = ctx.constraintOperator();
    if (operator == null) {
      return buildFocus(focus);
    }
    if (focus.eclConceptReference() == null) {
      throw new UnsupportedEclConstructError(
          "hierarchy operator applied to a wildcard, reference set, or compound expression");
    }
    return applyOperator(operator, conceptId(focus.eclConceptReference().getText()));
  }

  @Nonnull
  private static VclExpression buildFocus(@Nonnull final FocusContext ctx) {
    if (ctx.memberOf() != null) {
      return buildMemberOf(ctx.memberOf());
    }
    if (ctx.wildCard() != null) {
      return new VclWildcard();
    }
    if (ctx.eclConceptReference() != null) {
      return new VclCode(conceptId(ctx.eclConceptReference().getText()));
    }
    return buildExpr(ctx.expr());
  }

  @Nonnull
  private static VclExpression buildMemberOf(@Nonnull final MemberOfContext ctx) {
    if (ctx.eclConceptReference() == null) {
      throw new UnsupportedEclConstructError(
          "reference set membership over a wildcard or expression");
    }
    return new VclRefsetMembership(conceptId(ctx.eclConceptReference().getText()));
  }

  @Nonnull
  private static VclExpression applyOperator(
      @Nonnull final ConstraintOperatorContext operator, @Nonnull final String code) {
    if (operator.DESC_OR_SELF() != null) {
      return new VclFilter(PROPERTY_CONCEPT, VclFilterOperator.IS_A, new VclCodeValue(code));
    }
    if (operator.DESC() != null) {
      return new VclFilter(
          PROPERTY_CONCEPT, VclFilterOperator.DESCENDENT_OF, new VclCodeValue(code));
    }
    if (operator.CHILD() != null) {
      return new VclFilter(PROPERTY_CONCEPT, VclFilterOperator.CHILD_OF, new VclCodeValue(code));
    }
    if (operator.ANC_OR_SELF() != null) {
      return new VclFilter(PROPERTY_CONCEPT, VclFilterOperator.GENERALIZES, new VclCodeValue(code));
    }
    if (operator.ANC() != null) {
      // Ancestors only: ancestors-or-self, less the concept itself.
      return new VclExclusion(
          new VclFilter(PROPERTY_CONCEPT, VclFilterOperator.GENERALIZES, new VclCodeValue(code)),
          new VclCode(code));
    }
    if (operator.PARENT() != null) {
      return new VclNavigation(new VclCodeValue(code), PROPERTY_PARENT);
    }
    if (operator.CHILD_OR_SELF() != null) {
      throw new UnsupportedEclConstructError("child-or-self operator (<<!)");
    }
    throw new UnsupportedEclConstructError("parent-or-self operator (>>!)");
  }

  @Nonnull
  private static VclExpression buildRefinement(@Nonnull final RefinementContext ctx) {
    final List<VclExpression> parts = new ArrayList<>();
    final List<Integer> separators = new ArrayList<>();
    for (final SubRefinementContext sub : ctx.subRefinement()) {
      parts.add(buildSubRefinement(sub));
    }
    collectSeparators(ctx.children, separators);
    return groupByOr(parts, separators);
  }

  @Nonnull
  private static VclExpression buildSubRefinement(@Nonnull final SubRefinementContext ctx) {
    if (ctx.eclAttributeGroup() != null) {
      throw new UnsupportedEclConstructError("grouped attributes ({ ... })");
    }
    if (ctx.refinement() != null) {
      return buildRefinement(ctx.refinement());
    }
    return buildAttributeSet(ctx.eclAttributeSet());
  }

  @Nonnull
  private static VclExpression buildAttributeSet(@Nonnull final EclAttributeSetContext ctx) {
    final List<VclExpression> parts = new ArrayList<>();
    final List<Integer> separators = new ArrayList<>();
    for (final SubAttributeContext sub : ctx.subAttribute()) {
      parts.add(buildSubAttribute(sub));
    }
    collectSeparators(ctx.children, separators);
    return groupByOr(parts, separators);
  }

  @Nonnull
  private static VclExpression buildSubAttribute(@Nonnull final SubAttributeContext ctx) {
    if (ctx.eclAttributeSet() != null) {
      return buildAttributeSet(ctx.eclAttributeSet());
    }
    return buildAttribute(ctx.eclAttribute());
  }

  @Nonnull
  private static VclExpression buildAttribute(@Nonnull final EclAttributeContext ctx) {
    if (ctx.cardinality() != null) {
      throw new UnsupportedEclConstructError("attribute cardinality ([min..max])");
    }
    if (ctx.REVERSE() != null) {
      throw new UnsupportedEclConstructError("reverse attribute flag (R)");
    }
    final String attribute = attributeName(ctx.eclAttributeName());
    final VclExpression value = buildAttributeValue(ctx.attributeValue());
    final VclFilterOperator operator =
        ctx.comparison().NOTEQUALS() != null ? VclFilterOperator.NOT_IN : VclFilterOperator.IN;
    return new VclFilter(attribute, operator, new VclFilterListValue(List.of(value)));
  }

  @Nonnull
  private static VclExpression buildAttributeValue(@Nonnull final AttributeValueContext ctx) {
    if (ctx.concreteValue() != null) {
      throw new UnsupportedEclConstructError("concrete value (#number or \"string\")");
    }
    if (ctx.subExpr() != null) {
      return buildConstraint(ctx.subExpr().constraint());
    }
    return buildExpr(ctx.expr());
  }

  @Nonnull
  private static String attributeName(@Nonnull final EclAttributeNameContext ctx) {
    if (ctx.wildCard() != null) {
      throw new UnsupportedEclConstructError("wildcard attribute name");
    }
    if (ctx.constraintOperator() != null) {
      throw new UnsupportedEclConstructError("hierarchy operator on an attribute name");
    }
    return conceptId(ctx.eclConceptReference().getText());
  }

  /**
   * Groups parts separated by OR into a disjunction of conjunctions (AND and comma bind tighter).
   */
  @Nonnull
  private static VclExpression groupByOr(
      @Nonnull final List<VclExpression> parts, @Nonnull final List<Integer> separators) {
    final List<List<VclExpression>> groups = new ArrayList<>();
    List<VclExpression> current = new ArrayList<>();
    current.add(parts.get(0));
    for (int i = 0; i < separators.size(); i++) {
      if (separators.get(i) == EclParser.OR) {
        groups.add(current);
        current = new ArrayList<>();
      }
      current.add(parts.get(i + 1));
    }
    groups.add(current);

    final List<VclExpression> disjuncts = new ArrayList<>();
    for (final List<VclExpression> group : groups) {
      disjuncts.add(group.size() == 1 ? group.get(0) : new VclConjunction(group));
    }
    return disjuncts.size() == 1 ? disjuncts.get(0) : new VclDisjunction(disjuncts);
  }

  private static void collectSeparators(
      @Nonnull final List<ParseTree> children, @Nonnull final List<Integer> separators) {
    for (final ParseTree child : children) {
      if (child instanceof final TerminalNode terminal) {
        final int type = terminal.getSymbol().getType();
        if (type == EclParser.OR || type == EclParser.AND || type == EclParser.COMMA) {
          separators.add(type);
        }
      }
    }
  }

  /** Extracts the concept identifier from a reference, discarding any {@code |term|}. */
  @Nonnull
  private static String conceptId(@Nonnull final String reference) {
    final int pipe = reference.indexOf('|');
    return (pipe >= 0 ? reference.substring(0, pipe) : reference).trim();
  }
}
