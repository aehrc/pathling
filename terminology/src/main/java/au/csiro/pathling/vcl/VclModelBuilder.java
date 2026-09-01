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

package au.csiro.pathling.vcl;

import au.csiro.pathling.vcl.generated.VclParser.CodeContext;
import au.csiro.pathling.vcl.generated.VclParser.CodeListContext;
import au.csiro.pathling.vcl.generated.VclParser.ExprContext;
import au.csiro.pathling.vcl.generated.VclParser.FilterContext;
import au.csiro.pathling.vcl.generated.VclParser.FilterListContext;
import au.csiro.pathling.vcl.generated.VclParser.IncludeVsContext;
import au.csiro.pathling.vcl.generated.VclParser.SimpleExprContext;
import au.csiro.pathling.vcl.generated.VclParser.SubExprContext;
import au.csiro.pathling.vcl.generated.VclParser.SystemUriContext;
import au.csiro.pathling.vcl.generated.VclParser.VclContext;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

/**
 * Builds a {@link VclExpression} abstract syntax tree from a VCL parse tree. This is a recursive
 * descent over the generated parse-tree node types, mapping each grammar production onto the
 * corresponding model node.
 *
 * @author John Grimes
 */
class VclModelBuilder {

  @Nonnull
  VclExpression build(@Nonnull final VclContext ctx) {
    return buildExpr(ctx.expr());
  }

  @Nonnull
  private VclExpression buildExpr(@Nonnull final ExprContext ctx) {
    final VclExpression base = buildSubExpr(ctx.subExpr());
    if (ctx.conjunction() != null) {
      final List<VclExpression> operands = new ArrayList<>();
      operands.add(base);
      ctx.conjunction().subExpr().forEach(sub -> operands.add(buildSubExpr(sub)));
      return new VclConjunction(operands);
    }
    if (ctx.disjunction() != null) {
      final List<VclExpression> operands = new ArrayList<>();
      operands.add(base);
      ctx.disjunction().subExpr().forEach(sub -> operands.add(buildSubExpr(sub)));
      return new VclDisjunction(operands);
    }
    if (ctx.exclusion() != null) {
      return new VclExclusion(base, buildSubExpr(ctx.exclusion().subExpr()));
    }
    return base;
  }

  @Nonnull
  private VclExpression buildSubExpr(@Nonnull final SubExprContext ctx) {
    final VclExpression inner =
        ctx.simpleExpr() != null ? buildSimpleExpr(ctx.simpleExpr()) : buildExpr(ctx.expr());
    if (ctx.systemUri() != null) {
      return new VclSystemScoped(buildSystemUri(ctx.systemUri()), inner);
    }
    return inner;
  }

  @Nonnull
  private VclExpression buildSimpleExpr(@Nonnull final SimpleExprContext ctx) {
    if (ctx.STAR() != null) {
      return new VclWildcard();
    }
    if (ctx.code() != null) {
      return new VclCode(codeText(ctx.code()));
    }
    if (ctx.filter() != null) {
      return buildFilter(ctx.filter());
    }
    return buildIncludeVs(ctx.includeVs());
  }

  @Nonnull
  private VclExpression buildFilter(@Nonnull final FilterContext ctx) {
    if (ctx.DOT() != null) {
      return new VclNavigation(navigationSource(ctx), codeText(ctx.property().code()));
    }
    final String property = codeText(ctx.property().code());
    if (ctx.EQ() != null) {
      return new VclFilter(
          property, VclFilterOperator.EQUALS, new VclCodeValue(codeText(ctx.code())));
    }
    if (ctx.IS_A() != null) {
      return new VclFilter(
          property, VclFilterOperator.IS_A, new VclCodeValue(codeText(ctx.code())));
    }
    if (ctx.IS_NOT_A() != null) {
      return new VclFilter(
          property, VclFilterOperator.IS_NOT_A, new VclCodeValue(codeText(ctx.code())));
    }
    if (ctx.DESC_OF() != null) {
      return new VclFilter(
          property, VclFilterOperator.DESCENDENT_OF, new VclCodeValue(codeText(ctx.code())));
    }
    if (ctx.GENERALIZES() != null) {
      return new VclFilter(
          property, VclFilterOperator.GENERALIZES, new VclCodeValue(codeText(ctx.code())));
    }
    if (ctx.CHILD_OF() != null) {
      return new VclFilter(
          property, VclFilterOperator.CHILD_OF, new VclCodeValue(codeText(ctx.code())));
    }
    if (ctx.DESC_LEAF() != null) {
      return new VclFilter(
          property, VclFilterOperator.DESCENDENT_LEAF, new VclCodeValue(codeText(ctx.code())));
    }
    if (ctx.EXISTS() != null) {
      return new VclFilter(
          property, VclFilterOperator.EXISTS, new VclCodeValue(codeText(ctx.code())));
    }
    if (ctx.REGEX() != null) {
      return new VclFilter(
          property, VclFilterOperator.REGEX, new VclStringValue(stringText(ctx.str().getText())));
    }
    if (ctx.IN() != null) {
      return new VclFilter(property, VclFilterOperator.IN, membershipValue(ctx));
    }
    // The only remaining alternative is not-in.
    return new VclFilter(property, VclFilterOperator.NOT_IN, membershipValue(ctx));
  }

  @Nonnull
  private VclFilterValue navigationSource(@Nonnull final FilterContext ctx) {
    if (ctx.code() != null) {
      return new VclCodeValue(codeText(ctx.code()));
    }
    if (ctx.codeList() != null) {
      return new VclCodeListValue(codeListValues(ctx.codeList()));
    }
    if (ctx.STAR() != null) {
      return new VclWildcardValue();
    }
    if (ctx.URI() != null) {
      return new VclUriValue(ctx.URI().getText());
    }
    return new VclFilterListValue(filterListValues(ctx.filterList()));
  }

  @Nonnull
  private VclFilterValue membershipValue(@Nonnull final FilterContext ctx) {
    if (ctx.codeList() != null) {
      return new VclCodeListValue(codeListValues(ctx.codeList()));
    }
    if (ctx.URI() != null) {
      return new VclUriValue(ctx.URI().getText());
    }
    return new VclFilterListValue(filterListValues(ctx.filterList()));
  }

  @Nonnull
  private VclIncludeValueSet buildIncludeVs(@Nonnull final IncludeVsContext ctx) {
    if (ctx.URI() != null) {
      return new VclIncludeValueSet(ctx.URI().getText(), false);
    }
    return new VclIncludeValueSet(ctx.systemUri().URI().getText(), true);
  }

  @Nonnull
  private VclSystemUri buildSystemUri(@Nonnull final SystemUriContext ctx) {
    final String uri = ctx.URI().getText();
    final int pipe = uri.indexOf('|');
    if (pipe >= 0) {
      return new VclSystemUri(uri.substring(0, pipe), uri.substring(pipe + 1));
    }
    return new VclSystemUri(uri, null);
  }

  @Nonnull
  private List<String> codeListValues(@Nonnull final CodeListContext ctx) {
    final List<String> codes = new ArrayList<>();
    ctx.code().forEach(code -> codes.add(codeText(code)));
    return codes;
  }

  @Nonnull
  private List<VclExpression> filterListValues(@Nonnull final FilterListContext ctx) {
    // Each element is a `filter` production, either a property filter or a reverse navigation, so
    // the elements are held as the common VclExpression supertype.
    final List<VclExpression> filters = new ArrayList<>();
    ctx.filter().forEach(filter -> filters.add(buildFilter(filter)));
    return filters;
  }

  @Nonnull
  private String codeText(@Nonnull final CodeContext ctx) {
    if (ctx.SCODE() != null) {
      return ctx.SCODE().getText();
    }
    return stringText(ctx.QUOTED_VALUE().getText());
  }

  /**
   * Removes the surrounding double quotes from a quoted lexical value and unescapes the two
   * escapable characters ({@code \"} and {@code \\}).
   */
  @Nonnull
  private String stringText(@Nonnull final String quoted) {
    final String inner = quoted.substring(1, quoted.length() - 1);
    final StringBuilder result = new StringBuilder(inner.length());
    boolean escaped = false;
    for (int i = 0; i < inner.length(); i++) {
      final char c = inner.charAt(i);
      if (escaped) {
        result.append(c);
        escaped = false;
      } else if (c == '\\') {
        escaped = true;
      } else {
        result.append(c);
      }
    }
    return result.toString();
  }
}
