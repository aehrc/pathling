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

package au.csiro.pathling.terminology.local;

import au.csiro.pathling.terminology.local.index.CodeSystemIndexes;
import au.csiro.pathling.terminology.local.index.ConceptDictionary;
import au.csiro.pathling.terminology.local.index.HierarchyIndex;
import au.csiro.pathling.vcl.VclAttributeConstraint;
import au.csiro.pathling.vcl.VclCode;
import au.csiro.pathling.vcl.VclCodeListValue;
import au.csiro.pathling.vcl.VclCodeValue;
import au.csiro.pathling.vcl.VclConjunction;
import au.csiro.pathling.vcl.VclDisjunction;
import au.csiro.pathling.vcl.VclExclusion;
import au.csiro.pathling.vcl.VclExpression;
import au.csiro.pathling.vcl.VclFilter;
import au.csiro.pathling.vcl.VclFilterListValue;
import au.csiro.pathling.vcl.VclFilterOperator;
import au.csiro.pathling.vcl.VclFilterValue;
import au.csiro.pathling.vcl.VclNavigation;
import au.csiro.pathling.vcl.VclRefsetMembership;
import au.csiro.pathling.vcl.VclStringValue;
import au.csiro.pathling.vcl.VclSystemScoped;
import au.csiro.pathling.vcl.VclWildcard;
import au.csiro.pathling.vcl.VclWildcardValue;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import org.roaringbitmap.RoaringBitmap;

/**
 * Evaluates a {@link VclExpression} against the runtime indexes of one code system version,
 * producing the set of member concepts as a {@link RoaringBitmap} of dense identifiers.
 *
 * <p>By default the result is restricted to active concepts, matching the reference server's
 * behaviour for implicit value sets. An expression that explicitly selects inactive concepts (an
 * {@code inactive = true} property filter) is evaluated over the whole concept universe instead, so
 * such concepts survive.
 *
 * @author John Grimes
 */
public class VclEvaluator {

  static final String PROPERTY_CONCEPT = "concept";
  static final String PROPERTY_PARENT = "parent";
  static final String PROPERTY_CHILD = "child";
  static final String PROPERTY_INACTIVE = "inactive";
  static final String PROPERTY_MODULE_ID = "moduleId";
  static final String PROPERTY_SUFFICIENTLY_DEFINED = "sufficientlyDefined";
  static final String PROPERTY_EFFECTIVE_TIME = "effectiveTime";

  @Nonnull private final CodeSystemIndexes indexes;
  @Nonnull private final String systemUrl;

  /**
   * Creates an evaluator bound to one code system version.
   *
   * @param indexes the runtime indexes for the target code system version
   * @param systemUrl the canonical URL of the target code system
   */
  public VclEvaluator(@Nonnull final CodeSystemIndexes indexes, @Nonnull final String systemUrl) {
    this.indexes = indexes;
    this.systemUrl = systemUrl;
  }

  /**
   * Evaluates an expression to the set of member concept dense identifiers.
   *
   * @param expression the expression to evaluate
   * @return the members, as a bitmap of dense identifiers
   */
  @Nonnull
  public RoaringBitmap evaluate(@Nonnull final VclExpression expression) {
    final RoaringBitmap result = eval(expression);
    if (!selectsInactive(expression)) {
      result.and(indexes.dictionary().activeConcepts());
    }
    return result;
  }

  @Nonnull
  private RoaringBitmap eval(@Nonnull final VclExpression expression) {
    if (expression instanceof VclWildcard) {
      return indexes.dictionary().allConcepts();
    }
    if (expression instanceof final VclCode code) {
      return single(code.getCode());
    }
    if (expression instanceof final VclConjunction conjunction) {
      return combine(conjunction.getOperands(), true);
    }
    if (expression instanceof final VclDisjunction disjunction) {
      return combine(disjunction.getOperands(), false);
    }
    if (expression instanceof final VclExclusion exclusion) {
      final RoaringBitmap included = eval(exclusion.getIncluded());
      included.andNot(eval(exclusion.getExcluded()));
      return included;
    }
    if (expression instanceof final VclSystemScoped scoped) {
      // A subexpression scoped to a different system contributes nothing to this system's members.
      if (systemUrl.equals(scoped.getSystem().getSystem())) {
        return eval(scoped.getExpression());
      }
      return new RoaringBitmap();
    }
    if (expression instanceof final VclRefsetMembership refset) {
      return indexes.refsets().membersOf(refset.getRefsetCode());
    }
    if (expression instanceof final VclFilter filter) {
      return evalFilter(filter);
    }
    if (expression instanceof final VclAttributeConstraint attribute) {
      return evalAttributeConstraint(attribute);
    }
    if (expression instanceof final VclNavigation navigation) {
      return evalNavigation(navigation);
    }
    // Value set inclusions (VclIncludeValueSet) require cross-value-set resolution added with FHIR
    // terminology import; until then they contribute no members.
    return new RoaringBitmap();
  }

  @Nonnull
  private RoaringBitmap combine(
      @Nonnull final List<VclExpression> operands, final boolean intersect) {
    RoaringBitmap result = null;
    for (final VclExpression operand : operands) {
      final RoaringBitmap next = eval(operand);
      if (result == null) {
        result = next;
      } else if (intersect) {
        result.and(next);
      } else {
        result.or(next);
      }
    }
    return result == null ? new RoaringBitmap() : result;
  }

  @Nonnull
  private RoaringBitmap evalFilter(@Nonnull final VclFilter filter) {
    final String property = filter.getProperty();
    switch (property) {
      case PROPERTY_CONCEPT:
        return evalConceptFilter(filter);
      case PROPERTY_PARENT:
        // Concepts whose direct parent is the value (the inverse of a child edge).
        return indexes.hierarchy().childrenOf(requireDense(codeValue(filter.getValue())));
      case PROPERTY_CHILD:
        // Concepts whose direct child is the value.
        return indexes.hierarchy().parentsOf(requireDense(codeValue(filter.getValue())));
      case PROPERTY_INACTIVE:
        {
          final boolean wantInactive = "true".equalsIgnoreCase(codeValue(filter.getValue()));
          return dictionary().conceptsWhere(d -> dictionary().isActive(d) != wantInactive);
        }
      case PROPERTY_MODULE_ID:
        {
          final String module = codeValue(filter.getValue());
          return dictionary().conceptsWhere(d -> module.equals(dictionary().moduleId(d)));
        }
      case PROPERTY_SUFFICIENTLY_DEFINED:
        {
          final boolean wantDefined = "true".equalsIgnoreCase(codeValue(filter.getValue()));
          return dictionary().conceptsWhere(d -> dictionary().isDefined(d) == wantDefined);
        }
      case PROPERTY_EFFECTIVE_TIME:
        {
          final String time = codeValue(filter.getValue());
          return dictionary().conceptsWhere(d -> time.equals(dictionary().effectiveTime(d)));
        }
      default:
        // Any other property is an attribute SCTID: an attribute constraint.
        return evalAttributeConstraint(property, filter.getOperator(), filter.getValue());
    }
  }

  @Nonnull
  private RoaringBitmap evalConceptFilter(@Nonnull final VclFilter filter) {
    final HierarchyIndex hierarchy = indexes.hierarchy();
    final VclFilterOperator operator = filter.getOperator();
    if (operator == VclFilterOperator.IN || operator == VclFilterOperator.NOT_IN) {
      final RoaringBitmap listed = new RoaringBitmap();
      for (final String code : codeList(filter.getValue())) {
        listed.or(single(code));
      }
      if (operator == VclFilterOperator.NOT_IN) {
        final RoaringBitmap all = dictionary().activeConcepts();
        all.andNot(listed);
        return all;
      }
      return listed;
    }
    final Integer dense = dictionary().denseId(codeValue(filter.getValue()));
    if (dense == null) {
      return new RoaringBitmap();
    }
    switch (operator) {
      case EQUALS:
        return single(codeValue(filter.getValue()));
      case IS_A:
        return descendantsOrSelf(dense);
      case DESCENDENT_OF:
        return hierarchy.descendantsOf(dense);
      case CHILD_OF:
        return hierarchy.childrenOf(dense);
      case GENERALIZES:
        return ancestorsOrSelf(dense);
      case DESCENDENT_LEAF:
        return leaves(hierarchy.descendantsOf(dense));
      case IS_NOT_A:
        {
          final RoaringBitmap all = dictionary().activeConcepts();
          all.andNot(descendantsOrSelf(dense));
          return all;
        }
      default:
        return new RoaringBitmap();
    }
  }

  @Nonnull
  private RoaringBitmap evalAttributeConstraint(
      @Nonnull final String attributeType,
      @Nonnull final VclFilterOperator operator,
      @Nonnull final VclFilterValue value) {
    return attributeSources(
        List.of(attributeType), resolveValueSet(value), operator == VclFilterOperator.NOT_IN);
  }

  @Nonnull
  private RoaringBitmap evalAttributeConstraint(@Nonnull final VclAttributeConstraint constraint) {
    final List<String> attributeTypes = attributeTypeCodes(constraint);
    return attributeSources(attributeTypes, eval(constraint.getValue()), constraint.isNegated());
  }

  /** Expands the constraint's attribute type over the attribute-type hierarchy where requested. */
  @Nonnull
  private List<String> attributeTypeCodes(@Nonnull final VclAttributeConstraint constraint) {
    final List<String> codes = new ArrayList<>();
    if (constraint.isIncludeAttributeSelf()) {
      codes.add(constraint.getAttributeType());
    }
    if (constraint.isIncludeAttributeDescendants()) {
      // The attribute type is only expandable when it is itself a stored concept; otherwise the
      // named type stands alone (there are no descendant attribute types to add).
      final Integer dense = dictionary().denseId(constraint.getAttributeType());
      if (dense != null) {
        indexes
            .hierarchy()
            .descendantsOf(dense)
            .forEach((org.roaringbitmap.IntConsumer) desc -> codes.add(dictionary().code(desc)));
      }
    }
    return codes;
  }

  @Nonnull
  private RoaringBitmap attributeSources(
      @Nonnull final List<String> attributeTypes,
      @Nonnull final RoaringBitmap valueConcepts,
      final boolean negated) {
    final RoaringBitmap sources = new RoaringBitmap();
    for (final String attributeType : attributeTypes) {
      sources.or(indexes.relationships().sourcesOf(attributeType, valueConcepts));
    }
    if (negated) {
      final RoaringBitmap all = dictionary().activeConcepts();
      all.andNot(sources);
      return all;
    }
    return sources;
  }

  @Nonnull
  private RoaringBitmap evalNavigation(@Nonnull final VclNavigation navigation) {
    final RoaringBitmap sources = resolveNavigationSource(navigation.getSource());
    final String property = navigation.getProperty();
    if (PROPERTY_PARENT.equals(property)) {
      return gatherHierarchy(sources, true);
    }
    if (PROPERTY_CHILD.equals(property)) {
      return gatherHierarchy(sources, false);
    }
    // Otherwise the property is an attribute SCTID: forward dotted navigation.
    return indexes.relationships().targetsOf(property, sources);
  }

  @Nonnull
  private RoaringBitmap gatherHierarchy(
      @Nonnull final RoaringBitmap sources, final boolean parents) {
    final HierarchyIndex hierarchy = indexes.hierarchy();
    final RoaringBitmap result = new RoaringBitmap();
    sources.forEach(
        (org.roaringbitmap.IntConsumer)
            dense -> result.or(parents ? hierarchy.parentsOf(dense) : hierarchy.childrenOf(dense)));
    return result;
  }

  /**
   * Resolves a filter value that names concepts (a code, code list, wildcard, or nested filters).
   */
  @Nonnull
  private RoaringBitmap resolveValueSet(@Nonnull final VclFilterValue value) {
    if (value instanceof final VclCodeValue codeValue) {
      return single(codeValue.getCode());
    }
    if (value instanceof final VclCodeListValue codeList) {
      final RoaringBitmap result = new RoaringBitmap();
      for (final String code : codeList.getCodes()) {
        result.or(single(code));
      }
      return result;
    }
    if (value instanceof VclWildcardValue) {
      return dictionary().allConcepts();
    }
    if (value instanceof final VclFilterListValue filterList) {
      return combine(filterList.getFilters(), true);
    }
    // A URI value (a nested value set) is resolved once FHIR terminology import is available.
    return new RoaringBitmap();
  }

  @Nonnull
  private RoaringBitmap resolveNavigationSource(@Nonnull final VclFilterValue source) {
    return resolveValueSet(source);
  }

  @Nonnull
  private RoaringBitmap descendantsOrSelf(final int dense) {
    final RoaringBitmap result = indexes.hierarchy().descendantsOf(dense);
    result.add(dense);
    return result;
  }

  @Nonnull
  private RoaringBitmap ancestorsOrSelf(final int dense) {
    final RoaringBitmap result = indexes.hierarchy().ancestorsOf(dense);
    result.add(dense);
    return result;
  }

  @Nonnull
  private RoaringBitmap leaves(@Nonnull final RoaringBitmap candidates) {
    final HierarchyIndex hierarchy = indexes.hierarchy();
    final RoaringBitmap result = new RoaringBitmap();
    candidates.forEach(
        (org.roaringbitmap.IntConsumer)
            dense -> {
              if (hierarchy.childrenOf(dense).isEmpty()) {
                result.add(dense);
              }
            });
    return result;
  }

  @Nonnull
  private RoaringBitmap single(@Nonnull final String code) {
    final Integer dense = dictionary().denseId(code);
    final RoaringBitmap result = new RoaringBitmap();
    if (dense != null) {
      result.add(dense);
    }
    return result;
  }

  private int requireDense(@Nonnull final String code) {
    final Integer dense = dictionary().denseId(code);
    return dense == null ? Integer.MAX_VALUE : dense;
  }

  @Nonnull
  private ConceptDictionary dictionary() {
    return indexes.dictionary();
  }

  @Nonnull
  private static String codeValue(@Nonnull final VclFilterValue value) {
    if (value instanceof final VclCodeValue codeValue) {
      return codeValue.getCode();
    }
    if (value instanceof final VclStringValue stringValue) {
      return stringValue.getValue();
    }
    return "";
  }

  @Nonnull
  private static List<String> codeList(@Nonnull final VclFilterValue value) {
    if (value instanceof final VclCodeListValue codeList) {
      return codeList.getCodes();
    }
    if (value instanceof final VclCodeValue codeValue) {
      return List.of(codeValue.getCode());
    }
    return List.of();
  }

  /** Determines whether the expression explicitly asks for inactive concepts. */
  private static boolean selectsInactive(@Nonnull final VclExpression expression) {
    if (expression instanceof final VclFilter filter) {
      return PROPERTY_INACTIVE.equals(filter.getProperty())
          && "true".equalsIgnoreCase(codeValue(filter.getValue()));
    }
    if (expression instanceof final VclConjunction conjunction) {
      return conjunction.getOperands().stream().anyMatch(VclEvaluator::selectsInactive);
    }
    if (expression instanceof final VclDisjunction disjunction) {
      return disjunction.getOperands().stream().anyMatch(VclEvaluator::selectsInactive);
    }
    if (expression instanceof final VclExclusion exclusion) {
      return selectsInactive(exclusion.getIncluded());
    }
    if (expression instanceof final VclSystemScoped scoped) {
      return selectsInactive(scoped.getExpression());
    }
    return false;
  }
}
