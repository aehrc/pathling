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

import au.csiro.pathling.vcl.VclCode;
import au.csiro.pathling.vcl.VclCodeValue;
import au.csiro.pathling.vcl.VclConjunction;
import au.csiro.pathling.vcl.VclDisjunction;
import au.csiro.pathling.vcl.VclExclusion;
import au.csiro.pathling.vcl.VclExpression;
import au.csiro.pathling.vcl.VclFilter;
import au.csiro.pathling.vcl.VclFilterOperator;
import au.csiro.pathling.vcl.VclFilterValue;
import au.csiro.pathling.vcl.VclStringValue;
import au.csiro.pathling.vcl.VclWildcard;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.hl7.fhir.r4.model.CanonicalType;
import org.hl7.fhir.r4.model.ValueSet;
import org.hl7.fhir.r4.model.ValueSet.ConceptSetComponent;
import org.hl7.fhir.r4.model.ValueSet.ConceptSetFilterComponent;
import org.hl7.fhir.r4.model.ValueSet.ValueSetExpansionContainsComponent;

/**
 * Translates a FHIR {@code ValueSet.compose} (or an enumerated expansion when there is no compose)
 * into the VCL expression model that the local evaluator understands. Each compose include and
 * exclude maps onto VCL constructs one to one - enumerated concepts to a code disjunction, filters
 * to VCL filters, nested value set references to the expression of the referenced value set - so a
 * single evaluator serves value sets, VCL URLs, and SNOMED implicit value sets alike.
 *
 * <p>The translation targets a single primary code system, taken from the first include (or the
 * first expansion member). Value sets that span multiple code systems resolve against that primary
 * system, which is sufficient for coding membership testing where the coding's system selects the
 * relevant includes.
 *
 * @author John Grimes
 */
public class ComposeTranslator {

  @Nonnull private final ValueSetStore valueSetStore;

  /**
   * Creates a translator that resolves nested value set references against a store.
   *
   * @param valueSetStore the catalogue of imported value sets
   */
  public ComposeTranslator(@Nonnull final ValueSetStore valueSetStore) {
    this.valueSetStore = valueSetStore;
  }

  /**
   * Translates a value set to its code system and membership expression.
   *
   * @param valueSet the value set resource
   * @return the translated expression, or empty if it references no known code system
   */
  @Nonnull
  public Optional<ComposeResult> translate(@Nonnull final ValueSet valueSet) {
    if (valueSet.hasCompose()) {
      return translateCompose(valueSet);
    }
    if (valueSet.hasExpansion()) {
      return translateExpansion(valueSet);
    }
    return Optional.empty();
  }

  @Nonnull
  private Optional<ComposeResult> translateCompose(@Nonnull final ValueSet valueSet) {
    final List<VclExpression> included = new ArrayList<>();
    String system = null;
    for (final ConceptSetComponent include : valueSet.getCompose().getInclude()) {
      final Optional<ComposeResult> part = translateSet(include);
      if (part.isPresent()) {
        system = system == null ? part.get().getSystemUrl() : system;
        included.add(part.get().getExpression());
      }
    }
    if (system == null || included.isEmpty()) {
      return Optional.empty();
    }
    VclExpression expression = or(included);
    final List<VclExpression> excluded = new ArrayList<>();
    for (final ConceptSetComponent exclude : valueSet.getCompose().getExclude()) {
      translateSet(exclude).ifPresent(part -> excluded.add(part.getExpression()));
    }
    if (!excluded.isEmpty()) {
      expression = new VclExclusion(expression, or(excluded));
    }
    return Optional.of(new ComposeResult(system, expression));
  }

  @Nonnull
  private Optional<ComposeResult> translateExpansion(@Nonnull final ValueSet valueSet) {
    String system = null;
    final List<VclExpression> codes = new ArrayList<>();
    for (final ValueSetExpansionContainsComponent contains :
        valueSet.getExpansion().getContains()) {
      if (system == null) {
        system = contains.getSystem();
      }
      if (contains.getSystem() != null && contains.getSystem().equals(system)) {
        codes.add(new VclCode(contains.getCode()));
      }
    }
    if (system == null || codes.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(new ComposeResult(system, or(codes)));
  }

  @Nonnull
  private Optional<ComposeResult> translateSet(@Nonnull final ConceptSetComponent set) {
    if (set.hasValueSet()) {
      return translateNestedValueSets(set);
    }
    final String system = set.getSystem();
    if (system == null) {
      return Optional.empty();
    }
    final List<VclExpression> parts = new ArrayList<>();
    if (!set.getConcept().isEmpty()) {
      final List<VclExpression> codes = new ArrayList<>();
      set.getConcept().forEach(concept -> codes.add(new VclCode(concept.getCode())));
      parts.add(or(codes));
    }
    if (!set.getFilter().isEmpty()) {
      final List<VclExpression> filters = new ArrayList<>();
      set.getFilter().forEach(filter -> filters.add(translateFilter(filter)));
      parts.add(and(filters));
    }
    if (parts.isEmpty()) {
      // No concepts or filters means the whole code system is included.
      parts.add(new VclWildcard());
    }
    return Optional.of(new ComposeResult(system, or(parts)));
  }

  @Nonnull
  private Optional<ComposeResult> translateNestedValueSets(@Nonnull final ConceptSetComponent set) {
    final List<VclExpression> expressions = new ArrayList<>();
    String system = null;
    for (final CanonicalType reference : set.getValueSet()) {
      final String url = stripVersion(reference.getValue());
      final String version = versionOf(reference.getValue());
      final Optional<ComposeResult> nested =
          valueSetStore.resolve(url, version).flatMap(this::translate);
      if (nested.isPresent()) {
        system = system == null ? nested.get().getSystemUrl() : system;
        expressions.add(nested.get().getExpression());
      }
    }
    if (system == null || expressions.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(new ComposeResult(system, or(expressions)));
  }

  @Nonnull
  private VclFilter translateFilter(@Nonnull final ConceptSetFilterComponent filter) {
    final VclFilterOperator operator = mapOperator(filter.getOp());
    final VclFilterValue value =
        operator == VclFilterOperator.REGEX
            ? new VclStringValue(filter.getValue())
            : new VclCodeValue(filter.getValue());
    return new VclFilter(filter.getProperty(), operator, value);
  }

  @Nonnull
  private static VclFilterOperator mapOperator(@Nonnull final ValueSet.FilterOperator op) {
    return switch (op) {
      case ISA -> VclFilterOperator.IS_A;
      case DESCENDENTOF -> VclFilterOperator.DESCENDENT_OF;
      case ISNOTA -> VclFilterOperator.IS_NOT_A;
      case REGEX -> VclFilterOperator.REGEX;
      case IN -> VclFilterOperator.IN;
      case NOTIN -> VclFilterOperator.NOT_IN;
      case GENERALIZES -> VclFilterOperator.GENERALIZES;
      case EXISTS -> VclFilterOperator.EXISTS;
      default -> VclFilterOperator.EQUALS;
    };
  }

  @Nonnull
  private static String stripVersion(@Nonnull final String reference) {
    final int pipe = reference.indexOf('|');
    return pipe < 0 ? reference : reference.substring(0, pipe);
  }

  private static String versionOf(@Nonnull final String reference) {
    final int pipe = reference.indexOf('|');
    return pipe < 0 ? null : reference.substring(pipe + 1);
  }

  @Nonnull
  private static VclExpression or(@Nonnull final List<VclExpression> expressions) {
    return expressions.size() == 1 ? expressions.get(0) : new VclDisjunction(expressions);
  }

  @Nonnull
  private static VclExpression and(@Nonnull final List<VclExpression> expressions) {
    return expressions.size() == 1 ? expressions.get(0) : new VclConjunction(expressions);
  }
}
