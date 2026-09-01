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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.ecl.EclToVclTranslator;
import au.csiro.pathling.terminology.local.index.CodeSystemIndexes;
import au.csiro.pathling.terminology.local.index.ConceptDictionary;
import au.csiro.pathling.test.Rf2Mini;
import au.csiro.pathling.vcl.VclCodeValue;
import au.csiro.pathling.vcl.VclConjunction;
import au.csiro.pathling.vcl.VclExpression;
import au.csiro.pathling.vcl.VclFilter;
import au.csiro.pathling.vcl.VclFilterListValue;
import au.csiro.pathling.vcl.VclFilterOperator;
import au.csiro.pathling.vcl.VclRefsetMembership;
import au.csiro.pathling.vcl.VclWildcard;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.RoaringBitmap;

/**
 * Verifies the VCL evaluator against the rf2-mini indexes: hierarchy filter operators, set algebra,
 * wildcard, reference set membership, SNOMED property filters, and the active-only default versus
 * an explicit {@code inactive = true} selection.
 *
 * @author John Grimes
 */
class VclEvaluatorTest {

  private static CodeSystemIndexes indexes;
  private static ConceptDictionary dictionary;
  private static VclEvaluator evaluator;

  @BeforeAll
  static void setUp() {
    indexes = LocalTerminologyFixture.indexes();
    dictionary = indexes.dictionary();
    evaluator = new VclEvaluator(indexes, Rf2Mini.SNOMED_URI);
  }

  private Set<String> codes(final VclExpression expression) {
    final RoaringBitmap result = evaluator.evaluate(expression);
    final Set<String> codes = new HashSet<>();
    result.forEach((org.roaringbitmap.IntConsumer) dense -> codes.add(dictionary.code(dense)));
    return codes;
  }

  private static VclFilter concept(final VclFilterOperator operator, final String code) {
    return new VclFilter("concept", operator, new VclCodeValue(code));
  }

  @Test
  void descendantsOrSelfIncludesSelfAndExcludesInactive() {
    final Set<String> result = codes(concept(VclFilterOperator.IS_A, Rf2Mini.DIABETES));
    assertTrue(result.contains(Rf2Mini.DIABETES));
    assertTrue(result.contains(Rf2Mini.TYPE1_DIABETES));
    assertTrue(result.contains(Rf2Mini.TYPE2_WITH_COMPLICATION));
    // The inactive concept is excluded from implicit membership.
    assertFalse(result.contains(Rf2Mini.DIABETES_INACTIVE));
    // Hypertension is a sibling disorder, not under diabetes.
    assertFalse(result.contains(Rf2Mini.HYPERTENSION));
  }

  @Test
  void descendantOfExcludesSelf() {
    final Set<String> result = codes(concept(VclFilterOperator.DESCENDENT_OF, Rf2Mini.DIABETES));
    assertFalse(result.contains(Rf2Mini.DIABETES));
    assertTrue(result.contains(Rf2Mini.TYPE1_DIABETES));
  }

  @Test
  void childOfReturnsDirectChildrenOnly() {
    final Set<String> result = codes(concept(VclFilterOperator.CHILD_OF, Rf2Mini.DIABETES));
    assertTrue(result.contains(Rf2Mini.TYPE1_DIABETES));
    assertTrue(result.contains(Rf2Mini.GESTATIONAL_DIABETES));
    // A grandchild is not a direct child.
    assertFalse(result.contains(Rf2Mini.TYPE2_WITH_COMPLICATION));
  }

  @Test
  void generalizesReturnsAncestorsAndSelf() {
    final Set<String> result = codes(concept(VclFilterOperator.GENERALIZES, Rf2Mini.DIABETES));
    assertEquals(Set.of(Rf2Mini.DIABETES, Rf2Mini.DISORDER, Rf2Mini.ROOT_FINDING), result);
  }

  @Test
  void wildcardReturnsAllActiveConcepts() {
    assertEquals(Rf2Mini.CONCEPT_COUNT_20230601 - 1, codes(new VclWildcard()).size());
  }

  @Test
  void referenceSetMembershipReturnsMembers() {
    assertEquals(
        Set.of(Rf2Mini.TYPE1_DIABETES, Rf2Mini.TYPE2_DIABETES, Rf2Mini.GESTATIONAL_DIABETES),
        codes(new VclRefsetMembership(Rf2Mini.SIMPLE_REFSET)));
  }

  @Test
  void conjunctionIntersectsAndExclusionSubtracts() {
    // (<< DISORDER) MINUS (<< DIABETES): disorders that are not diabetes.
    final VclExpression expression =
        EclToVclTranslator.translate("<< " + Rf2Mini.DISORDER + " MINUS << " + Rf2Mini.DIABETES);
    final Set<String> result = codes(expression);
    assertTrue(result.contains(Rf2Mini.HYPERTENSION));
    assertFalse(result.contains(Rf2Mini.DIABETES));
    assertFalse(result.contains(Rf2Mini.TYPE1_DIABETES));
  }

  @Test
  void attributeConstraintSelectsByRelationshipValue() {
    // << DISORDER : finding site = pancreas.
    final VclExpression expression =
        new VclConjunction(
            List.of(
                concept(VclFilterOperator.IS_A, Rf2Mini.DISORDER),
                new VclFilter(
                    Rf2Mini.FINDING_SITE,
                    VclFilterOperator.IN,
                    new VclFilterListValue(
                        List.of(new au.csiro.pathling.vcl.VclCode(Rf2Mini.PANCREAS_STRUCTURE))))));
    assertEquals(
        Set.of(Rf2Mini.DIABETES, Rf2Mini.TYPE1_DIABETES, Rf2Mini.TYPE2_DIABETES),
        codes(expression));
  }

  @Test
  void attributeNameHierarchyMatchesTheAttributeSubtree() {
    // A hierarchy operator on the attribute name (<< finding site) must be accepted and evaluated.
    // The fixture has no descendant attribute types, so it matches the same concepts as the exact
    // attribute constraint rather than being rejected.
    final VclExpression expression =
        EclToVclTranslator.translate(
            "< "
                + Rf2Mini.DISORDER
                + " : << "
                + Rf2Mini.FINDING_SITE
                + " = "
                + Rf2Mini.PANCREAS_STRUCTURE);
    assertEquals(
        Set.of(Rf2Mini.DIABETES, Rf2Mini.TYPE1_DIABETES, Rf2Mini.TYPE2_DIABETES),
        codes(expression));
  }

  @Test
  void moduleIdFilterMatchesAllCoreConcepts() {
    final Set<String> result =
        codes(
            new VclFilter(
                "moduleId", VclFilterOperator.EQUALS, new VclCodeValue(Rf2Mini.CORE_MODULE)));
    assertEquals(Rf2Mini.CONCEPT_COUNT_20230601 - 1, result.size());
  }

  @Test
  void inactiveFilterSelectsInactiveConcepts() {
    final Set<String> result =
        codes(new VclFilter("inactive", VclFilterOperator.EQUALS, new VclCodeValue("true")));
    assertEquals(Set.of(Rf2Mini.DIABETES_INACTIVE), result);
  }

  @Test
  void sufficientlyDefinedFilterMatchesDefinedConcepts() {
    final Set<String> result =
        codes(
            new VclFilter(
                "sufficientlyDefined", VclFilterOperator.EQUALS, new VclCodeValue("true")));
    assertTrue(result.contains(Rf2Mini.DIABETES));
    assertTrue(result.contains(Rf2Mini.TYPE1_DIABETES));
    // Gestational diabetes was defined as primitive in the fixture.
    assertFalse(result.contains(Rf2Mini.GESTATIONAL_DIABETES));
  }
}
