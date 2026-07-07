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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.vcl.VclAttributeConstraint;
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
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Golden tests for the ECL to VCL translator. Each supported construct must map to the expected VCL
 * model node (per research.md R4), and each recognised-but-unsupported construct must raise an
 * {@link UnsupportedEclConstructError} naming it. Malformed input reports its position.
 *
 * @author John Grimes
 */
class EclToVclTranslatorTest {

  private static final String DIABETES = "73211009";
  private static final String TYPE1 = "46635009";
  private static final String CLINICAL_FINDING = "404684003";
  private static final String FINDING_SITE = "363698007";
  private static final String PANCREAS = "181277001";
  private static final String SAME_AS = "900000000000527005";

  private static VclFilter concept(final VclFilterOperator operator, final String code) {
    return new VclFilter("concept", operator, new VclCodeValue(code));
  }

  @Test
  void translatesConceptReferences() {
    assertEquals(new VclCode(DIABETES), EclToVclTranslator.translate(DIABETES));
    // The display term is discarded.
    assertEquals(
        new VclCode(DIABETES), EclToVclTranslator.translate(DIABETES + " |Diabetes mellitus|"));
  }

  @Test
  void translatesWildcard() {
    assertEquals(new VclWildcard(), EclToVclTranslator.translate("*"));
  }

  @Test
  void translatesHierarchyOperators() {
    assertEquals(
        concept(VclFilterOperator.DESCENDENT_OF, DIABETES),
        EclToVclTranslator.translate("< " + DIABETES));
    assertEquals(
        concept(VclFilterOperator.IS_A, DIABETES), EclToVclTranslator.translate("<< " + DIABETES));
    assertEquals(
        concept(VclFilterOperator.CHILD_OF, DIABETES),
        EclToVclTranslator.translate("<! " + DIABETES));
    assertEquals(
        concept(VclFilterOperator.GENERALIZES, DIABETES),
        EclToVclTranslator.translate(">> " + DIABETES));
    // Ancestors only: ancestors-or-self minus self.
    assertEquals(
        new VclExclusion(concept(VclFilterOperator.GENERALIZES, DIABETES), new VclCode(DIABETES)),
        EclToVclTranslator.translate("> " + DIABETES));
    // Parents only: navigate the parent property.
    assertEquals(
        new VclNavigation(new VclCodeValue(DIABETES), "parent"),
        EclToVclTranslator.translate(">! " + DIABETES));
  }

  @Test
  void translatesReferenceSetMembership() {
    assertEquals(new VclRefsetMembership(SAME_AS), EclToVclTranslator.translate("^ " + SAME_AS));
  }

  @Test
  void translatesSetOperators() {
    assertEquals(
        new VclConjunction(List.of(new VclCode(DIABETES), new VclCode(TYPE1))),
        EclToVclTranslator.translate(DIABETES + " AND " + TYPE1));
    assertEquals(
        new VclDisjunction(List.of(new VclCode(DIABETES), new VclCode(TYPE1))),
        EclToVclTranslator.translate(DIABETES + " OR " + TYPE1));
    assertEquals(
        new VclExclusion(new VclCode(DIABETES), new VclCode(TYPE1)),
        EclToVclTranslator.translate(DIABETES + " MINUS " + TYPE1));
  }

  @Test
  void translatesAttributeConstraints() {
    // Exact attribute value.
    final VclExpression exact =
        EclToVclTranslator.translate(
            "<< " + CLINICAL_FINDING + " : " + FINDING_SITE + " = " + PANCREAS);
    assertEquals(
        new VclConjunction(
            List.of(
                concept(VclFilterOperator.IS_A, CLINICAL_FINDING),
                new VclAttributeConstraint(
                    FINDING_SITE, true, false, false, new VclCode(PANCREAS)))),
        exact);

    // Hierarchy on the attribute value.
    final VclExpression hierarchyValue =
        EclToVclTranslator.translate(
            "<< " + CLINICAL_FINDING + " : " + FINDING_SITE + " = << " + PANCREAS);
    assertEquals(
        new VclConjunction(
            List.of(
                concept(VclFilterOperator.IS_A, CLINICAL_FINDING),
                new VclAttributeConstraint(
                    FINDING_SITE, true, false, false, concept(VclFilterOperator.IS_A, PANCREAS)))),
        hierarchyValue);
  }

  @Test
  void translatesHierarchyOnTheAttributeName() {
    // << on the attribute name broadens over descendant attribute types.
    final VclExpression descendantsOrSelf =
        EclToVclTranslator.translate(
            "<< " + CLINICAL_FINDING + " : << " + FINDING_SITE + " = " + PANCREAS);
    assertEquals(
        new VclConjunction(
            List.of(
                concept(VclFilterOperator.IS_A, CLINICAL_FINDING),
                new VclAttributeConstraint(
                    FINDING_SITE, true, true, false, new VclCode(PANCREAS)))),
        descendantsOrSelf);

    // < on the attribute name matches descendant attribute types only.
    final VclExpression descendantsOnly =
        EclToVclTranslator.translate(
            "<< " + CLINICAL_FINDING + " : < " + FINDING_SITE + " = " + PANCREAS);
    assertEquals(
        new VclConjunction(
            List.of(
                concept(VclFilterOperator.IS_A, CLINICAL_FINDING),
                new VclAttributeConstraint(
                    FINDING_SITE, false, true, false, new VclCode(PANCREAS)))),
        descendantsOnly);
  }

  @Test
  void translatesNegatedAttributeConstraint() {
    final VclExpression negated =
        EclToVclTranslator.translate(
            "<< " + CLINICAL_FINDING + " : " + FINDING_SITE + " != " + PANCREAS);
    assertEquals(
        new VclConjunction(
            List.of(
                concept(VclFilterOperator.IS_A, CLINICAL_FINDING),
                new VclAttributeConstraint(
                    FINDING_SITE, true, false, true, new VclCode(PANCREAS)))),
        negated);
  }

  @Test
  void translatesDottedReverseAttribute() {
    assertEquals(
        new VclNavigation(
            new VclFilterListValue(
                List.of(concept(VclFilterOperator.DESCENDENT_OF, CLINICAL_FINDING))),
            FINDING_SITE),
        EclToVclTranslator.translate("< " + CLINICAL_FINDING + " . " + FINDING_SITE));
  }

  @Test
  void rejectsRoleGroups() {
    final UnsupportedEclConstructError e =
        assertThrows(
            UnsupportedEclConstructError.class,
            () ->
                EclToVclTranslator.translate(
                    "<< " + CLINICAL_FINDING + " : { " + FINDING_SITE + " = " + PANCREAS + " }"));
    assertTrue(e.getMessage().toLowerCase().contains("group"));
  }

  @Test
  void rejectsCardinality() {
    final UnsupportedEclConstructError e =
        assertThrows(
            UnsupportedEclConstructError.class,
            () ->
                EclToVclTranslator.translate(
                    "<< " + CLINICAL_FINDING + " : [1..3] " + FINDING_SITE + " = " + PANCREAS));
    assertTrue(e.getMessage().toLowerCase().contains("cardinality"));
  }

  @Test
  void rejectsConcreteValues() {
    final UnsupportedEclConstructError e =
        assertThrows(
            UnsupportedEclConstructError.class,
            () ->
                EclToVclTranslator.translate(
                    "<< " + CLINICAL_FINDING + " : " + FINDING_SITE + " = #5"));
    assertTrue(e.getMessage().toLowerCase().contains("concrete"));
  }

  @Test
  void rejectsTermFilters() {
    final UnsupportedEclConstructError e =
        assertThrows(
            UnsupportedEclConstructError.class,
            () -> EclToVclTranslator.translate("<< " + DIABETES + " {{ term = \"type 1\" }}"));
    assertTrue(e.getMessage().contains("{{"));
  }

  @Test
  void rejectsHistorySupplements() {
    final UnsupportedEclConstructError e =
        assertThrows(
            UnsupportedEclConstructError.class,
            () -> EclToVclTranslator.translate("<< " + DIABETES + " {{ + HISTORY-MIN }}"));
    assertTrue(e.getMessage().toLowerCase().contains("history"));
  }

  @Test
  void rejectsReverseFlag() {
    assertThrows(
        UnsupportedEclConstructError.class,
        () ->
            EclToVclTranslator.translate(
                "<< " + CLINICAL_FINDING + " : R " + FINDING_SITE + " = " + PANCREAS));
  }

  @Test
  void rejectsChildOrSelfAndParentOrSelf() {
    assertThrows(
        UnsupportedEclConstructError.class, () -> EclToVclTranslator.translate("<<! " + DIABETES));
    assertThrows(
        UnsupportedEclConstructError.class, () -> EclToVclTranslator.translate(">>! " + DIABETES));
  }

  @Test
  void reportsParseErrorsWithPosition() {
    final EclParseException e =
        assertThrows(
            EclParseException.class, () -> EclToVclTranslator.translate(DIABETES + " AND"));
    assertTrue(e.getMessage().contains("position"));
  }
}
