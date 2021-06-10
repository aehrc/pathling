/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.terminology;

import static au.csiro.pathling.test.helpers.TerminologyHelpers.newVersionedCoding;
import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.test.fixtures.ConceptMapBuilder;
import au.csiro.pathling.test.fixtures.RelationBuilder;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.stream.Stream;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("UnitTest")
public class ClosureMappingTest {

  private static final Coding CODING_1_1_1 = newVersionedCoding("system1", "code1", "version1",
      "");
  private static final Coding CODING_1_2_1 = newVersionedCoding("system1", "code2", "version1",
      "");
  private static final Coding CODING_1_3_1 = newVersionedCoding("system1", "code3", "version1",
      "");

  private static final Coding CODING_2_1_1 = newVersionedCoding("system2", "code2", "version1",
      "");
  private static final Coding CODING_3_1_1 = newVersionedCoding("system3", "code2", "version1",
      "");


  private final static Relation EMPTY_RELATION = RelationBuilder.empty().build();

  @Test
  public void toRelationFromEmptyMap() {
    final Relation emptyRelation = ClosureMapping
        .relationFromConceptMap(ConceptMapBuilder.empty().build());
    assertEquals(EMPTY_RELATION, emptyRelation);
  }

  @Test
  public void toRelationFromComplexMap() {
    // system1|code2 -- subsumes --> system1|code1
    // system1|code3 -- subsumes --> system1|code1
    // system1|code3 -- isSubsumedBy --> system1|code2 (equiv: system1|code2 -- subsumes --> system1|code3)
    // system1|code3 -- equal --> system2|code1 (equiv: system1|code3 -- subsumes --> system2|code1 and 
    //                                                     system2|code1 -- subsumes --> system1|code3)
    // system1|code1 -- unmatched --> system3|code1 (equiv: NONE)
    final ConceptMap complexMap = ConceptMapBuilder.empty()
        .withSubsumes(CODING_1_1_1, CODING_1_2_1)
        .withSubsumes(CODING_1_1_1, CODING_1_3_1)
        .withSpecializes(CODING_1_2_1, CODING_1_3_1)
        .with(CODING_2_1_1, CODING_1_3_1, ConceptMapEquivalence.EQUAL)
        .with(CODING_3_1_1, CODING_1_1_1, ConceptMapEquivalence.UNMATCHED)
        .build();

    final Relation expectedRelation = RelationBuilder.empty()
        .add(CODING_1_3_1, CODING_1_1_1, CODING_2_1_1)
        .add(CODING_1_2_1, CODING_1_1_1, CODING_1_3_1)
        .add(CODING_2_1_1, CODING_1_3_1)
        .build();
    assertEquals(expectedRelation, ClosureMapping.relationFromConceptMap(complexMap));
  }

  @Test
  public void toRelationIgnoresUnknownEquivalenceTypes() {
    final Collection<ConceptMapEquivalence> validRelations = new HashSet<>(Arrays.asList(
        ConceptMapEquivalence.SPECIALIZES, ConceptMapEquivalence.SUBSUMES,
        ConceptMapEquivalence.EQUAL, ConceptMapEquivalence.UNMATCHED));

    Stream.of(ConceptMapEquivalence.values()).filter(e -> !validRelations.contains(e))
        .forEach(e -> {
          final ConceptMap invalidMap = ConceptMapBuilder.empty()
              .with(CODING_1_1_1, CODING_1_1_1, e).build();
          assertEquals(EMPTY_RELATION, ClosureMapping.relationFromConceptMap(invalidMap));
        });
  }
}
