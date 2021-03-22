/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.terminology;

import static au.csiro.pathling.test.fixtures.ConceptMapFixtures.CM_EMPTY;
import static au.csiro.pathling.test.fixtures.ConceptMapFixtures.createConceptMap;
import static au.csiro.pathling.test.fixtures.ConceptMapFixtures.newVersionedCoding;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import au.csiro.pathling.test.fixtures.ConceptMapEntry;
import java.util.*;
import java.util.stream.Stream;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("UnitTest")
public class ClosureMappingTest {

  private static final Coding CODING_1_1_1 = newVersionedCoding("system1", "code1", "version1",
      null);
  private static final Coding CODING_1_2_1 = newVersionedCoding("system1", "code2", "version1",
      null);
  private static final Coding CODING_1_3_1 = newVersionedCoding("system1", "code3", "version1",
      null);

  private static final Coding CODING_2_1_1 = newVersionedCoding("system2", "code2", "version1",
      null);
  private static final Coding CODING_3_1_1 = newVersionedCoding("system3", "code2", "version1",
      null);

  @Test
  public void testMapsEmptyConceptMapCorrectly() {
    final Relation relation = ClosureMapping.closureFromConceptMap(CM_EMPTY);
    assertTrue(relation.getMappings().isEmpty());
  }

  @Test
  public void testIgnoresUnknownEquivalenceTypes() {
    final Collection<ConceptMapEquivalence> validRelations = new HashSet<>(Arrays.asList(
        ConceptMapEquivalence.SPECIALIZES, ConceptMapEquivalence.SUBSUMES,
        ConceptMapEquivalence.EQUAL, ConceptMapEquivalence.UNMATCHED));

    Stream.of(ConceptMapEquivalence.values()).filter(e -> !validRelations.contains(e))
        .forEach(e -> {
          final ConceptMap invalidMap = createConceptMap(
              ConceptMapEntry.of(CODING_1_1_1, CODING_1_1_1, e));
          assertTrue(ClosureMapping.closureFromConceptMap(invalidMap).getMappings().isEmpty());
        });
  }

  @Test
  public void testComplexResponse() {
    // system1|code2 -- subsumes --> system1|code1
    // system1|code3 -- subsumes --> system1|code1
    // system1|code3 -- isSubsumedBy --> system1|code2 (equiv: system1|code2 -- subsumes --> system1|code3)
    // system1|code3 -- equal --> system2|code1 (equiv: system1|code3 -- subsumes --> system2|code1 and 
    //                                                     system2|code1 -- subsumes --> system1|code3)
    // system1|code1 -- unmatched --> system3|code1 (equiv: NONE)
    final ConceptMap complexMap = createConceptMap(
        ConceptMapEntry.subsumesOf(CODING_1_1_1, CODING_1_2_1),
        ConceptMapEntry.subsumesOf(CODING_1_1_1, CODING_1_3_1),
        ConceptMapEntry.specializesOf(CODING_1_2_1, CODING_1_3_1),
        ConceptMapEntry.of(CODING_2_1_1, CODING_1_3_1, ConceptMapEquivalence.EQUAL),
        ConceptMapEntry.of(CODING_3_1_1, CODING_1_1_1, ConceptMapEquivalence.UNMATCHED)
    );

    final Map<SimpleCoding, List<SimpleCoding>> expectedMappings = new HashMap<>();
    expectedMappings.put(new SimpleCoding(CODING_1_3_1),
        Arrays.asList(new SimpleCoding(CODING_1_1_1), new SimpleCoding(CODING_2_1_1)));
    expectedMappings.put(new SimpleCoding(CODING_1_2_1),
        Arrays.asList(new SimpleCoding(CODING_1_1_1), new SimpleCoding(CODING_1_3_1)));
    expectedMappings.put(new SimpleCoding(CODING_2_1_1),
        Collections.singletonList(new SimpleCoding(CODING_1_3_1)));
    assertEquals(expectedMappings, ClosureMapping.closureFromConceptMap(complexMap).getMappings());
  }
}
