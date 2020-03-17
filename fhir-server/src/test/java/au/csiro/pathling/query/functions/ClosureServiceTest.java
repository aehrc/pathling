package au.csiro.pathling.query.functions;

import static au.csiro.pathling.test.fixtures.ConceptMapFixtures.CM_EMPTY;
import static au.csiro.pathling.test.fixtures.ConceptMapFixtures.createConceptMap;
import static au.csiro.pathling.test.fixtures.ConceptMapFixtures.newVersionedCoding;
import static org.assertj.core.api.Assertions.assertThat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import org.junit.Test;
import au.csiro.pathling.encoding.SimpleCoding;
import au.csiro.pathling.test.fixtures.ConceptMapEntry;

public class ClosureServiceTest {

  private static final Coding CODING_1_1_1 = newVersionedCoding("system1", "code1", "version1", null);
  private static final Coding CODING_1_2_1 = newVersionedCoding("system1", "code2", "version1", null);
  private static final Coding CODING_1_3_1 = newVersionedCoding("system1", "code3", "version1", null);

  private static final Coding CODING_2_1_1 = newVersionedCoding("system2", "code2", "version1", null);
  private static final Coding CODING_3_1_1 = newVersionedCoding("system3", "code2", "version1", null);

  @Test
  public void testMapsEmptyConceptMapCorrectly() {
    Closure closure = ClosureService.conceptMapToClosure(CM_EMPTY);
    assertThat(closure.getMappings()).isEmpty();
  }

  @Test
  public void testIgnoresUnknownEquivalenceTypes() {
    Set<ConceptMapEquivalence> validRelations = new HashSet<ConceptMapEquivalence>(Arrays.asList(
        ConceptMapEquivalence.SPECIALIZES, ConceptMapEquivalence.SUBSUMES, ConceptMapEquivalence.EQUAL, ConceptMapEquivalence.UNMATCHED));
    
    Stream.of(ConceptMapEquivalence.values()).filter(e -> !validRelations.contains(e)).forEach(e -> {
      ConceptMap invalidMap = createConceptMap(ConceptMapEntry.of(CODING_1_1_1, CODING_1_1_1, e));
      assertThat(ClosureService.conceptMapToClosure(invalidMap).getMappings()).isEmpty();
    });
  }
 
  @Test
  public void testComplexReponse() {
    // system1|code2 -- subsumes --> system1|code1
    // system1|code3 -- subsumes --> system1|code1
    // system1|code3 -- isSubsumedBy --> system1|code2 (equiv: system1|code2 -- subsumes --> system1|code3)
    // system1|code3 -- equal --> system2|code1 (equiv: system1|code3 -- subsumes --> system2|code1 and 
    //                                                     system2|code1 -- subsumes --> system1|code3)
    // system1|code1 -- unmatched --> system3|code1 (equiv: NONE)
    ConceptMap complexMap = createConceptMap(
          ConceptMapEntry.subsumesOf(CODING_1_1_1,CODING_1_2_1), 
          ConceptMapEntry.subsumesOf(CODING_1_1_1, CODING_1_3_1),
          ConceptMapEntry.specializesOf(CODING_1_2_1,CODING_1_3_1),
          ConceptMapEntry.of(CODING_2_1_1, CODING_1_3_1, ConceptMapEquivalence.EQUAL), 
          ConceptMapEntry.of(CODING_3_1_1, CODING_1_1_1, ConceptMapEquivalence.UNMATCHED) 
    );
    
    HashMap<SimpleCoding, List<SimpleCoding>> expectedMappings = new HashMap<SimpleCoding, List<SimpleCoding>>();
    expectedMappings.put(new SimpleCoding(CODING_1_3_1), 
        Arrays.asList(new SimpleCoding(CODING_1_1_1)));
    expectedMappings.put(new SimpleCoding(CODING_1_2_1), 
        Arrays.asList(new SimpleCoding(CODING_1_1_1), new SimpleCoding(CODING_1_3_1)));
    expectedMappings.put(new SimpleCoding(CODING_1_3_1), 
        Arrays.asList(new SimpleCoding(CODING_2_1_1)));
    expectedMappings.put(new SimpleCoding(CODING_2_1_1), 
        Arrays.asList(new SimpleCoding(CODING_1_3_1)));
    assertThat(ClosureService.conceptMapToClosure(complexMap).getMappings()).isEqualTo(expectedMappings);
  }
}
