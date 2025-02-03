package au.csiro.pathling.fhirpath.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.fhirpath.execution.DataRoot.ResolveRoot;
import au.csiro.pathling.fhirpath.execution.DataRoot.ResourceRoot;
import au.csiro.pathling.fhirpath.execution.DataRoot.ReverseResolveRoot;
import java.util.List;
import java.util.Set;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Test;

class JoinSetTest {


  @Test
  void testSingleResourcePath() {

    final List<DataRoot> result = JoinSet.toPath(
        ResourceRoot.of(ResourceType.PATIENT));
    assertEquals(List.of(ResourceRoot.of(ResourceType.PATIENT)), result);
  }

  @Test
  void testReverseResolvePath() {
    final List<DataRoot> result = JoinSet.toPath(
        ReverseResolveRoot.of(ResourceRoot.of(ResourceType.PATIENT), ResourceType.CONDITION,
            "subject"));
    assertEquals(List.of(
        ResourceRoot.of(ResourceType.PATIENT),
        ReverseResolveRoot.of(ResourceRoot.of(ResourceType.PATIENT), ResourceType.CONDITION,
            "subject")
    ), result);
  }

  @Test
  void singleResourceRoots() {
    final List<JoinSet> result = JoinSet.mergeRoots(
        Set.of(
            ResourceRoot.of(ResourceType.PATIENT),
            ResourceRoot.of(ResourceType.CONDITION)
        )
    );
    System.out.println(result);
  }

  @Test
  void singleReverseResolvePath() {
    final List<JoinSet> result = JoinSet.mergeRoots(
        Set.of(
            ReverseResolveRoot.of(ResourceRoot.of(ResourceType.PATIENT), ResourceType.CONDITION,
                "subject")
        )
    );
    System.out.println(result);
  }
  
  @Test
  void nestedRoots() {
    final List<JoinSet> result = JoinSet.mergeRoots(
        Set.of(
            ResourceRoot.of(ResourceType.PATIENT),
            ReverseResolveRoot.of(ResourceRoot.of(ResourceType.PATIENT), ResourceType.CONDITION,
                "subject"),
            ReverseResolveRoot.of(ResourceRoot.of(ResourceType.PATIENT), ResourceType.ENCOUNTER,
                "subject")
        )
    );
    System.out.println(result);
  }
  @Test
  void complexNestedRoots() {

    final ResourceRoot mainRoot1 = ResourceRoot.of(ResourceType.PATIENT);
    final DataRoot root_1_1 = ReverseResolveRoot.of(mainRoot1, ResourceType.CONDITION, "subject");
    final DataRoot root_1_2 = ReverseResolveRoot.of(mainRoot1, ResourceType.ENCOUNTER, "subject");
    final DataRoot root_1_2_1 = ResolveRoot.of(root_1_2, ResourceType.OBSERVATION, "observations");

    System.out.println(JoinSet.toPath(root_1_2_1));
    
    
    final List<JoinSet> result = JoinSet.mergeRoots(
        Set.of(
            mainRoot1,
            root_1_1,
            root_1_2,
            root_1_2_1
        )
    );
    
    
    System.out.println(result);
  }

}


