package au.csiro.pathling.fhirpath.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.fhirpath.execution.DataRoot.ResolveRoot;
import au.csiro.pathling.fhirpath.execution.DataRoot.ResourceRoot;
import au.csiro.pathling.fhirpath.execution.DataRoot.ReverseResolveRoot;
import au.csiro.pathling.fhirpath.parser.Parser;
import jakarta.annotation.Nonnull;
import java.util.Set;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Test;


class DataRootResolverTest {

  @Nonnull
  final DataRootResolver resolver = new DataRootResolver();

  @Nonnull
  final Parser parser = new Parser();

  private @Nonnull Set<DataRoot> getDataRoots(ResourceType patient, String id) {
    return resolver.findDataRoots(patient,
        parser.parse(id));
  }

  @Test
  void testFindSimplePath() {
    final Set<DataRoot> roots = getDataRoots(ResourceType.PATIENT, "id");
    assertEquals(Set.of(ResourceRoot.of(ResourceType.PATIENT)), roots);
  }


  @Test
  void testFindSimplePathWithResourceReference() {
    final Set<DataRoot> roots = getDataRoots(ResourceType.CONDITION, "%resource.id");
    assertEquals(Set.of(ResourceRoot.of(ResourceType.CONDITION)), roots);
  }

  @Test
  void testSimpleReverseResolve() {
    final Set<DataRoot> roots = getDataRoots(ResourceType.PATIENT,
        "reverseResolve(Condition.subject).count()");
    assertEquals(Set.of(
        ResourceRoot.of(ResourceType.PATIENT),
        ReverseResolveRoot.ofResource(ResourceType.PATIENT, ResourceType.CONDITION,
            "subject")
    ), roots);
  }

  @Test
  void testNestedReverseResolve() {
    final Set<DataRoot> roots = getDataRoots(ResourceType.PATIENT,
        "reverseResolve(Encounter.subject).reverseResolve(Condition.encounter)");

    roots.forEach(System.out::println);
    assertEquals(Set.of(
        ResourceRoot.of(ResourceType.PATIENT),
        ReverseResolveRoot.ofResource(ResourceType.PATIENT, ResourceType.ENCOUNTER,
            "subject"),
        ReverseResolveRoot.of(
            ReverseResolveRoot.ofResource(ResourceType.PATIENT, ResourceType.ENCOUNTER,
                "subject"), ResourceType.CONDITION, "encounter")
    ), roots);
  }

  @Test
  void multipleReverseResolveInOperator() {
    final Set<DataRoot> roots = getDataRoots(ResourceType.PATIENT,
        "reverseResolve(Condition.subject).code.coding.count() + reverseResolve(Condition.subject).id.count()");
    assertEquals(Set.of(
        ResourceRoot.of(ResourceType.PATIENT),
        ReverseResolveRoot.ofResource(ResourceType.PATIENT, ResourceType.CONDITION, "subject")
    ), roots);
  }


  @Test
  void reverseResolveInWhereWithChildResourceCountFunction() {
    final Set<DataRoot> roots = getDataRoots(ResourceType.PATIENT,
        "where(reverseResolve(Encounter.subject).count() = 2).id");
    assertEquals(Set.of(
        ResourceRoot.of(ResourceType.PATIENT),
        ReverseResolveRoot.ofResource(ResourceType.PATIENT, ResourceType.ENCOUNTER, "subject")
    ), roots);
  }
  
  @Test
  void simpleResolve() {
    final Set<DataRoot> roots = getDataRoots(ResourceType.CONDITION,
        "encounter.resolve().id");
    assertEquals(Set.of(
        ResourceRoot.of(ResourceType.CONDITION),
        ResolveRoot.of(ResourceRoot.of(ResourceType.CONDITION), ResourceType.ENCOUNTER, "encounter")
    ), roots);
  }
}
