package au.csiro.pathling.fhirpath.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.fhirpath.execution.DataRoot.ResolveRoot;
import au.csiro.pathling.fhirpath.execution.DataRoot.ResourceRoot;
import au.csiro.pathling.fhirpath.execution.DataRoot.ReverseResolveRoot;
import au.csiro.pathling.fhirpath.parser.Parser;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.Set;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Test;


class DataRootResolverTest {

  @Nonnull
  final Parser parser = new Parser();

  private @Nonnull Set<DataRoot> getDataRoots(ResourceType patient, String fhirpath) {
    final DataRootResolver resolver = new DataRootResolver(patient, FhirContext.forR4());
    return resolver.findDataRoots(
        parser.parse(fhirpath));
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
  void simpleMonomorphicResolve() {
    final Set<DataRoot> roots = getDataRoots(ResourceType.CONDITION,
        "encounter.resolve().id");
    roots.forEach(System.out::println);
    assertEquals(Set.of(
        ResourceRoot.of(ResourceType.CONDITION),
        ResolveRoot.of(ResourceRoot.of(ResourceType.CONDITION), ResourceType.ENCOUNTER, "encounter")
    ), roots);
  }

  @Test
  void monomorphicResolveWithResourceAndElementModifiers() {
    final Set<DataRoot> roots = getDataRoots(ResourceType.CONDITION,
        "where(id='3232').encounter.first().resolve().id");
    roots.forEach(System.out::println);
    assertEquals(Set.of(
        ResourceRoot.of(ResourceType.CONDITION),
        ResolveRoot.of(ResourceRoot.of(ResourceType.CONDITION), ResourceType.ENCOUNTER, "encounter")
    ), roots);
  }

  @Test
  void dualResolveToTheSameRoot() {
    final Set<DataRoot> roots = getDataRoots(ResourceType.ENCOUNTER,
        "where(subject.resolve().ofType(Patient).gender = 'male').episodeOfCare.resolve().status");
    roots.forEach(System.out::println);
    assertEquals(Set.of(
        ResourceRoot.of(ResourceType.ENCOUNTER),
        ResolveRoot.of(ResourceRoot.of(ResourceType.ENCOUNTER), ResourceType.PATIENT, "subject"),
        ResolveRoot.of(ResourceRoot.of(ResourceType.ENCOUNTER), ResourceType.EPISODEOFCARE,
            "episodeOfCare")
    ), roots);
  }


  @Test
  void resourceReferenceInForeingJoin() {
    final Set<DataRoot> roots = getDataRoots(ResourceType.PATIENT,
        "reverseResolve(Condition.subject).where(id=%resource.reverseResolve(Encounter.subject).id.first())");
    roots.forEach(System.out::println);
    assertEquals(Set.of(
        ResourceRoot.of(ResourceType.PATIENT),
        ReverseResolveRoot.of(ResourceRoot.of(ResourceType.PATIENT), ResourceType.CONDITION,
            "subject"),
        ReverseResolveRoot.of(ResourceRoot.of(ResourceType.PATIENT), ResourceType.ENCOUNTER,
            "subject")
    ), roots);
  }
  
}
