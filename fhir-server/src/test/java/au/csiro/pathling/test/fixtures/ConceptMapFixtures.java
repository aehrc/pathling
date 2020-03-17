package au.csiro.pathling.test.fixtures;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.ConceptMap.ConceptMapGroupComponent;
import org.hl7.fhir.r4.model.ConceptMap.SourceElementComponent;
import org.hl7.fhir.r4.model.ConceptMap.TargetElementComponent;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;

public interface ConceptMapFixtures {

  public static final String SNOWMED_VERSION_DEF =
      "http://snomed.info/sct/32506021000036107/version/20200229";

  public static final ConceptMap CM_EMPTY = creatEmptyConceptMap();


  public static Coding newVersionedCoding(String system, String code, String version,
      String description) {
    Coding newCoding = new Coding(system, code, description);
    newCoding.setVersion(version);
    return newCoding;
  }

  // http://snomed.info/sct|444814009 -- subsumes --> http://snomed.info/sct|40055000
  public static final ConceptMap CM_SNOWMED_444814009_SUBSUMES_40055000 =
      createConceptMap(ConceptMapEntry.subsumesOf(
          new Coding("http://snomed.info/sct", "40055000", "Chronic sinusitis (disorder)"),
          new Coding("http://snomed.info/sct", "444814009", "Viral sinusitis (disorder)")));

  // http://snomed.info/sct|444814009 -- subsumes --> http://snomed.info/sct|40055000
  public static final ConceptMap CM_SNOWMED_444814009_SUBSUMES_40055000_VERSIONED =
      createConceptMap(ConceptMapEntry.subsumesOf(
          newVersionedCoding("http://snomed.info/sct", "40055000", SNOWMED_VERSION_DEF,
              "Chronic sinusitis (disorder)"),
          newVersionedCoding("http://snomed.info/sct", "444814009", SNOWMED_VERSION_DEF,
              "Viral sinusitis (disorder)")));


  public static class VersionedSystem {
    private final String system;
    private final String version;

    private VersionedSystem(String system, String version) {
      super();
      this.system = system;
      this.version = version;
    }

    public String getSystem() {
      return system;
    }

    public String getVersion() {
      return version;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((system == null) ? 0 : system.hashCode());
      result = prime * result + ((version == null) ? 0 : version.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      VersionedSystem other = (VersionedSystem) obj;
      if (system == null) {
        if (other.system != null)
          return false;
      } else if (!system.equals(other.system))
        return false;
      if (version == null) {
        if (other.version != null)
          return false;
      } else if (!version.equals(other.version))
        return false;
      return true;
    }

    public static VersionedSystem fromCoding(Coding coding) {
      return new VersionedSystem(coding.getSystem(), coding.getVersion());
    }
  }


  public static Pair<VersionedSystem, VersionedSystem> getVersionedSystems(
      ConceptMapEntry mapping) {
    return Pair.of(VersionedSystem.fromCoding(mapping.getSource()),
        VersionedSystem.fromCoding(mapping.getTarget()));
  }


  public static ConceptMap creatEmptyConceptMap() {
    final ConceptMap result = new ConceptMap();
    result.setStatus(PublicationStatus.ACTIVE);
    return result;
  }

  public static ConceptMap createConceptMap(ConceptMapEntry... mappings) {

    Map<Pair<VersionedSystem, VersionedSystem>, List<ConceptMapEntry>> mappingsBySystem =
        Stream.of(mappings).collect(Collectors.groupingBy(ConceptMapFixtures::getVersionedSystems));

    final ConceptMap result = creatEmptyConceptMap();
    mappingsBySystem.forEach((srcAndTarget, systemMappins) -> {
      final ConceptMapGroupComponent group = result.addGroup();
      group.setSource(srcAndTarget.getLeft().getSystem());
      group.setSourceVersion(srcAndTarget.getLeft().getVersion());
      group.setTarget(srcAndTarget.getRight().getSystem());
      group.setTargetVersion(srcAndTarget.getRight().getVersion());
      systemMappins.forEach(m -> {
        SourceElementComponent sourceElement = group.addElement();
        sourceElement.setCode(m.getSource().getCode());
        TargetElementComponent targetElemnt = sourceElement.addTarget();
        targetElemnt.setCode(m.getTarget().getCode());
        targetElemnt.setEquivalence(m.getEquivalence());
      });
    });
    return result;
  }
}
