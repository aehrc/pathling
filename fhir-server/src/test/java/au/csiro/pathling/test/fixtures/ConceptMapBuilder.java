package au.csiro.pathling.test.fixtures;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang3.tuple.Pair;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.ConceptMap.ConceptMapGroupComponent;
import org.hl7.fhir.r4.model.ConceptMap.SourceElementComponent;
import org.hl7.fhir.r4.model.ConceptMap.TargetElementComponent;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;

public class ConceptMapBuilder {

  @Data
  class ConceptMapEntry {

    private final Coding source;
    private final Coding target;
    private final ConceptMapEquivalence equivalence;
  }

  private final List<ConceptMapEntry> entries = new ArrayList<>();

  private ConceptMapBuilder() {
  }

  @EqualsAndHashCode
  private static class VersionedSystem {

    private final String system;
    private final String version;

    private VersionedSystem(final String system, final String version) {
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

    public static VersionedSystem fromCoding(final Coding coding) {
      return new VersionedSystem(coding.getSystem(), coding.getVersion());
    }
  }

  private static Pair<VersionedSystem, VersionedSystem> getVersionedSystems(
      final ConceptMapEntry mapping) {
    return Pair.of(VersionedSystem.fromCoding(mapping.getSource()),
        VersionedSystem.fromCoding(mapping.getTarget()));
  }

  public ConceptMapBuilder withSubsumes(@Nonnull final Coding src, @Nonnull final Coding target) {
    return with(src, target, ConceptMapEquivalence.SUBSUMES);
  }

  public ConceptMapBuilder withSpecializes(@Nonnull final Coding src,
      @Nonnull final Coding target) {
    return with(src, target, ConceptMapEquivalence.SPECIALIZES);
  }

  public ConceptMapBuilder with(@Nonnull final Coding src, @Nonnull final Coding target,
      @Nonnull final ConceptMapEquivalence equivalence) {
    entries.add(new ConceptMapEntry(src, target, equivalence));
    return this;
  }

  public ConceptMap build() {
    final Map<Pair<VersionedSystem, VersionedSystem>, List<ConceptMapEntry>> mappingsBySystem =
        entries.stream().collect(Collectors.groupingBy(ConceptMapBuilder::getVersionedSystems));

    final ConceptMap result = new ConceptMap();
    result.setStatus(PublicationStatus.ACTIVE);
    mappingsBySystem.forEach((srcAndTarget, systemMappins) -> {
      final ConceptMapGroupComponent group = result.addGroup();
      group.setSource(srcAndTarget.getLeft().getSystem());
      group.setSourceVersion(srcAndTarget.getLeft().getVersion());
      group.setTarget(srcAndTarget.getRight().getSystem());
      group.setTargetVersion(srcAndTarget.getRight().getVersion());
      systemMappins.forEach(m -> {
        final SourceElementComponent sourceElement = group.addElement();
        sourceElement.setCode(m.getSource().getCode());
        final TargetElementComponent targetElement = sourceElement.addTarget();
        targetElement.setCode(m.getTarget().getCode());
        targetElement.setEquivalence(m.getEquivalence());
      });
    });
    return result;
  }

  public static ConceptMapBuilder empty() {
    return new ConceptMapBuilder();
  }

}
