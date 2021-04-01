/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.fixtures;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
  private static class ConceptMapEntry {

    @Nonnull
    private final Coding source;
    @Nonnull
    private final Coding target;
    @Nonnull
    private final ConceptMapEquivalence equivalence;
  }

  @Nonnull
  private final Collection<ConceptMapEntry> entries = new ArrayList<>();

  private ConceptMapBuilder() {
  }

  @EqualsAndHashCode
  private static class VersionedSystem {

    @Nonnull
    private final String system;

    @Nullable
    private final String version;

    private VersionedSystem(@Nonnull final String system, @Nullable final String version) {
      super();
      this.system = system;
      this.version = version;
    }

    @Nonnull
    public String getSystem() {
      return system;
    }

    @Nullable
    public String getVersion() {
      return version;
    }

    private static VersionedSystem fromCoding(@Nonnull final Coding coding) {
      return new VersionedSystem(coding.getSystem(), coding.getVersion());
    }
  }

  @Nonnull
  private static Pair<VersionedSystem, VersionedSystem> getVersionedSystems(
      @Nonnull final ConceptMapEntry mapping) {
    return Pair.of(VersionedSystem.fromCoding(mapping.getSource()),
        VersionedSystem.fromCoding(mapping.getTarget()));
  }

  @Nonnull
  public ConceptMapBuilder withSubsumes(@Nonnull final Coding src, @Nonnull final Coding target) {
    return with(src, target, ConceptMapEquivalence.SUBSUMES);
  }

  @Nonnull
  public ConceptMapBuilder withSpecializes(@Nonnull final Coding src,
      @Nonnull final Coding target) {
    return with(src, target, ConceptMapEquivalence.SPECIALIZES);
  }

  @Nonnull
  public ConceptMapBuilder with(@Nonnull final Coding src, @Nonnull final Coding target,
      @Nonnull final ConceptMapEquivalence equivalence) {
    entries.add(new ConceptMapEntry(src, target, equivalence));
    return this;
  }

  @Nonnull
  public ConceptMap build() {
    final Map<Pair<VersionedSystem, VersionedSystem>, List<ConceptMapEntry>> mappingsBySystem =
        entries.stream().collect(Collectors.groupingBy(ConceptMapBuilder::getVersionedSystems));

    final ConceptMap result = new ConceptMap();
    result.setStatus(PublicationStatus.ACTIVE);
    mappingsBySystem.forEach((srcAndTarget, systemMappings) -> {
      final ConceptMapGroupComponent group = result.addGroup();
      group.setSource(srcAndTarget.getLeft().getSystem());
      group.setSourceVersion(srcAndTarget.getLeft().getVersion());
      group.setTarget(srcAndTarget.getRight().getSystem());
      group.setTargetVersion(srcAndTarget.getRight().getVersion());
      systemMappings.forEach(m -> {
        final SourceElementComponent sourceElement = group.addElement();
        sourceElement.setCode(m.getSource().getCode());
        final TargetElementComponent targetElement = sourceElement.addTarget();
        targetElement.setCode(m.getTarget().getCode());
        targetElement.setEquivalence(m.getEquivalence());
      });
    });
    return result;
  }

  @Nonnull
  public static ConceptMapBuilder empty() {
    return new ConceptMapBuilder();
  }

}
