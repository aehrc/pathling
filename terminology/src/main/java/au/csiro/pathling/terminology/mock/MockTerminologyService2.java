package au.csiro.pathling.terminology.mock;

import au.csiro.pathling.terminology.TerminologyService2;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.apache.commons.lang3.tuple.Pair;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome;
import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MockTerminologyService2 implements TerminologyService2 {

  @Value
  @AllArgsConstructor
  static class SystemAndCode {

    @Nonnull
    String system;
    @Nonnull
    String code;

    static SystemAndCode of(@Nonnull final Coding coding) {
      return new SystemAndCode(coding.getSystem(), coding.getCode());
    }
  }

  static class ValueSet {

    private final Set<SystemAndCode> members;

    ValueSet(final Coding... coding) {
      members = Stream.of(coding)
          .map(SystemAndCode::of)
          .collect(Collectors.toUnmodifiableSet());
    }

    boolean contains(@Nonnull final Coding coding) {
      return members.contains(SystemAndCode.of(coding));
    }

    public static final ValueSet EMPTY = new ValueSet();
  }

  private final Map<String, ValueSet> valueSets = new HashMap<>();
  private final Set<Pair<SystemAndCode, SystemAndCode>> subsumes = new HashSet<>();

  public MockTerminologyService2() {
    valueSets.put("http://snomed.info/sct?fhir_vs=refset/723264001",
        new ValueSet(new Coding("http://snomed.info/sct", "368529001", null)));
    valueSets.put("http://loinc.org/vs/LP14885-5",
        new ValueSet(new Coding("http://loinc.org", "55915-3", null)));

    subsumes.add(Pair.of(new SystemAndCode("http://snomed.info/sct", "107963000"),
        new SystemAndCode("http://snomed.info/sct", "63816008")));

  }

  @Override
  public boolean validate(@Nonnull final String url, @Nonnull final Coding coding) {
    return valueSets.getOrDefault(url, ValueSet.EMPTY).contains(coding);
  }

  @Nonnull
  @Override
  public Parameters translate(@Nonnull final Coding coding, @Nonnull final String conceptMapUrl,
      final boolean reverse) {
    throw new UnsupportedOperationException();
  }

  @Override
  @Nonnull
  public ConceptSubsumptionOutcome subsumes(@Nonnull final Coding codingA,
      @Nonnull final Coding codingB) {

    final SystemAndCode systemAndCodeA = SystemAndCode.of(codingA);
    final SystemAndCode systemAndCodeB = SystemAndCode.of(codingB);

    if (systemAndCodeA.equals(systemAndCodeB)) {
      return ConceptSubsumptionOutcome.EQUIVALENT;
    } else if (subsumes.contains(Pair.of(systemAndCodeA, systemAndCodeB))) {
      return ConceptSubsumptionOutcome.SUBSUMES;
    } else if (subsumes.contains(Pair.of(systemAndCodeB, systemAndCodeA))) {
      return ConceptSubsumptionOutcome.SUBSUMEDBY;
    } else {
      return ConceptSubsumptionOutcome.NOTSUBSUMED;
    }
  }
}

