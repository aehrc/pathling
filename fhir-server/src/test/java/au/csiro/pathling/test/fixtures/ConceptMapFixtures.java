package au.csiro.pathling.test.fixtures;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.ConceptMap.ConceptMapGroupComponent;
import org.hl7.fhir.r4.model.ConceptMap.SourceElementComponent;
import org.hl7.fhir.r4.model.ConceptMap.TargetElementComponent;
import au.csiro.pathling.encoding.Mapping;

public interface ConceptMapFixtures {

  public static final ConceptMap CM_EMPTY = new ConceptMap();
  
  
  // http://snomed.info/sct|444814009 -- subsumes --> http://snomed.info/sct|40055000
  public static final ConceptMap CM_SNOWMED_444814009_SUBSUMES_40055000 = createConceptMap(
      new Mapping("http://snomed.info/sct", "444814009", "http://snomed.info/sct", "40055000"));
  
  public static Pair<String, String> getSystems(Mapping mapping) {
    return Pair.of(mapping.getFrom().getSystem(), mapping.getTo().getSystem());
  }

  public static ConceptMap createConceptMap(Mapping... mappings) {

    Map<Pair<String, String>, List<Mapping>> mappingsBySystem =
        Stream.of(mappings).collect(Collectors.groupingBy(ConceptMapFixtures::getSystems));

    final ConceptMap result = new ConceptMap();
    mappingsBySystem.forEach((srcAndTarget, systemMappins) -> {
      final ConceptMapGroupComponent group = result.addGroup();
      group.setSource(srcAndTarget.getLeft());
      group.setTarget(srcAndTarget.getRight());
      systemMappins.forEach(m -> {
        SourceElementComponent sourceElement = group.addElement();
        sourceElement.setCode(m.getFrom().getCode());
        TargetElementComponent targetElemnt = sourceElement.addTarget();
        targetElemnt.setCode(m.getTo().getCode());
      });
    });
    return result;
  }
}
