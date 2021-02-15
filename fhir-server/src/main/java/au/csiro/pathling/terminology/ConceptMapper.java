package au.csiro.pathling.terminology;

import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hl7.fhir.r4.model.Coding;

public class ConceptMapper implements Serializable {

  private final Map<SimpleCoding, List<Coding>> codingMapping;


  public ConceptMapper() {
    codingMapping = Collections.emptyMap();
  }

  public ConceptMapper(Map<SimpleCoding, List<Coding>> codingMapping) {
    this.codingMapping = codingMapping;
  }

  @Nonnull
  public List<Coding> translate(@Nullable Collection<SimpleCoding> codings) {

    return codings == null
           ? Collections.emptyList()
           : codings.stream()
               .flatMap(c -> codingMapping.getOrDefault(c, Collections.emptyList()).stream())
               //.distinct()
               .collect(
                   Collectors.toList());
  }
}
