package au.csiro.pathling.terminology;

import au.csiro.pathling.fhirpath.encoding.ImmutableCoding;
import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.hl7.fhir.r4.model.Coding;

@ToString
@EqualsAndHashCode
public class ConceptTranslator implements Serializable {

  private final Map<SimpleCoding, List<ImmutableCoding>> codingMapping;

  public ConceptTranslator() {
    this(Collections.emptyMap());
  }

  public ConceptTranslator(Map<SimpleCoding, List<ImmutableCoding>> codingMapping) {
    this.codingMapping = codingMapping;
  }

  @Nonnull
  public List<Coding> translate(@Nullable Collection<SimpleCoding> codings) {

    return codings == null
           ? Collections.emptyList()
           : codings.stream()
               .flatMap(c -> codingMapping.getOrDefault(c, Collections.emptyList()).stream())
               .map(ImmutableCoding::toCoding)
               .collect(
                   Collectors.toList());
  }
}
