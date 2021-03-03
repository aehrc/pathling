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

/**
 * Local representation of the concept map flattened with respect to  equivalence types.
 */
@ToString
@EqualsAndHashCode
public class ConceptTranslator implements Serializable {

  @Nonnull
  private final Map<SimpleCoding, List<ImmutableCoding>> codingMapping;

  public ConceptTranslator() {
    this(Collections.emptyMap());
  }

  public ConceptTranslator(@Nonnull final Map<SimpleCoding, List<ImmutableCoding>> codingMapping) {
    this.codingMapping = codingMapping;
  }

  /**
   * Translates a collection of coding according to this map to the distinct list of translated
   * codings.
   *
   * @param codings the codings to be translated.
   * @return the list of distinct coding translations.
   */
  @Nonnull
  public List<Coding> translate(@Nullable Collection<SimpleCoding> codings) {

    return codings == null
           ? Collections.emptyList()
           : codings.stream()
               .flatMap(c -> codingMapping.getOrDefault(c, Collections.emptyList()).stream())
               .distinct()
               .map(ImmutableCoding::toCoding)
               .collect(
                   Collectors.toList());
  }
}
