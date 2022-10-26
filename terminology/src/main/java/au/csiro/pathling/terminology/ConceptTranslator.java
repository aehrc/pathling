/*
 * Copyright 2022 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
 * Local representation of the concept map flattened with respect to equivalence types.
 */
@ToString
@EqualsAndHashCode
public class ConceptTranslator implements Serializable {

  private static final long serialVersionUID = -8246857034657784595L;

  @Nonnull
  private final Map<SimpleCoding, List<ImmutableCoding>> codingMapping;

  /**
   * Default constructor.
   */
  public ConceptTranslator() {
    this(Collections.emptyMap());
  }

  /**
   * @param codingMapping The map of Codings to use within translate operations
   */
  public ConceptTranslator(@Nonnull final Map<SimpleCoding, List<ImmutableCoding>> codingMapping) {
    this.codingMapping = codingMapping;
  }

  /**
   * Returns an empty concept translator.
   *
   * @return an empty translator.
   */
  @Nonnull
  public static ConceptTranslator empty() {
    return new ConceptTranslator();
  }

  /**
   * Translates a collection of coding according to this map to the distinct list of translated
   * codings.
   *
   * @param codings the codings to be translated
   * @return the list of distinct coding translations
   */
  @Nonnull
  public List<Coding> translate(@Nullable final Collection<SimpleCoding> codings) {

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
