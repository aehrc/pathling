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

package au.csiro.pathling.test.fixtures;

import au.csiro.pathling.fhirpath.encoding.ImmutableCoding;
import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import au.csiro.pathling.terminology.ConceptTranslator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hl7.fhir.r4.model.Coding;

public class ConceptTranslatorBuilder {

  @Nonnull
  private final Map<SimpleCoding, List<ImmutableCoding>> mappings;

  @Nullable
  private String toSystem;

  private ConceptTranslatorBuilder() {
    mappings = new HashMap<>();
  }

  @Nonnull
  public ConceptTranslatorBuilder putTimes(@Nonnull final SimpleCoding coding,
      final int noOfMappings) {
    if (toSystem == null) {
      throw new IllegalStateException("toSystem is undefined");
    }
    mappings.put(coding, IntStream.range(0, noOfMappings)
        .mapToObj(i -> new Coding(toSystem, coding.getCode() + "-" + i, "Display-" + i))
        .map(ImmutableCoding::of)
        .collect(Collectors.toUnmodifiableList()));
    return this;
  }

  @Nonnull
  public ConceptTranslatorBuilder put(@Nonnull final SimpleCoding coding,
      @Nonnull final Coding... translatedCodings) {
    mappings.put(coding,
        Stream.of(translatedCodings).map(ImmutableCoding::of)
            .collect(Collectors.toUnmodifiableList()));
    return this;
  }


  @Nonnull
  public ConceptTranslatorBuilder put(@Nonnull final Coding coding,
      @Nonnull final Coding... translatedCodings) {
    return put(new SimpleCoding(coding), translatedCodings);
  }


  @Nonnull
  public ConceptTranslator build() {
    return new ConceptTranslator(mappings);
  }

  @Nonnull
  public static ConceptTranslatorBuilder empty() {
    return new ConceptTranslatorBuilder();
  }

  @Nonnull
  public static ConceptTranslatorBuilder toSystem(@Nonnull final String system) {
    final ConceptTranslatorBuilder result = new ConceptTranslatorBuilder();
    result.toSystem = system;
    return result;
  }
}
