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


import static au.csiro.pathling.utilities.Preconditions.checkResponse;

import au.csiro.pathling.fhirpath.encoding.ImmutableCoding;
import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import ca.uhn.fhir.context.FhirContext;
import com.google.common.collect.Streams;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Bundle.BundleEntryComponent;
import org.hl7.fhir.r4.model.Bundle.BundleEntryRequestComponent;
import org.hl7.fhir.r4.model.Bundle.BundleType;
import org.hl7.fhir.r4.model.Bundle.HTTPVerb;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.UriType;

/**
 * Input/Output mapping for translate operation.
 *
 * @see <a href="https://hapifhir.io/hapi-fhir/docs/client/examples.html">HAPI Bundle example</a>.
 */
@Deprecated
public final class TranslateMapping extends BaseMapping {

  private TranslateMapping() {
  }

  /**
   * Represents a concept translation, along with an equivalence.
   */
  @SuppressWarnings("WeakerAccess")
  @Data
  @AllArgsConstructor
  @NoArgsConstructor
  public static class TranslationEntry {

    @Nonnull
    private Coding concept;

    @Nonnull
    private CodeType equivalence;

    public static TranslationEntry of(@Nonnull final ConceptMapEquivalence equivalence,
        @Nonnull final Coding coding) {
      return new TranslationEntry(coding, new CodeType(equivalence.toCode()));
    }
  }

  /**
   * Converts {@link TerminologyService#translate(Collection, String, boolean, Collection, String)}
   * parameters to a batch request Bundle.
   *
   * @param codings The list of codings to be translated
   * @param conceptMapUrl The concept map url
   * @param reverse If reverse translation is required
   * @return The batch Bundle for the requested parameters
   */
  @Nonnull
  public static Bundle toRequestBundle(@Nonnull final Iterable<SimpleCoding> codings,
      @Nonnull final String conceptMapUrl,
      final boolean reverse, @Nullable final String target) {
    final Bundle translateBatch = new Bundle();
    translateBatch.setType(BundleType.BATCH);
    codings.forEach(coding -> {
      final BundleEntryComponent entry = translateBatch.addEntry();
      final BundleEntryRequestComponent request = entry.getRequest();
      request.setMethod(HTTPVerb.POST);
      request.setUrl("ConceptMap/$translate");
      final Parameters parameters = new Parameters();
      entry.setResource(parameters);
      parameters.addParameter().setName("url").setValue(new UriType(conceptMapUrl));
      parameters.addParameter("reverse", reverse);
      parameters.addParameter().setName("coding").setValue(coding.toCoding());
      Optional.ofNullable(target).ifPresent(t -> parameters.addParameter("target", new UriType(t)));
    });
    return translateBatch;
  }

  /**
   * Builds ConceptTranslator from the batch response bundle for {@link
   * TerminologyService#translate(Collection, String, boolean, Collection, String)}.
   *
   * @param responseBundle The response from the terminology server
   * @param inputCodes The list of coding requested for translation
   * @param equivalences The list of equivalences to be included the translator
   * @param fhirContext A {@link FhirContext} for interpreting the response
   * @return The ConceptTranslator
   */
  @Nonnull
  public static ConceptTranslator fromResponseBundle(@Nonnull final Bundle responseBundle,
      @Nonnull final List<SimpleCoding> inputCodes,
      @Nonnull final Collection<ConceptMapEquivalence> equivalences,
      @Nonnull final FhirContext fhirContext) {

    checkResponse("batch-response".equals(responseBundle.getType().toCode()),
        "Expected bundle type 'batch-response' but got: '%s'",
        responseBundle.getType().toCode());
    checkResponse(inputCodes.size() == responseBundle.getEntry().size(),
        "The size of the response bundle: %s does not match the size of the request bundle: %s",
        inputCodes.size(), responseBundle.getEntry().size());

    final Set<String> equivalenceCodes = equivalences.stream()
        .map(ConceptMapEquivalence::toCode).collect(Collectors.toSet());

    final Stream<List<ImmutableCoding>> outputEntries = responseBundle.getEntry().stream()
        .map(e -> TranslateMapping.parametersFromEntry(e, fhirContext))
        .map(TranslateMapping::entriesFromParameters)
        .map(s -> s.filter(e -> equivalenceCodes.contains(e.getEquivalence().getCode()))
            .map(TranslationEntry::getConcept)
            .map(ImmutableCoding::of)
            .collect(Collectors.toUnmodifiableList()));

    @SuppressWarnings("UnstableApiUsage")
    final Stream<Pair<SimpleCoding, List<ImmutableCoding>>> pairs = Streams
        .zip(inputCodes.stream(), outputEntries, Pair::of);

    return new ConceptTranslator(
        pairs
            .filter(p -> !p.getValue().isEmpty()) // filter out empty mappings
            .collect(Collectors.toUnmodifiableMap(Pair::getKey, Pair::getValue)));
  }

  @Nonnull
  public static Stream<TranslationEntry> entriesFromParameters(
      @Nonnull final Parameters parameters) {
    return parameters.getParameterBool("result")
           ? parameters.getParameter().stream()
               .filter(pc -> "match".equals(pc.getName()))
               .map(pc -> partToBean(pc, TranslationEntry::new))
           : Stream.empty();
  }

}
