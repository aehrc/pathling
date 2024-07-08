/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.terminology.translate;

import static au.csiro.pathling.fhir.ParametersUtils.toMatchParts;
import static au.csiro.pathling.terminology.TerminologyParameters.optional;
import static au.csiro.pathling.terminology.TerminologyParameters.required;
import static java.util.Objects.isNull;

import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhirpath.encoding.ImmutableCoding;
import au.csiro.pathling.terminology.TerminologyOperation;
import au.csiro.pathling.terminology.TerminologyService.Translation;
import au.csiro.pathling.terminology.caching.CacheableListCollector;
import ca.uhn.fhir.rest.gclient.IOperationUntypedWithInput;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Optional;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.UriType;
import org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence;

/**
 * An implementation of {@link TerminologyOperation} for the translate operation.
 *
 * @author John Grimes
 * @see <a
 * href="https://www.hl7.org/fhir/R4/conceptmap-operation-translate.html">ConceptMap/$translate</a>
 */
public class TranslateExecutor implements
    TerminologyOperation<Parameters, ArrayList<Translation>> {

  @Nonnull
  private final TerminologyClient terminologyClient;

  @Nonnull
  private final TranslateParameters parameters;

  public TranslateExecutor(@Nonnull final TerminologyClient terminologyClient,
      @Nonnull final TranslateParameters parameters) {
    this.terminologyClient = terminologyClient;
    this.parameters = parameters;
  }

  @Override
  @Nonnull
  public Optional<ArrayList<Translation>> validate() {
    final ImmutableCoding coding = parameters.getCoding();

    // If the system or the code of the coding is null, the result is an empty list.
    if (isNull(coding.getSystem()) || isNull(coding.getCode())) {
      return Optional.of(new ArrayList<>());
    } else {
      return Optional.empty();
    }
  }

  @Override
  @Nonnull
  public IOperationUntypedWithInput<Parameters> buildRequest() {
    final String conceptMapUrl = parameters.getConceptMapUrl();
    final ImmutableCoding coding = parameters.getCoding();
    final boolean reverse = parameters.isReverse();
    final String target = parameters.getTarget();

    return terminologyClient.buildTranslate(
        required(UriType::new, conceptMapUrl),
        required(UriType::new, coding.getSystem()),
        optional(StringType::new, coding.getVersion()),
        required(CodeType::new, coding.getCode()),
        new BooleanType(reverse),
        optional(UriType::new, target)
    );
  }

  @Override
  @Nonnull
  public ArrayList<Translation> extractResult(@Nonnull final Parameters response) {
    return toTranslations(response);
  }

  @Override
  @Nonnull
  public ArrayList<Translation> invalidRequestFallback() {
    return new ArrayList<>();
  }

  @Nonnull
  private static ArrayList<Translation> toTranslations(final @Nonnull Parameters parameters) {
    return toMatchParts(parameters)
        .map(tp -> Translation.of(ConceptMapEquivalence.fromCode(tp.getEquivalence().getCode()),
            tp.getConcept()))
        .collect(new CacheableListCollector<>());
  }

}
