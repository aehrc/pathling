/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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
import au.csiro.pathling.terminology.TranslationList;
import au.csiro.pathling.terminology.caching.TranslationListCollector;
import ca.uhn.fhir.rest.gclient.IOperationUntypedWithInput;
import jakarta.annotation.Nonnull;
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
 *     href="https://www.hl7.org/fhir/R4/conceptmap-operation-translate.html">ConceptMap/$translate</a>
 */
public class TranslateExecutor implements TerminologyOperation<Parameters, TranslationList> {

  @Nonnull private final TerminologyClient terminologyClient;

  @Nonnull private final TranslateParameters parameters;

  /**
   * Creates a new TranslateExecutor with the specified terminology client and parameters.
   *
   * @param terminologyClient the terminology client to use for translation
   * @param parameters the parameters for the translation
   */
  public TranslateExecutor(
      @Nonnull final TerminologyClient terminologyClient,
      @Nonnull final TranslateParameters parameters) {
    this.terminologyClient = terminologyClient;
    this.parameters = parameters;
  }

  @Override
  @Nonnull
  public Optional<TranslationList> validate() {
    final ImmutableCoding coding = parameters.coding();

    // If the system or the code of the coding is null, the result is an empty list.
    if (isNull(coding.getSystem()) || isNull(coding.getCode())) {
      return Optional.of(new TranslationList());
    } else {
      return Optional.empty();
    }
  }

  @Override
  @Nonnull
  public IOperationUntypedWithInput<Parameters> buildRequest() {
    final String conceptMapUrl = parameters.conceptMapUrl();
    final ImmutableCoding coding = parameters.coding();
    final boolean reverse = parameters.reverse();
    final String target = parameters.target();

    return terminologyClient.buildTranslate(
        required(UriType::new, conceptMapUrl),
        required(UriType::new, coding.getSystem()),
        optional(StringType::new, coding.getVersion()),
        required(CodeType::new, coding.getCode()),
        new BooleanType(reverse),
        optional(UriType::new, target));
  }

  @Override
  @Nonnull
  public TranslationList extractResult(@Nonnull final Parameters response) {
    return toTranslations(response);
  }

  @Override
  @Nonnull
  public TranslationList invalidRequestFallback() {
    return new TranslationList();
  }

  @Nonnull
  private static TranslationList toTranslations(final @Nonnull Parameters parameters) {
    return toMatchParts(parameters)
        .map(
            tp ->
                Translation.of(
                    ConceptMapEquivalence.fromCode(tp.getEquivalence().getCode()), tp.getConcept()))
        .collect(new TranslationListCollector());
  }
}
