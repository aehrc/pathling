/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

import static java.util.Objects.requireNonNull;

import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhirpath.encoding.ImmutableCoding;
import au.csiro.pathling.terminology.lookup.LookupExecutor;
import au.csiro.pathling.terminology.lookup.LookupParameters;
import au.csiro.pathling.terminology.subsumes.SubsumesExecutor;
import au.csiro.pathling.terminology.subsumes.SubsumesParameters;
import au.csiro.pathling.terminology.translate.TranslateExecutor;
import au.csiro.pathling.terminology.translate.TranslateParameters;
import au.csiro.pathling.terminology.validatecode.ValidateCodeExecutor;
import au.csiro.pathling.terminology.validatecode.ValidateCodeParameters;
import ca.uhn.fhir.rest.gclient.IOperationUntypedWithInput;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.Closeable;
import java.util.List;
import java.util.Optional;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome;

/**
 * The default implementation of the TerminologyServiceFactory accessing the REST interface of FHIR
 * compliant terminology server.
 *
 * @author Piotr Szul
 * @author John Grimes
 */
public class DefaultTerminologyService extends BaseTerminologyService {

  /**
   * Creates a new instance of the DefaultTerminologyService.
   *
   * @param terminologyClient the terminology client to use for requests
   * @param resourcesToClose additional resources to close when this service is closed
   */
  public DefaultTerminologyService(
      @Nonnull final TerminologyClient terminologyClient,
      @Nonnull final Closeable... resourcesToClose) {
    super(terminologyClient, resourcesToClose);
  }

  @Override
  public boolean validateCode(@Nonnull final String valueSetUrl, @Nonnull final Coding coding) {
    final ValidateCodeParameters parameters =
        new ValidateCodeParameters(valueSetUrl, ImmutableCoding.of(coding));
    final ValidateCodeExecutor executor = new ValidateCodeExecutor(terminologyClient, parameters);
    return execute(executor);
  }

  @Nonnull
  @Override
  public List<Translation> translate(
      @Nonnull final Coding coding,
      @Nonnull final String conceptMapUrl,
      final boolean reverse,
      @Nullable final String target) {
    final TranslateParameters parameters =
        new TranslateParameters(ImmutableCoding.of(coding), conceptMapUrl, reverse, target);
    final TranslateExecutor executor = new TranslateExecutor(terminologyClient, parameters);
    return requireNonNull(execute(executor));
  }

  @Nonnull
  @Override
  public ConceptSubsumptionOutcome subsumes(
      @Nonnull final Coding codingA, @Nonnull final Coding codingB) {
    final SubsumesParameters parameters =
        new SubsumesParameters(ImmutableCoding.of(codingA), ImmutableCoding.of(codingB));
    final SubsumesExecutor executor = new SubsumesExecutor(terminologyClient, parameters);
    return requireNonNull(execute(executor));
  }

  @Nonnull
  @Override
  public List<PropertyOrDesignation> lookup(
      @Nonnull final Coding coding,
      @Nullable final String property,
      @Nullable final String acceptLanguage) {
    final LookupParameters parameters =
        new LookupParameters(ImmutableCoding.of(coding), property, acceptLanguage);
    final LookupExecutor executor = new LookupExecutor(terminologyClient, parameters);
    return requireNonNull(execute(executor));
  }

  @Nonnull
  private static <S, R> R execute(@Nonnull final TerminologyOperation<S, R> operation) {
    final Optional<R> invalidResult = operation.validate();
    if (invalidResult.isPresent()) {
      return invalidResult.get();
    }

    try {
      final IOperationUntypedWithInput<S> request = operation.buildRequest();
      final S response = request.execute();
      return operation.extractResult(response);

    } catch (final BaseServerResponseException e) {
      // If the terminology server rejects the request as invalid, use false as the result.
      final R fallback = operation.invalidRequestFallback();
      return handleError(e, fallback);
    }
  }
}
