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

package au.csiro.pathling.terminology.subsumes;

import static au.csiro.pathling.fhir.ParametersUtils.toSubsumptionOutcome;
import static au.csiro.pathling.fhirpath.CodingHelpers.codingEquals;
import static au.csiro.pathling.terminology.TerminologyParameters.required;
import static org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome.NOTSUBSUMED;

import au.csiro.pathling.fhir.TerminologyClient;
import au.csiro.pathling.fhirpath.encoding.ImmutableCoding;
import au.csiro.pathling.terminology.TerminologyOperation;
import au.csiro.pathling.terminology.TerminologyParameters;
import ca.uhn.fhir.rest.gclient.IOperationUntypedWithInput;
import jakarta.annotation.Nonnull;
import java.util.Optional;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.UriType;
import org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome;

/**
 * An implementation of {@link TerminologyOperation} for the subsumes operation.
 *
 * @author John Grimes
 * @see <a
 *     href="https://www.hl7.org/fhir/R4/codesystem-operation-subsumes.html">CodeSystem/$subsumes</a>
 */
public class SubsumesExecutor
    implements TerminologyOperation<Parameters, ConceptSubsumptionOutcome> {

  @Nonnull private final TerminologyClient terminologyClient;

  @Nonnull private final SubsumesParameters parameters;

  /**
   * Creates a new SubsumesExecutor with the specified terminology client and parameters.
   *
   * @param terminologyClient the terminology client to use for subsumption testing
   * @param parameters the parameters for the subsumption test
   */
  public SubsumesExecutor(
      @Nonnull final TerminologyClient terminologyClient,
      @Nonnull final SubsumesParameters parameters) {
    this.terminologyClient = terminologyClient;
    this.parameters = parameters;
  }

  @Override
  @Nonnull
  public Optional<ConceptSubsumptionOutcome> validate() {
    final ImmutableCoding codingA = parameters.codingA();
    final ImmutableCoding codingB = parameters.codingB();

    // If either of the systems are null, or they don't match, the result is not subsumed.
    if (codingA.getSystem() == null || !codingA.getSystem().equals(codingB.getSystem())) {
      return Optional.of(NOTSUBSUMED);
    }

    // If either code is null, the result is not subsumed.
    if (codingA.getCode() == null || codingB.getCode() == null) {
      return Optional.of(NOTSUBSUMED);
    }

    // If both versions are present, they must be equal.
    if (!(codingA.getVersion() == null
        || codingB.getVersion() == null
        || codingA.getVersion().equals(codingB.getVersion()))) {
      return Optional.of(NOTSUBSUMED);
    }

    // If both codings are obviously equal, we don't need to consult the terminology server.
    if (codingEquals(codingA.toCoding(), codingB.toCoding())) {
      return Optional.of(ConceptSubsumptionOutcome.EQUIVALENT);
    }

    return Optional.empty();
  }

  @Override
  @Nonnull
  public IOperationUntypedWithInput<Parameters> buildRequest() {
    final ImmutableCoding codingA = parameters.codingA();
    final ImmutableCoding codingB = parameters.codingB();
    final String resolvedSystem = codingA.getSystem();
    final String resolvedVersion =
        codingA.getVersion() != null ? codingA.getVersion() : codingB.getVersion();

    return terminologyClient.buildSubsumes(
        required(CodeType::new, codingA.getCode()),
        required(CodeType::new, codingB.getCode()),
        required(UriType::new, resolvedSystem),
        TerminologyParameters.optional(StringType::new, resolvedVersion));
  }

  @Override
  @Nonnull
  public ConceptSubsumptionOutcome extractResult(@Nonnull final Parameters response) {
    return toSubsumptionOutcome(response);
  }

  @Override
  @Nonnull
  public ConceptSubsumptionOutcome invalidRequestFallback() {
    return NOTSUBSUMED;
  }
}
