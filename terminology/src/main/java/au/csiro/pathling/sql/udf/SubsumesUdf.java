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

package au.csiro.pathling.sql.udf;

import static au.csiro.pathling.sql.udf.TerminologyUdfHelpers.decodeOneOrMany;
import static au.csiro.pathling.sql.udf.TerminologyUdfHelpers.validCodings;
import static org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome.EQUIVALENT;
import static org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome.SUBSUMEDBY;
import static org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome.SUBSUMES;

import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome;

/**
 * The implementation of the 'subsumes' UDF.
 */
@Slf4j
public class SubsumesUdf implements SqlFunction,
    SqlFunction3<Object, Object, Boolean, Boolean> {

  private static final long serialVersionUID = 7605853352299165569L;

  public static final String FUNCTION_NAME = "subsumes";

  public static final DataType RETURN_TYPE = DataTypes.BooleanType;
  public static final boolean PARAM_INVERTED_DEFAULT = false;

  @Nonnull
  private final TerminologyServiceFactory terminologyServiceFactory;

  public SubsumesUdf(@Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
    this.terminologyServiceFactory = terminologyServiceFactory;
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public DataType getReturnType() {
    return RETURN_TYPE;
  }

  @Nullable
  protected Boolean doCall(@Nullable final Stream<Coding> codingsA,
      @Nullable final Stream<Coding> codingsB,
      @Nullable Boolean inverted) {
    if (codingsA == null || codingsB == null) {
      return null;
    }
    final boolean resolvedInverted = inverted != null
                                     ? inverted
                                     : PARAM_INVERTED_DEFAULT;

    final TerminologyService terminologyService = terminologyServiceFactory.build();

    // does any of the input codings subsume any of the output codings (within the same system)
    final List<Coding> validCodingsB = validCodings(codingsB)
        .collect(Collectors.toUnmodifiableList());

    return validCodings(codingsA)
        .anyMatch(codingA ->
            validCodingsB.stream()
                .filter(codingB -> codingA.getSystem().equals(codingB.getSystem()))
                .anyMatch(codingB -> isSubsumes(terminologyService.subsumes(codingA, codingB),
                    resolvedInverted))
        );
  }

  @Nullable
  @Override
  public Boolean call(@Nullable final Object codingRowOrArrayA,
      @Nullable final Object codingRowOrArrayB,
      @Nullable final Boolean inverted) {
    return doCall(
        decodeOneOrMany(codingRowOrArrayA),
        decodeOneOrMany(codingRowOrArrayB, 1),
        inverted);
  }

  private static boolean isSubsumes(@Nonnull final ConceptSubsumptionOutcome outcome,
      final boolean inverted) {
    return EQUIVALENT.equals(outcome) || (inverted
                                          ? SUBSUMEDBY
                                          : SUBSUMES).equals(outcome);
  }
}
