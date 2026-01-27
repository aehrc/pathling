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

package au.csiro.pathling.sql.udf;

import static au.csiro.pathling.sql.udf.TerminologyUdfHelpers.decodeOneOrMany;
import static au.csiro.pathling.sql.udf.TerminologyUdfHelpers.encodeMany;
import static au.csiro.pathling.sql.udf.TerminologyUdfHelpers.validCodings;
import static java.util.Objects.nonNull;
import static java.util.function.Predicate.not;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.fhirpath.encoding.CodingSchema;
import au.csiro.pathling.fhirpath.encoding.ImmutableCoding;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyService.Translation;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import com.google.common.collect.ImmutableSet;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.Serial;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import scala.collection.Seq;
import scala.reflect.ClassTag;

/** The implementation of the 'translate()' udf. */
@Slf4j
public class TranslateUdf
    implements SqlFunction, SqlFunction5<Object, String, Boolean, Seq<String>, String, Row[]> {

  @Serial private static final long serialVersionUID = 7605853352299165569L;

  /** Set of valid equivalence codes for translation. */
  public static final Set<String> VALID_EQUIVALENCE_CODES =
      Stream.of(ConceptMapEquivalence.values())
          .map(ConceptMapEquivalence::toCode)
          .filter(Objects::nonNull)
          .collect(Collectors.toUnmodifiableSet());

  /** Default set of equivalences used for translation. */
  public static final Set<String> DEFAULT_EQUIVALENCES =
      ImmutableSet.of(ConceptMapEquivalence.EQUIVALENT.toCode());

  /** The name of the translate UDF function. */
  public static final String FUNCTION_NAME = "translate_coding";

  /** The return type of the translate UDF function. */
  public static final DataType RETURN_TYPE = DataTypes.createArrayType(CodingSchema.DATA_TYPE);

  /** The default value for the reverse parameter. */
  public static final boolean PARAM_REVERSE_DEFAULT = false;

  /** The terminology service factory used to create terminology services. */
  @Nonnull private final TerminologyServiceFactory terminologyServiceFactory;

  /**
   * Creates a new TranslateUdf with the specified terminology service factory.
   *
   * @param terminologyServiceFactory the terminology service factory to use
   */
  TranslateUdf(@Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
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

  /**
   * Validates that the given equivalence code is valid.
   *
   * @param code the equivalence code to validate
   * @return the validated code
   * @throws InvalidUserInputError if the code is not valid
   */
  @Nonnull
  public static String checkValidEquivalenceCode(@Nonnull final String code) {
    if (!VALID_EQUIVALENCE_CODES.contains(code)) {
      throw new InvalidUserInputError(
          String.format("Unknown ConceptMapEquivalence code '%s'", code));
    } else {
      return code;
    }
  }

  /**
   * Executes the translation operation for the given codings.
   *
   * @param codings the codings to translate
   * @param conceptMapUri the URI of the concept map to use for translation
   * @param reverse whether to reverse the translation direction
   * @param equivalences the equivalence codes to include in the translation
   * @param target the target system for translation
   * @return a stream of translated codings, or null if no translation is possible
   */
  @Nullable
  protected Stream<Coding> doCall(
      @Nullable final Stream<Coding> codings,
      @Nullable final String conceptMapUri,
      @Nullable final Boolean reverse,
      @Nullable final String[] equivalences,
      @Nullable final String target) {
    if (codings == null || conceptMapUri == null) {
      return null;
    }

    final boolean resolvedReverse = reverse != null ? reverse : PARAM_REVERSE_DEFAULT;

    final Set<String> includeEquivalences =
        (equivalences == null) ? DEFAULT_EQUIVALENCES : toValidSetOfEquivalenceCodes(equivalences);

    if (includeEquivalences.isEmpty()) {
      return Stream.empty();
    }

    final TerminologyService terminologyService = terminologyServiceFactory.build();
    return validCodings(codings)
        .flatMap(
            coding ->
                terminologyService
                    .translate(coding, conceptMapUri, resolvedReverse, target)
                    .stream())
        .filter(entry -> includeEquivalences.contains(entry.getEquivalence().toCode()))
        .map(Translation::getConcept)
        .map(ImmutableCoding::of)
        .distinct()
        .map(ImmutableCoding::toCoding);
  }

  @Nullable
  @Override
  public Row[] call(
      @Nullable final Object codingRowOrArray,
      @Nullable final String conceptMapUri,
      @Nullable final Boolean reverse,
      @Nullable final Seq<String> equivalences,
      @Nullable final String target) {

    //noinspection RedundantCast
    return encodeMany(
        doCall(
            decodeOneOrMany(codingRowOrArray),
            conceptMapUri,
            reverse,
            nonNull(equivalences)
                ? (String[]) equivalences.toArray(ClassTag.apply(String.class))
                : null,
            target));
  }

  @Nonnull
  private Set<String> toValidSetOfEquivalenceCodes(@Nonnull final String[] equivalences) {
    return Stream.of(equivalences)
        .filter(Objects::nonNull)
        .filter(not(String::isEmpty))
        .map(TranslateUdf::checkValidEquivalenceCode)
        .collect(Collectors.toUnmodifiableSet());
  }
}
