package au.csiro.pathling.sql.udf;

import static au.csiro.pathling.sql.udf.TerminologyUdfHelpers.decodeOneOrMany;
import static au.csiro.pathling.sql.udf.TerminologyUdfHelpers.encodeMany;
import static au.csiro.pathling.sql.udf.TerminologyUdfHelpers.validCodings;
import static au.csiro.pathling.utilities.Preconditions.wrapInUserInputError;

import au.csiro.pathling.fhirpath.encoding.CodingEncoding;
import au.csiro.pathling.fhirpath.encoding.ImmutableCoding;
import au.csiro.pathling.terminology.TerminologyService2;
import au.csiro.pathling.terminology.TerminologyService2.Translation;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import au.csiro.pathling.utilities.Strings;
import com.google.common.collect.ImmutableSet;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The implementation of the 'translate()' udf.
 */
@Slf4j
public class TranslateUdf implements SqlFunction,
    SqlFunction5<Object, String, Boolean, String, String, Row[]> {

  private static final long serialVersionUID = 7605853352299165569L;

  public static final String FUNCTION_NAME = "translate";
  public static final DataType RETURN_TYPE = DataTypes.createArrayType(CodingEncoding.DATA_TYPE);
  public static final boolean PARAM_REVERSE_DEFAULT = false;

  @Nonnull
  private final TerminologyServiceFactory terminologyServiceFactory;

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

  @Nullable
  protected Stream<Coding> doCall(@Nullable final Stream<Coding> codings,
      @Nullable final String conceptMapUri, @Nullable Boolean reverse,
      @Nullable final String equivalences,
      @Nullable final String target) {
    if (codings == null || conceptMapUri == null) {
      return null;
    }

    // TODO: Add per stage caching of parsed equivalences
    final Set<String> includeEquivalences = (equivalences == null || equivalences.isBlank())
                                            ? ImmutableSet.of("equivalent")
                                            : Strings.parseCsvList(equivalences,
                                                    wrapInUserInputError(
                                                        ConceptMapEquivalence::fromCode)).stream()
                                                .map(ConceptMapEquivalence::toCode)
                                                .collect(Collectors.toUnmodifiableSet());

    final boolean resolvedReverse = reverse != null
                                    ? reverse
                                    : PARAM_REVERSE_DEFAULT;

    final TerminologyService2 terminologyService = terminologyServiceFactory.buildService2();
    // TODO: make codings unique maybe without using ImmutableCoding
    return validCodings(codings)
        .flatMap(coding ->
            terminologyService.translate(coding, conceptMapUri, resolvedReverse, target).stream())
        .filter(entry -> includeEquivalences.contains(entry.getEquivalence().toCode()))
        .map(Translation::getConcept)
        .map(ImmutableCoding::of)
        .distinct()
        .map(ImmutableCoding::toCoding);
  }

  @Nullable
  @Override
  public Row[] call(@Nullable final Object codingRowOrArray, @Nullable final String conceptMapUri,
      @Nullable final Boolean reverse, @Nullable final String equivalences,
      @Nullable final String target) {
    return encodeMany(
        doCall(decodeOneOrMany(codingRowOrArray), conceptMapUri, reverse, equivalences, target));
  }
}
