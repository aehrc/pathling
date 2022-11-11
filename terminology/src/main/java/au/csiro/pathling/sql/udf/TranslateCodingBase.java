package au.csiro.pathling.sql.udf;

import static au.csiro.pathling.utilities.Preconditions.wrapInUserInputError;

import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.fhirpath.encoding.CodingEncoding;
import au.csiro.pathling.fhirpath.encoding.ImmutableCoding;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TranslateMapping;
import au.csiro.pathling.terminology.TranslateMapping.TranslationEntry;
import au.csiro.pathling.utilities.Strings;
import com.google.common.collect.ImmutableSet;
import java.io.Serializable;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;

@Slf4j
public abstract class TranslateCodingBase implements SqlFunction, Serializable {

  private static final long serialVersionUID = 7605853352299165569L;

  public static final DataType RETURN_TYPE = DataTypes.createArrayType(CodingEncoding.DATA_TYPE);

  public static boolean PARAM_REVERSE_DEFAULT = false;

  @Nonnull
  protected final TerminologyServiceFactory terminologyServiceFactory;

  protected TranslateCodingBase(
      @Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
    this.terminologyServiceFactory = terminologyServiceFactory;
  }

  @Override
  public DataType getReturnType() {
    return RETURN_TYPE;
  }

  @Nullable
  protected Stream<Coding> doCall(@Nullable final Stream<Coding> codings,
      @Nullable final String conceptMapUri, @Nullable Boolean reverse,
      @Nullable final String equivalences) {
    if (codings == null || conceptMapUri == null) {
      return null;
    }

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

    final TerminologyService terminologyService = terminologyServiceFactory.buildService(
        log);
    // TODO: make codings unique maybe without using ImmutableCoding
    return codings
        .flatMap(coding -> TranslateMapping.entriesFromParameters(
            terminologyService.translateCoding(coding, conceptMapUri, resolvedReverse)))
        .filter(entry -> includeEquivalences.contains(entry.getEquivalence().getValue()))
        .map(TranslationEntry::getConcept)
        .map(ImmutableCoding::of)
        .distinct()
        .map(ImmutableCoding::toCoding);
  }
}
