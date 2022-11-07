package au.csiro.pathling.sql.udf;

import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.fhirpath.encoding.CodingEncoding;
import au.csiro.pathling.fhirpath.encoding.ImmutableCoding;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TranslateMapping;
import au.csiro.pathling.terminology.TranslateMapping.TranslationEntry;
import au.csiro.pathling.utilities.Strings;
import com.google.common.collect.ImmutableSet;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.shaded.org.apache.curator.shaded.com.google.common.collect.Streams;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import org.hl7.fhir.r4.model.Parameters;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static au.csiro.pathling.utilities.Preconditions.wrapInUserInputError;

@Component
@Profile("core|unit-test")
@Slf4j
public class TranslateCodingArray implements
    SqlFunction4<WrappedArray<Row>, String, Boolean, String, Row[]> {

  private static final long serialVersionUID = 7605853352299165569L;

  public static final DataType RETURN_TYPE = DataTypes.createArrayType(CodingEncoding.DATA_TYPE);

  public static final String FUNCTION_NAME = "translate_coding_array";

  @Nonnull
  private final TerminologyServiceFactory terminologyServiceFactory;

  public TranslateCodingArray(@Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
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
  @Override
  public Row[] call(@Nullable final WrappedArray<Row> codingsArrayRow,
      @Nullable final String conceptMapUri, @Nullable Boolean reverse,
      @Nullable final String equivalences) throws Exception {
    if (conceptMapUri == null || codingsArrayRow == null) {
      return null;
    }

    final Set<String> includeEquivalences = (equivalences == null || equivalences.isBlank())
                                            ? ImmutableSet.of("equivalent")
                                            : Strings.parseCsvList(equivalences,
                                                    wrapInUserInputError(
                                                        ConceptMapEquivalence::fromCode)).stream()
                                                .map(ConceptMapEquivalence::toCode)
                                                .collect(Collectors.toUnmodifiableSet());

    // TODO: better defaults and unify with the non array version
    final boolean resolvedReverse = reverse != null
                                    ? reverse
                                    : false;

    final TerminologyService terminologyService = terminologyServiceFactory.buildService(
        log);
    // TODO: make codings unique maybe without using ImmutableCoding
    return Streams.stream(JavaConverters.asJavaIterable(codingsArrayRow))
        .map(CodingEncoding::decode)
        .flatMap(coding -> TranslateMapping.entriesFromParameters(
            terminologyService.translateCoding(coding, conceptMapUri, resolvedReverse)))
        .filter(entry -> includeEquivalences.contains(entry.getEquivalence().getValue()))
        .map(TranslationEntry::getConcept)
        .map(ImmutableCoding::of)
        .distinct()
        .map(ImmutableCoding::toCoding)
        .map(CodingEncoding::encode)
        .toArray(Row[]::new);
  }
}
