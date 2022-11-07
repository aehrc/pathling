package au.csiro.pathling.sql.udf;

import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.fhirpath.encoding.CodingEncoding;
import au.csiro.pathling.terminology.TranslateMapping;
import au.csiro.pathling.terminology.TranslateMapping.TranslationEntry;
import au.csiro.pathling.utilities.Strings;
import com.google.common.collect.ImmutableSet;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import org.hl7.fhir.r4.model.Parameters;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.constraints.Null;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static au.csiro.pathling.utilities.Preconditions.wrapInUserInputError;

@Component
@Profile("core|unit-test")
@Slf4j
public class TranslateCoding implements SqlFunction4<Row, String, Boolean, String, Row[]> {

  private static final long serialVersionUID = 7605853352299165569L;

  public static final DataType RETURN_TYPE = DataTypes.createArrayType(CodingEncoding.DATA_TYPE);

  public static final String FUNCTION_NAME = "translate_coding";

  @Nonnull
  private final TerminologyServiceFactory terminologyServiceFactory;

  public TranslateCoding(@Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
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
  public Row[] call(@Nullable final Row codingRow, @Nullable final String conceptMapUri,
      @Nullable final Boolean reverse, @Nullable final String equivalences) throws Exception {
    if (conceptMapUri == null || codingRow == null) {
      return null;
    }

    final Coding coding = CodingEncoding.decode(codingRow);
    // TODO: Somewhere here add validation for invalid codings
    // TODO: Provide better defaults
    final Parameters parameters = terminologyServiceFactory.buildService(log)
        .translateCoding(coding, conceptMapUri, reverse != null
                                                ? reverse
                                                : false);
    // We need more logic here now
    final Stream<TranslationEntry> entries = TranslateMapping.entriesFromParameters(
        parameters);

    final Set<String> includeEquivalences = (equivalences == null || equivalences.isBlank())
                                            ? ImmutableSet.of("equivalent")
                                            : Strings.parseCsvList(equivalences,
                                                    wrapInUserInputError(
                                                        ConceptMapEquivalence::fromCode)).stream()
                                                .map(ConceptMapEquivalence::toCode)
                                                .collect(Collectors.toUnmodifiableSet());

    return entries
        .filter(entry -> includeEquivalences.contains(entry.getEquivalence().getValue()))
        .map(TranslationEntry::getConcept)
        .map(CodingEncoding::encode)
        .toArray(Row[]::new);
  }
}
