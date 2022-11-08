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

import static au.csiro.pathling.sql.udf.TerminologyUdfHelpers.decodeOne;
import static au.csiro.pathling.sql.udf.TerminologyUdfHelpers.encodeMany;
import static au.csiro.pathling.utilities.Preconditions.wrapInUserInputError;

@Component
@Profile("core|unit-test")
@Slf4j
public class TranslateCoding extends TranslateCodingBase implements
    SqlFunction4<Row, String, Boolean, String, Row[]> {

  private static final long serialVersionUID = 7605853352299165569L;

  public static final String FUNCTION_NAME = "translate_coding";


  public TranslateCoding(@Nonnull final TerminologyServiceFactory terminologyServiceFactory) {
    super(terminologyServiceFactory);
  }

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Nullable
  @Override
  public Row[] call(@Nullable final Row codingRow, @Nullable final String conceptMapUri,
      @Nullable final Boolean reverse, @Nullable final String equivalences) {
    return encodeMany(doCall(decodeOne(codingRow), conceptMapUri, reverse, equivalences));
  }
}
