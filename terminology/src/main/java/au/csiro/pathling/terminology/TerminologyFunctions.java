package au.csiro.pathling.terminology;

import static au.csiro.pathling.utilities.Preconditions.wrapInUserInputError;

import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.fhirpath.encoding.CodingEncoding;
import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import au.csiro.pathling.fhirpath.encoding.SimpleCodingsDecoders;
import au.csiro.pathling.fhirpath.function.memberof.MemberOfMapper;
import au.csiro.pathling.fhirpath.function.translate.TranslateMapper;
import au.csiro.pathling.sql.MapperWithPreview;
import au.csiro.pathling.sql.SqlExtensions;
import au.csiro.pathling.utilities.Strings;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import org.slf4j.MDC;

public interface TerminologyFunctions {

  @Nonnull
  static Dataset<Row> memberOf(@Nonnull final Column codingArrayCol,
      @Nonnull final String valueSetUri, @Nonnull final Dataset<Row> dataset,
      @Nonnull final String outputColumnName,
      @Nonnull final TerminologyServiceFactory terminologyServiceFactory) {

    final MapperWithPreview<List<SimpleCoding>, Boolean, Set<SimpleCoding>> mapper =
        new MemberOfMapper(MDC.get("requestId"), terminologyServiceFactory,
            valueSetUri);

    return SqlExtensions
        .mapWithPartitionPreview(dataset, codingArrayCol,
            SimpleCodingsDecoders::decodeList,
            mapper,
            StructField.apply(outputColumnName, DataTypes.BooleanType, true, Metadata.empty()));
  }

  @Nonnull
  static Dataset<Row> translate(@Nonnull final Column codingArrayCol,
      @Nonnull final String conceptMapUrl, final boolean reverse, @Nonnull final String equivalence,
      @Nonnull final Dataset<Row> dataset,
      @Nonnull final String outputColumnName,
      @Nonnull final TerminologyServiceFactory terminologyServiceFactory) {

    final MapperWithPreview<List<SimpleCoding>, Row[], ConceptTranslator> mapper =
        new TranslateMapper(MDC.get("requestId"), terminologyServiceFactory,
            conceptMapUrl, reverse, Strings.parseCsvList(equivalence,
            wrapInUserInputError(ConceptMapEquivalence::fromCode)));

    return SqlExtensions
        .mapWithPartitionPreview(dataset, codingArrayCol,
            SimpleCodingsDecoders::decodeList,
            mapper,
            StructField.apply(outputColumnName, DataTypes.createArrayType(CodingEncoding.DATA_TYPE),
                true,
                Metadata.empty()));
  }

}
