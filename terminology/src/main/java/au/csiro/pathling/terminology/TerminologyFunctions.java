package au.csiro.pathling.terminology;

import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import au.csiro.pathling.fhirpath.encoding.SimpleCodingsDecoders;
import au.csiro.pathling.fhirpath.function.memberof.MemberOfMapper;
import au.csiro.pathling.sql.MapperWithPreview;
import au.csiro.pathling.sql.SqlExtensions;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.slf4j.MDC;

public interface TerminologyFunctions {

  @Nonnull
  static Dataset<Row> memberOf(final Column codingArrayCol,
      final String valueSetUri, final Dataset<Row> dataset, @Nonnull final String outputColumnName,
      final TerminologyServiceFactory terminologyServiceFactory) {
    // Perform a "validate code" operation on each Coding or CodeableConcept in the input dataset,
    // then create a new dataset with the boolean results.
    final MapperWithPreview<List<SimpleCoding>, Boolean, Set<SimpleCoding>> mapper =
        new MemberOfMapper(MDC.get("requestId"), terminologyServiceFactory,
            valueSetUri);

    // This de-duplicates the Codings to be validated, then performs the validation on a
    // per-partition basis.
    return SqlExtensions
        .mapWithPartitionPreview(dataset, codingArrayCol,
            SimpleCodingsDecoders::decodeList,
            mapper,
            StructField.apply(outputColumnName, DataTypes.BooleanType, true, Metadata.empty()));
  }
 
}
