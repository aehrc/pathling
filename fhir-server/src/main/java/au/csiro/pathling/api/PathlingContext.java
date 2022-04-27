package au.csiro.pathling.api;

import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;

import au.csiro.pathling.fhir.DefaultTerminologyServiceFactory;
import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import au.csiro.pathling.fhirpath.encoding.SimpleCodingsDecoders;
import au.csiro.pathling.fhirpath.function.memberof.MemberOfMapperWithPreview;
import au.csiro.pathling.sql.MapperWithPreview;
import au.csiro.pathling.sql.SqlExtensions;
import ca.uhn.fhir.context.FhirContext;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;


public class PathlingContext {

  private final String serverUrl;
  private final TerminologyServiceFactory terminologyServiceFactory;

  private PathlingContext(String serverUrl) {

    this.serverUrl = serverUrl;
    this.terminologyServiceFactory = new DefaultTerminologyServiceFactory(
        FhirContext.forR4(),
        serverUrl,
        60000,
        false
    );
  }

  @Nonnull
  public Dataset<Row> memberOf(@Nonnull final Dataset<Row> codingDataframe,
      @Nonnull final Column codingColumn, @Nonnull final String valueSetUri,
      @Nonnull final String outputColumnName) {

    final Column codingArrayCol = when(codingColumn.isNotNull(), array(codingColumn))
        .otherwise(lit(null));

    // Prepare the data which will be used within the map operation. All of these things must be
    // Serializable.

    // Perform a validate code operation on each Coding or CodeableConcept in the input dataset,
    // then create a new dataset with the boolean results.
    // TODO: Find a better request id
    final MapperWithPreview<List<SimpleCoding>, Boolean, Set<SimpleCoding>> mapper =
        new MemberOfMapperWithPreview("none", terminologyServiceFactory,
            valueSetUri);

    // This de-duplicates the Codings to be validated, then performs the validation on a
    // per-partition basis.
    return SqlExtensions
        .mapWithPartitionPreview(codingDataframe, codingArrayCol,
            SimpleCodingsDecoders::decodeList,
            mapper,
            StructField.apply(outputColumnName, DataTypes.BooleanType, true, Metadata.empty()));
    //return codingDataframe.withColumn(outputColumnName, functions.lit(true));
  }

  public static PathlingContext create(@Nonnull final String serverUrl) {
    return new PathlingContext(serverUrl);
  }
}
