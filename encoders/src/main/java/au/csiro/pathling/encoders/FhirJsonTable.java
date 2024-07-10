package au.csiro.pathling.encoders;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.fs.FileStatus;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.FileFormat;
import org.apache.spark.sql.execution.datasources.v2.json.JsonTable;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.Option;
import scala.Some;
import scala.collection.Seq;

public class FhirJsonTable extends JsonTable {

  private final RuntimeResourceDefinition resourceDefinition;
  private final Map<String, DataType> readCasts = Map.of(
      "decimal", DataTypes.StringType,
      "integer", DataTypes.IntegerType,
      "positiveInt", DataTypes.IntegerType,
      "unsignedInt", DataTypes.IntegerType,
      "integer64", DataTypes.LongType
  );
  private final Map<String, DataType> writeCasts = Map.of(
      "decimal", DataTypes.createDecimalType(38, 6)
  );

  public FhirJsonTable(@Nonnull final FhirContext fhirContext,
      @Nullable final String name, @Nullable final SparkSession sparkSession,
      @Nullable final CaseInsensitiveStringMap options, @Nullable final Seq<String> paths,
      @Nullable final Option<StructType> userSpecifiedSchema,
      @Nullable final Class<? extends FileFormat> fallbackFileFormat) {
    super(name, sparkSession, options, paths, userSpecifiedSchema, fallbackFileFormat);
    final String resourceType = Optional.ofNullable(options)
        .map(o -> o.get("resourceType"))
        .orElseThrow(() -> new IllegalArgumentException("resourceType option must be specified"));
    try {
      resourceDefinition = Optional.ofNullable(fhirContext.getResourceDefinition(resourceType))
          .orElseThrow();
    } catch (final Exception e) {
      throw new IllegalArgumentException("Unknown resource type: " + resourceType, e);
    }
  }

  @Override
  @Nonnull
  public Option<StructType> inferSchema(@Nullable final Seq<FileStatus> files) {
    final Option<StructType> maybeSchema = super.inferSchema(files);
    if (maybeSchema.nonEmpty()) {
      final StructType schema = maybeSchema.get();
      final SchemaCaster schemaCaster = new SchemaCaster(readCasts);
      final StructType modifiedSchema = schemaCaster.processStruct(schema, resourceDefinition);
      return new Some<>(modifiedSchema);
    }
    return maybeSchema;
  }

}
