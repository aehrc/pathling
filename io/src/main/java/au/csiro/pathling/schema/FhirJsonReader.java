package au.csiro.pathling.schema;

import static au.csiro.pathling.schema.DatasetTransformer.transformColumn;
import static java.util.Objects.requireNonNull;
import static org.apache.spark.sql.functions.unbase64;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import ca.uhn.fhir.parser.DataFormatException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import scala.compat.java8.OptionConverters;

/**
 * Reads FHIR JSON data into a {@link Dataset} that is compatible with the Parquet on FHIR
 * specification.
 *
 * @author John Grimes
 * @see <a href="https://github.com/aehrc/parquet-on-fhir">Parquet on FHIR</a>
 */
public class FhirJsonReader {

  /**
   * A set of transforms that are applied to columns of specific types when reading FHIR JSON data.
   */
  @NotNull
  private static final Map<String, Function<ColumnDescriptor, Stream<Column>>> readTransforms = Map.of(
      "decimal", FhirJsonReader::transformDecimal,
      "integer", FhirJsonReader::transformInteger32,
      "positiveInt", FhirJsonReader::transformInteger32,
      "unsignedInt", FhirJsonReader::transformInteger32,
      "integer64", FhirJsonReader::transformInteger64,
      "base64Binary", FhirJsonReader::transformBinary
  );

  @NotNull
  private final SparkSession spark;

  @Nullable
  private final Map<String, String> options;

  @Nullable
  private final RuntimeResourceDefinition resourceDefinition;

  @Nullable
  private final DatasetTransformer datasetTransformer;

  public FhirJsonReader() {
    this.spark = OptionConverters.toJava(SparkSession.getActiveSession()).orElseThrow(
        () -> new IllegalStateException("No active Spark session"));
    this.options = null;
    this.resourceDefinition = null;
    this.datasetTransformer = null;
  }

  public FhirJsonReader(@NotNull final String resourceType, @NotNull final String fhirVersion,
      @NotNull final Map<String, String> options) throws IllegalArgumentException {
    this.spark = OptionConverters.toJava(SparkSession.getActiveSession()).orElseThrow(
        () -> new IllegalStateException("No active Spark session"));
    this.options = options;
    final FhirVersionEnum fhirVersionEnum = FhirVersionEnum.forVersionString(
        requireNonNull(fhirVersion));
    if (fhirVersionEnum == null) {
      throw new IllegalArgumentException("Unknown FHIR version: " + fhirVersion);
    }
    final FhirContext fhirContext = FhirContext.forCached(fhirVersionEnum);
    try {
      this.resourceDefinition = requireNonNull(fhirContext.getResourceDefinition(resourceType));
    } catch (final DataFormatException | NullPointerException e) {
      throw new IllegalArgumentException("Unknown resource type: " + resourceType, e);
    }
    this.datasetTransformer = new DatasetTransformer(readTransforms);
  }

  @NotNull
  private static Stream<Column> transformDecimal(@NotNull final ColumnDescriptor descriptor) {
    final Column original = transformColumn(descriptor.column(),
        c -> c.cast(DataTypes.StringType), descriptor.name(), descriptor.type());
    final Column numeric = transformColumn(descriptor.column(),
        c -> c.cast(DataTypes.createDecimalType(38, 6)), "__" + descriptor.name() + "_numeric",
        descriptor.type());
    return Stream.of(original, numeric);
  }

  @NotNull
  private static Stream<Column> transformInteger32(@NotNull final ColumnDescriptor descriptor) {
    final Column result = transformColumn(descriptor.column(),
        c -> c.cast(DataTypes.IntegerType), descriptor.name(), descriptor.type());
    return Stream.of(result);
  }

  @NotNull
  private static Stream<Column> transformInteger64(@NotNull final ColumnDescriptor descriptor) {
    final Column result = transformColumn(descriptor.column(),
        c -> c.cast(DataTypes.LongType), descriptor.name(), descriptor.type());
    return Stream.of(result);
  }

  @NotNull
  private static Stream<Column> transformBinary(@NotNull final ColumnDescriptor descriptor) {
    final Column result = transformColumn(descriptor.column(),
        c -> unbase64(c).cast(DataTypes.BinaryType), descriptor.name(), descriptor.type());
    return Stream.of(result);
  }

  @NotNull
  public Dataset<Row> read(@NotNull final String path) {
    return read(new String[]{path});
  }

  @NotNull
  public Dataset<Row> read(@NotNull final String... paths) {
    final DataFrameReader reader = spark.read();
    if (options != null && datasetTransformer != null && resourceDefinition != null) {
      options.keySet().forEach(key -> reader.option(key, options.get(key)));
      final Dataset<Row> json = reader.json(paths);
      return datasetTransformer.transformDataset(json, resourceDefinition);
    }
    return reader.json(paths);
  }

  public Dataset<Row> read(@NotNull final List<String> jsonStrings) {
    final Dataset<Row> jsonData = spark.createDataset(jsonStrings, Encoders.STRING())
        .toDF("jsonString");
    final Dataset<Row> structuredData = spark.read()
        .json(jsonData.select("jsonString").as(Encoders.STRING()));
    if (datasetTransformer != null && resourceDefinition != null) {
      return datasetTransformer.transformDataset(structuredData, resourceDefinition);
    }
    return structuredData;
  }


}
