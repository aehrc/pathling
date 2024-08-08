package au.csiro.pathling.schema;

import static au.csiro.pathling.schema.SchemaTransformer.transformColumn;
import static java.util.Objects.requireNonNull;
import static org.apache.spark.sql.functions.unbase64;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import ca.uhn.fhir.parser.DataFormatException;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import scala.compat.java8.OptionConverters;

public class FhirJsonReader {

  private static final Map<String, Function<ColumnDescriptor, Stream<Column>>> readTransforms = Map.of(
      "decimal", FhirJsonReader::transformDecimal,
      "integer", FhirJsonReader::transformInteger32,
      "positiveInt", FhirJsonReader::transformInteger32,
      "unsignedInt", FhirJsonReader::transformInteger32,
      "integer64", FhirJsonReader::transformInteger64,
      "base64Binary", FhirJsonReader::transformBinary
  );

  private final SparkSession spark;

  private final Map<String, String> options;

  private final RuntimeResourceDefinition resourceDefinition;

  private final SchemaTransformer schemaTransformer;

  public FhirJsonReader(final String resourceType, final String fhirVersion,
      final Map<String, String> options) throws IllegalArgumentException {
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
    this.schemaTransformer = new SchemaTransformer(readTransforms);
  }

  public Dataset<Row> read(final String path) {
    if (path == null) {
      throw new IllegalArgumentException("Path must not be null");
    }
    return read(new String[]{path});
  }

  public Dataset<Row> read(final String... paths) {
    if (paths == null) {
      throw new IllegalArgumentException("Paths must not be null");
    }
    final DataFrameReader reader = spark.read();
    options.keySet().forEach(key -> reader.option(key, options.get(key)));
    final Dataset<Row> json = reader.json(paths);
    return schemaTransformer.transformDataset(json, resourceDefinition);
  }

  private static Stream<Column> transformDecimal(final ColumnDescriptor descriptor) {
    final Column original = transformColumn(descriptor.column(),
        c -> c.cast(DataTypes.StringType), descriptor.name(), descriptor.type());
    final Column numeric = transformColumn(descriptor.column(),
        c -> c.cast(DataTypes.createDecimalType(38, 6)), "__" + descriptor.name() + "_numeric",
        descriptor.type());
    return Stream.of(original, numeric);
  }

  private static Stream<Column> transformInteger32(final ColumnDescriptor descriptor) {
    final Column result = transformColumn(descriptor.column(),
        c -> c.cast(DataTypes.IntegerType), descriptor.name(), descriptor.type());
    return Stream.of(result);
  }

  private static Stream<Column> transformInteger64(final ColumnDescriptor descriptor) {
    final Column result = transformColumn(descriptor.column(),
        c -> c.cast(DataTypes.LongType), descriptor.name(), descriptor.type());
    return Stream.of(result);
  }

  private static Stream<Column> transformBinary(final ColumnDescriptor descriptor) {
    final Column result = transformColumn(descriptor.column(),
        c -> unbase64(c).cast(DataTypes.BinaryType), descriptor.name(), descriptor.type());
    return Stream.of(result);
  }

}
