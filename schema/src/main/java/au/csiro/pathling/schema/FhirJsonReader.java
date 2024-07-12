package au.csiro.pathling.schema;

import static java.util.Objects.requireNonNull;
import static org.apache.spark.sql.functions.unbase64;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import ca.uhn.fhir.parser.DataFormatException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

public class FhirJsonReader {

  @Nonnull
  private static final Map<String, BiFunction<Column, String, Stream<Column>>> readTransforms = Map.of(
      "decimal", FhirJsonReader::transformDecimal,
      "integer", FhirJsonReader::transformInteger32,
      "positiveInt", FhirJsonReader::transformInteger32,
      "unsignedInt", FhirJsonReader::transformInteger32,
      "integer64", FhirJsonReader::transformInteger64,
      "base64Binary", FhirJsonReader::transformBinary
  );

  @Nonnull
  private final SparkSession spark;

  @Nonnull
  private final RuntimeResourceDefinition resourceDefinition;

  @Nonnull
  private final SchemaTransformer schemaTransformer;

  public FhirJsonReader(@Nullable final SparkSession spark, @Nullable final String fhirVersion,
      @Nullable final String resourceType) throws IllegalArgumentException {
    this.spark = requireNonNull(spark);
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

  public Dataset<Row> read(@Nullable final String path) {
    if (path == null) {
      throw new IllegalArgumentException("Path must not be null");
    }
    final Dataset<Row> json = spark.read().option("multiLine", "true").json(path);
    return schemaTransformer.transformDataset(json, resourceDefinition);
  }

  @Nonnull
  private static Stream<Column> transformDecimal(@Nonnull final Column column,
      @Nonnull final String name) {
    return Stream.of(
        column.cast(DataTypes.StringType).alias(name),
        column.cast(DataTypes.createDecimalType(38, 6)).alias("__" + name + "_numeric")
    );
  }

  @Nonnull
  private static Stream<Column> transformInteger32(@Nonnull final Column column,
      @Nonnull final String name) {
    return Stream.of(column.cast(DataTypes.IntegerType).alias(name));
  }

  @Nonnull
  private static Stream<Column> transformInteger64(@Nonnull final Column column,
      @Nonnull final String name) {
    return Stream.of(column.cast(DataTypes.LongType).alias(name));
  }

  @Nonnull
  private static Stream<Column> transformBinary(@Nonnull final Column column,
      @Nonnull final String name) {
    return Stream.of(unbase64(column).alias(name));
  }

}
