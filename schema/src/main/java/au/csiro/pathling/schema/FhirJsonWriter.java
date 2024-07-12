package au.csiro.pathling.schema;

import static java.util.Objects.requireNonNull;
import static org.apache.spark.sql.functions.base64;
import static org.apache.spark.sql.functions.regexp_replace;

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
import org.apache.spark.sql.types.DataTypes;

public class FhirJsonWriter {

  @Nonnull
  private static final Map<String, BiFunction<Column, String, Stream<Column>>> writeTransforms = Map.of(
      "decimal", FhirJsonWriter::transformDecimal,
      "base64Binary", FhirJsonWriter::transformBinary
  );

  @Nonnull
  private final RuntimeResourceDefinition resourceDefinition;

  @Nonnull
  private final SchemaTransformer schemaTransformer;

  public FhirJsonWriter(@Nullable final String fhirVersion, @Nullable final String resourceType) {
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
    this.schemaTransformer = new SchemaTransformer(writeTransforms);
  }

  public void write(@Nullable final Dataset<Row> data, @Nullable final String path) {
    if (data == null) {
      throw new IllegalArgumentException("Data must not be null");
    }
    if (path == null) {
      throw new IllegalArgumentException("Path must not be null");
    }
    schemaTransformer.transformDataset(data, resourceDefinition).write().json(path);
  }

  @Nonnull
  private static Stream<Column> transformDecimal(@Nonnull final Column column,
      @Nonnull final String columnName) {
    return Stream.of(column.cast(DataTypes.createDecimalType(38, 6)).alias(columnName));
  }

  @Nonnull
  private static Stream<Column> transformBinary(@Nonnull final Column column,
      @Nonnull final String columnName) {
    return Stream.of(regexp_replace(base64(column), "[\r\n]", "").alias(columnName));
  }

}
