package au.csiro.pathling.schema;

import static au.csiro.pathling.schema.SchemaTransformer.transformColumn;
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
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;

public class FhirJsonWriter {

  @Nonnull
  private static final Map<String, Function<ColumnDescriptor, Stream<Column>>> writeTransforms = Map.of(
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
  private static Stream<Column> transformDecimal(@Nonnull final ColumnDescriptor descriptor) {
    final Column result = transformColumn(descriptor.column(), c -> c.cast(
        DataTypes.createDecimalType(38, 6)), descriptor.name(), descriptor.type());
    return Stream.of(result);
  }

  @Nonnull
  private static Stream<Column> transformBinary(@Nonnull final ColumnDescriptor descriptor) {
    final Column result = transformColumn(descriptor.column(), c -> regexp_replace(base64(c),
        "[\r\n]", ""), descriptor.name(), descriptor.type());
    return Stream.of(result);
  }

}
