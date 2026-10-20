package au.csiro.pathling.spike08.orig;

import static au.csiro.pathling.spike08.orig.DatasetTransformer.transformColumn;
import static java.util.Objects.requireNonNull;
import static org.apache.spark.sql.functions.base64;
import static org.apache.spark.sql.functions.regexp_replace;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import ca.uhn.fhir.parser.DataFormatException;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Writes a {@link Dataset} to FHIR JSON format, according to the Parquet on FHIR specification.
 *
 * @author John Grimes
 * @see <a href="https://github.com/aehrc/parquet-on-fhir">Parquet on FHIR</a>
 */
public class FhirJsonWriter {

  /**
   * A set of transforms that are applied to columns of specific types when writing FHIR JSON data.
   */
  @NotNull
  private static final Map<String, Function<ColumnDescriptor, Stream<Column>>> writeTransforms = Map.of(
      "decimal", FhirJsonWriter::transformDecimal,
      "base64Binary", FhirJsonWriter::transformBinary
  );

  @NotNull
  private final RuntimeResourceDefinition resourceDefinition;

  @NotNull
  private final DatasetTransformer datasetTransformer;

  public FhirJsonWriter(@NotNull final String fhirVersion, @NotNull final String resourceType) {
    @Nullable final FhirVersionEnum fhirVersionEnum = FhirVersionEnum.forVersionString(fhirVersion);
    if (fhirVersionEnum == null) {
      throw new IllegalArgumentException("Unknown FHIR version: " + fhirVersion);
    }
    final FhirContext fhirContext = FhirContext.forCached(fhirVersionEnum);
    try {
      this.resourceDefinition = requireNonNull(fhirContext.getResourceDefinition(resourceType));
    } catch (final DataFormatException | NullPointerException e) {
      throw new IllegalArgumentException("Unknown resource type: " + resourceType, e);
    }
    this.datasetTransformer = new DatasetTransformer(writeTransforms);
  }

  public void write(@NotNull final Dataset<Row> data, @NotNull final String path) {
    datasetTransformer.transformDataset(data, resourceDefinition).write().json(path);
  }

  // Spike 08: the dataset that write() hands to the JSON writer, so that the harness can render it without
  // writing files.
  @NotNull
  public Dataset<Row> transform(@NotNull final Dataset<Row> data) {
    return datasetTransformer.transformDataset(data, resourceDefinition);
  }

  @NotNull
  private static Stream<Column> transformDecimal(@NotNull final ColumnDescriptor descriptor) {
    final Column result = transformColumn(descriptor.column(), c -> c.cast(
        DataTypes.createDecimalType(38, 6)), descriptor.name(), descriptor.type());
    return Stream.of(result);
  }

  @NotNull
  private static Stream<Column> transformBinary(@NotNull final ColumnDescriptor descriptor) {
    final Column result = transformColumn(descriptor.column(), c -> regexp_replace(base64(c),
        "[\r\n]", ""), descriptor.name(), descriptor.type());
    return Stream.of(result);
  }

}
