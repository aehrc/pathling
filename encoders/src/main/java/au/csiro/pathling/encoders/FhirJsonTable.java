package au.csiro.pathling.encoders;

import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.FileFormat;
import org.apache.spark.sql.execution.datasources.v2.json.JsonTable;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.Option;
import scala.Some;
import scala.collection.Seq;

public class FhirJsonTable extends JsonTable {

  private final RuntimeResourceDefinition resourceDefinition;

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
      final StructType modifiedSchema = processStruct(schema, resourceDefinition);
      return new Some<>(modifiedSchema);
    }
    return maybeSchema;
  }

  @Nonnull
  private static StructType processStruct(@Nonnull final StructType schema,
      @Nonnull final BaseRuntimeElementDefinition elementDefinition) {
    final List<StructField> fields = Arrays.asList(schema.fields());
    final Stream<StructField> processedFields = fields.stream()
        .flatMap(f -> processField(f, elementDefinition));
    return new StructType(processedFields.toArray(StructField[]::new));
  }

  @Nonnull
  private static Stream<StructField> processField(@Nonnull final StructField field,
      @Nonnull final BaseRuntimeElementDefinition elementDefinition) {
    if (field.name().equals("resourceType")) {
      return Stream.of(field);
    }
    final BaseRuntimeChildDefinition child = Optional.ofNullable(
            elementDefinition.getChildByName(field.name()))
        .orElseThrow(() -> new IllegalArgumentException(
            "Field does not match a corresponding FHIR element: " + field.name()));
    final BaseRuntimeElementDefinition<?> childElement = Optional.ofNullable(
        child.getChildByName(field.name())).orElseThrow(() -> new AssertionError(
        "Failed to retrieve element definition from child definition: " + field.name()));
    final DataType dataType = field.dataType();
    if (dataType instanceof StructType) {
      return Stream.of(new StructField(field.name(),
          processStruct((StructType) dataType, childElement), field.nullable(),
          field.metadata()));
    } else if (dataType instanceof ArrayType) {
      return Stream.of(new StructField(field.name(),
          processArray((ArrayType) dataType, childElement), field.nullable(), field.metadata()));
    }
    final String fhirType = childElement.getName();
    return switch (fhirType) {
      case "decimal" ->
          Stream.of(new StructField(field.name(), DataTypes.StringType, field.nullable(),
              field.metadata()));
      case "integer", "positiveInt" ->
          Stream.of(new StructField(field.name(), DataTypes.IntegerType, field.nullable(),
              field.metadata()));
      case "integer64" ->
          Stream.of(new StructField(field.name(), DataTypes.LongType, field.nullable(),
              field.metadata()));
      default -> Stream.of(field);
    };
  }

  @Nonnull
  private static ArrayType processArray(@Nonnull final ArrayType array,
      @Nonnull final BaseRuntimeElementDefinition elementDefinition) {
    final DataType elementType = array.elementType();
    if (elementType instanceof StructType) {
      return new ArrayType(processStruct((StructType) elementType, elementDefinition),
          array.containsNull());
    }
    return array;
  }

}
