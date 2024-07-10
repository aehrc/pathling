package au.csiro.pathling.encoders;

import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public record SchemaCaster(@Nonnull Map<String, DataType> casts) {

  @Nonnull
  public StructType processStruct(@Nonnull final StructType schema,
      @Nonnull final BaseRuntimeElementDefinition elementDefinition) {
    final List<StructField> fields = Arrays.asList(schema.fields());
    final Stream<StructField> processedFields = fields.stream()
        .flatMap(f -> processField(f, elementDefinition));
    return new StructType(processedFields.toArray(StructField[]::new));
  }

  @Nonnull
  private Stream<StructField> processField(@Nonnull final StructField field,
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
    final Optional<DataType> cast = Optional.ofNullable(casts.get(fhirType));
    return cast
        .map(type -> Stream.of(
            new StructField(field.name(), type, field.nullable(), field.metadata())))
        .orElse(Stream.of(field));
  }

  @Nonnull
  private ArrayType processArray(@Nonnull final ArrayType array,
      @Nonnull final BaseRuntimeElementDefinition elementDefinition) {
    final DataType elementType = array.elementType();
    if (elementType instanceof StructType) {
      return new ArrayType(processStruct((StructType) elementType, elementDefinition),
          array.containsNull());
    }
    return array;
  }

}
