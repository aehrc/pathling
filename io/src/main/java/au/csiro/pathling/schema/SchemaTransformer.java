package au.csiro.pathling.schema;

import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.transform;

import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementCompositeDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import scala.Function1;

public record SchemaTransformer(
    @Nonnull Map<String, Function<ColumnDescriptor, Stream<Column>>> transforms) {

  @Nonnull
  public Dataset<Row> transformDataset(@Nullable final Dataset<Row> dataset,
      @Nullable final RuntimeResourceDefinition resourceDefinition) {
    if (dataset == null) {
      throw new IllegalArgumentException("Dataset must not be null");
    } else if (resourceDefinition == null) {
      throw new IllegalArgumentException("Resource definition must not be null");
    }
    final List<String> columnNames = List.of(dataset.columns());
    final List<Column> columns = columnNames.stream()
        // Drop contained resources.
        .filter(columnName -> !columnName.equals("contained"))
        // Drop annotations.
        .filter(columnName -> !columnName.startsWith("__"))
        .flatMap(columnName -> {
          if (columnName.equals("resourceType") || columnName.startsWith("_")) {
            return Stream.of(dataset.col(columnName));
          }
          final Column column = dataset.col(columnName);
          final BaseRuntimeChildDefinition childDefinition = getChildDefinition(
              resourceDefinition, columnName,
              "Field does not match a corresponding FHIR element: " + columnName);
          final DataType dataType = column.expr().dataType();
          return transformField(column, dataType,
              childToElementDefinition(childDefinition, columnName), columnName);
        })
        .toList();
    return dataset.select(columns.toArray(Column[]::new));
  }

  @Nonnull
  private Stream<Column> transformField(@Nonnull final Column column,
      @Nonnull final DataType dataType,
      @Nonnull final BaseRuntimeElementDefinition elementDefinition, final String columnName) {
    // Delegate processing of structs and arrays to their respective methods.
    final Column candidateColumn;
    if (dataType instanceof StructType) {
      if (elementDefinition instanceof BaseRuntimeElementCompositeDefinition) {
        candidateColumn = transformStruct(column, (StructType) dataType,
            (BaseRuntimeElementCompositeDefinition) elementDefinition, columnName);
      } else {
        throw new RuntimeException("Struct field does not match a composite FHIR element: "
            + columnName);
      }
    } else if (dataType instanceof ArrayType) {
      candidateColumn = transformArray(column, (ArrayType) dataType, elementDefinition,
          columnName);
    } else {
      candidateColumn = column;
    }

    // If there is a transform for this data type, apply it.
    // Otherwise return the column unaltered.
    final ColumnDescriptor descriptor = new ColumnDescriptor(candidateColumn, columnName, dataType);
    return Optional.ofNullable(transforms.get(elementDefinition.getName()))
        .map(transform -> transform.apply(descriptor))
        .orElse(Stream.of(candidateColumn));
  }

  @Nonnull
  private Column transformStruct(@Nonnull final Column struct,
      @Nonnull final StructType structType,
      @Nonnull final BaseRuntimeElementCompositeDefinition compositeDefinition,
      @Nonnull final String columnName) {
    final List<Column> fields = Stream.of(structType.fields())
        // Drop annotations.
        .filter(field -> !field.name().startsWith("__"))
        .flatMap(field -> {
          // Let primitive extensions and annotations pass through unaltered.
          final String fieldName = field.name();
          if (fieldName.startsWith("_")) {
            return Stream.of(struct.getField(fieldName));
          }
          // Traverse to the definition of the field within the FHIR element.
          final BaseRuntimeChildDefinition fieldChildDefinition = getChildDefinition(
              compositeDefinition, fieldName,
              "Field does not match a corresponding FHIR element: " + columnName + "."
                  + fieldName);

          // Retrieve the child element definition from the child definition.
          final BaseRuntimeElementDefinition fieldDefinition = childToElementDefinition(
              fieldChildDefinition, fieldName);

          final DataType fieldType = structType.fields()[structType.fieldIndex(
              fieldName)].dataType();
          Column fieldColumn = struct.getField(fieldName);
          // If the column name does not match the field name, alias it. This happens when we 
          // traverse into arrays.
          if (!fieldColumn.toString().equals(fieldName)) {
            fieldColumn = fieldColumn.alias(fieldName);
          }
          return transformField(fieldColumn, fieldType, fieldDefinition, fieldName);
        })
        .toList();
    return struct(fields.toArray(Column[]::new)).alias(columnName);
  }

  @Nonnull
  private Column transformArray(@Nonnull final Column array, @Nonnull final ArrayType arrayType,
      @Nonnull final BaseRuntimeElementDefinition elementDefinition,
      @Nonnull final String columnName) {
    final DataType elementType = arrayType.elementType();

    // Transform any nested struct or array.
    final Column transformed = transform(array, element -> {
      if (elementType instanceof StructType) {
        if (elementDefinition instanceof BaseRuntimeElementCompositeDefinition) {
          return transformStruct(element, (StructType) elementType,
              (BaseRuntimeElementCompositeDefinition) elementDefinition, columnName);
        } else {
          throw new RuntimeException("Struct field does not match a composite FHIR element: "
              + columnName);
        }
      } else if (elementType instanceof ArrayType) {
        return transformArray(element, (ArrayType) elementType, elementDefinition, columnName);
      } else {
        return element;
      }
    });
    return transformed.alias(columnName);

  }

  @Nonnull
  private static BaseRuntimeChildDefinition getChildDefinition(
      @Nonnull final BaseRuntimeElementCompositeDefinition compositeDefinition,
      @Nonnull final String fieldName, @Nonnull final String errorMessage) {
    return Optional.ofNullable(
            compositeDefinition.getChildByName(fieldName))
        // If there is no such field in the FHIR definition, throw an exception.
        .orElseThrow(() -> new IllegalArgumentException(
            errorMessage));
  }

  @Nonnull
  private static BaseRuntimeElementDefinition<?> childToElementDefinition(
      @Nonnull final BaseRuntimeChildDefinition child, @Nonnull final String columnName) {
    return Optional.ofNullable(
        child.getChildByName(columnName)).orElseThrow(() -> new AssertionError(
        "Failed to retrieve element definition from child definition: " + columnName));
  }

  @Nonnull
  public static Column transformColumn(@Nonnull final Column column,
      @Nonnull final Function1<Column, Column> transformation, @Nonnull final String columnName,
      @Nonnull final DataType dataType) {
    return dataType instanceof ArrayType
           ? transform(column, transformation).alias(columnName)
           : transformation.apply(column).alias(columnName);
  }

}
