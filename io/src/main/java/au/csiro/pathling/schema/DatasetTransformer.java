package au.csiro.pathling.schema;

import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.transform;

import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementCompositeDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
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
import org.hl7.fhir.instance.model.api.IBase;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import scala.Function1;

/**
 * Transforms a dataset for both reading and writing, applying a set of transforms to columns of
 * specific types.
 *
 * @param transforms a map of transforms to apply to columns of specific types
 * @author John Grimes
 */
public record DatasetTransformer(
    @NotNull Map<String, Function<ColumnDescriptor, Stream<Column>>> transforms) {

  @NotNull
  private static Optional<BaseRuntimeChildDefinition> getChildDefinition(
      @NotNull final BaseRuntimeElementCompositeDefinition<? extends IBase> compositeDefinition,
      @NotNull final String fieldName) {
    return Optional.ofNullable(compositeDefinition.getChildByName(fieldName));
  }

  @NotNull
  private static BaseRuntimeElementDefinition<?> childToElementDefinition(
      @NotNull final BaseRuntimeChildDefinition child, @NotNull final String columnName) {
    return Optional.ofNullable(
        child.getChildByName(columnName)).orElseThrow(() -> new AssertionError(
        "Failed to retrieve element definition from child definition: " + columnName));
  }

  @NotNull
  public static Column transformColumn(@NotNull final Column column,
      @NotNull final Function1<Column, Column> transformation, @NotNull final String columnName,
      @NotNull final DataType dataType) {
    return dataType instanceof ArrayType
           ? transform(column, transformation).alias(columnName)
           : transformation.apply(column).alias(columnName);
  }

  @NotNull
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
          final @NotNull Optional<BaseRuntimeChildDefinition> childDefinition = getChildDefinition(
              resourceDefinition, columnName);
          return childDefinition.map(child -> {
                final DataType dataType = column.expr().dataType();
                return transformField(column, dataType,
                    childToElementDefinition(child, columnName), columnName);
              })
              .orElse(Stream.of(column));
        })
        .toList();
    return dataset.select(columns.toArray(Column[]::new));
  }

  @NotNull
  private Stream<Column> transformField(@NotNull final Column column,
      @NotNull final DataType dataType,
      @NotNull final BaseRuntimeElementDefinition<? extends IBase> elementDefinition,
      final String columnName) {
    // Delegate processing of structs and arrays to their respective methods.
    final Column candidateColumn;
    if (dataType instanceof StructType) {
      if (elementDefinition instanceof BaseRuntimeElementCompositeDefinition) {
        candidateColumn = transformStruct(column, (StructType) dataType,
            (BaseRuntimeElementCompositeDefinition<? extends IBase>) elementDefinition, columnName);
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

  @NotNull
  private Column transformStruct(@NotNull final Column struct,
      @NotNull final StructType structType,
      @NotNull final BaseRuntimeElementCompositeDefinition<? extends IBase> compositeDefinition,
      @NotNull final String columnName) {
    final List<Column> fields = Stream.of(structType.fields())
        // Drop annotations.
        .filter(field -> !field.name().startsWith("__"))
        .flatMap(field -> {
          // Let primitive extensions and annotations pass through unaltered.
          final String fieldName = field.name();
          final Stream<Column> passThroughResult = Stream.of(struct.getField(fieldName));
          if (fieldName.startsWith("_")) {
            return passThroughResult;
          }
          // Traverse to the definition of the field within the FHIR element.
          final @NotNull Optional<BaseRuntimeChildDefinition> fieldChildDefinition = getChildDefinition(
              compositeDefinition, fieldName
          );

          return fieldChildDefinition.map(childDefinition -> {
            // Retrieve the child element definition from the child definition.
            final BaseRuntimeElementDefinition<? extends IBase> fieldDefinition = childToElementDefinition(
                childDefinition, fieldName);

            final DataType fieldType = structType.fields()[structType.fieldIndex(
                fieldName)].dataType();
            Column fieldColumn = struct.getField(fieldName);
            // If the column name does not match the field name, alias it. This happens when we 
            // traverse into arrays.
            if (!fieldColumn.toString().equals(fieldName)) {
              fieldColumn = fieldColumn.alias(fieldName);
            }
            return transformField(fieldColumn, fieldType, fieldDefinition, fieldName);
          }).orElse(passThroughResult);
        })
        .toList();
    return struct(fields.toArray(Column[]::new)).alias(columnName);
  }

  @NotNull
  private Column transformArray(@NotNull final Column array, @NotNull final ArrayType arrayType,
      @NotNull final BaseRuntimeElementDefinition<? extends IBase> elementDefinition,
      @NotNull final String columnName) {
    final DataType elementType = arrayType.elementType();

    // Transform any nested struct or array.
    final Column transformed = transform(array, element -> {
      if (elementType instanceof StructType) {
        if (elementDefinition instanceof BaseRuntimeElementCompositeDefinition) {
          return transformStruct(element, (StructType) elementType,
              (BaseRuntimeElementCompositeDefinition<? extends IBase>) elementDefinition,
              columnName);
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

}
