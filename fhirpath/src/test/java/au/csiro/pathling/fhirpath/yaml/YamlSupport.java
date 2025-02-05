package au.csiro.pathling.fhirpath.yaml;

import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

import au.csiro.pathling.fhirpath.collection.DecimalCollection;
import au.csiro.pathling.fhirpath.definition.ChildDefinition;
import au.csiro.pathling.fhirpath.definition.ResourceDefinition;
import au.csiro.pathling.fhirpath.definition.def.DefCompositeDefinition;
import au.csiro.pathling.fhirpath.definition.def.DefPrimitiveDefinition;
import au.csiro.pathling.fhirpath.definition.def.DefResourceDefinition;
import au.csiro.pathling.fhirpath.definition.def.DefResourceTag;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.experimental.UtilityClass;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.yaml.snakeyaml.Yaml;

@UtilityClass
public class YamlSupport {

  static final Yaml YAML = new Yaml();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  
  static Map<FHIRDefinedType, DataType> FHIR_TO_SQL = Map.of(
      FHIRDefinedType.STRING, org.apache.spark.sql.types.DataTypes.StringType,
      FHIRDefinedType.INTEGER, DataTypes.IntegerType,
      FHIRDefinedType.BOOLEAN, DataTypes.BooleanType,
      FHIRDefinedType.DECIMAL, DecimalCollection.DECIMAL_TYPE,
      FHIRDefinedType.NULL, DataTypes.NullType
  );

  @Nonnull
  public static ResourceDefinition yamlToDefinition(@Nonnull final String resourcCode,
      @Nonnull final Map<Object, Object> data) {
    final List<ChildDefinition> definedFields = elementsFromYaml(data);
    final Set<String> definedFieldNames = definedFields.stream()
        .map(ChildDefinition::getName)
        .collect(Collectors.toUnmodifiableSet());

    return DefResourceDefinition.of(
        DefResourceTag.of(resourcCode),
        Stream.concat(
            Stream.of(
                    DefPrimitiveDefinition.single("id", FHIRDefinedType.STRING),
                    DefPrimitiveDefinition.single("id_versioned", FHIRDefinedType.STRING)
                )
                .filter(field -> !definedFieldNames.contains(field.getName())),
            definedFields.stream()
        ).toList()
    );
  }

  @Nonnull
  static List<ChildDefinition> elementsFromYaml(@Nonnull final Map<Object, Object> data) {
    return data.entrySet().stream()
        .map(entry -> elementFromYaml(entry.getKey().toString(), entry.getValue()))
        .toList();
  }

  static ChildDefinition elementFromYaml(String key, Object value) {
    if (value instanceof List<?> list) {
      return elementFromValues(key, list);
    } else {
      return elementFromValue(key, value, 1);
    }
  }

  static ChildDefinition elementFromValues(@Nonnull String key, @Nonnull List<?> values) {

    // the problem here is that we only want to support lists of the same types.
    // we need to check the first element and then check the rest of the elements
    // and not nested 
    // also how do we represent an empty list of a certain type?

    // TODO: what do do with null values in lists

    final Set<Class<?>> types = values.stream()
        .filter(Objects::nonNull)
        .map(Object::getClass)
        .collect(Collectors.toUnmodifiableSet());

    if (types.size() == 1) {
      return elementFromValue(key, values.get(0), -1);
    } else if (types.size() > 1) {
      throw new IllegalArgumentException("Unsupported list with multiple types: " + types);
    } else {
      return elementFromValue(key, null, -1);
    }
  }

  @SuppressWarnings("unchecked")
  @Nonnull
  private static ChildDefinition elementFromValue(@Nonnull final String key,
      @Nullable final Object value, final int cardinality) {
    if (isNull(value)) {
      return DefPrimitiveDefinition.of(key, FHIRDefinedType.NULL, cardinality);
    } else if (value instanceof String) {
      return DefPrimitiveDefinition.of(key, FHIRDefinedType.STRING, cardinality);
    } else if (value instanceof Integer) {
      return DefPrimitiveDefinition.of(key, FHIRDefinedType.INTEGER, cardinality);
    } else if (value instanceof Boolean) {
      return DefPrimitiveDefinition.of(key, FHIRDefinedType.BOOLEAN, cardinality);
    } else if (value instanceof Double) {
      return DefPrimitiveDefinition.of(key, FHIRDefinedType.DECIMAL, cardinality);
    } else if (value instanceof Map<?, ?> map) {
      return DefCompositeDefinition.of(key, elementsFromYaml((Map<Object, Object>) map),
          cardinality);
    } else {
      throw new IllegalArgumentException("Unsupported data type: " + value + " (" + value.getClass()
          .getName() + ")");
    }
  }

  @Nonnull
  public static StructType defnitiontoStruct(
      @Nonnull final DefResourceDefinition resourceDefinition) {
    return childrendToStruct(resourceDefinition.getChildren());
  }

  @Nonnull
  static StructType childrendToStruct(
      @Nonnull final List<ChildDefinition> childDefinitions) {
    return new StructType(
        childDefinitions.stream()
            .map(YamlSupport::elementToStructField)
            .toArray(StructField[]::new)
    );
  }

  private static StructField elementToStructField(ChildDefinition childDefinition) {
    if (childDefinition instanceof DefPrimitiveDefinition primitiveDefinition) {
      final DataType elementType = requireNonNull(
          FHIR_TO_SQL.get(primitiveDefinition.getType()),
          "No SQL type for " + primitiveDefinition.getFhirType());
      return new StructField(
          primitiveDefinition.getName(),
          primitiveDefinition.getCardinality() < 0
          ? new ArrayType(elementType, true)
          : elementType,
          true, Metadata.empty()
      );
    } else if (childDefinition instanceof DefCompositeDefinition compositeDefinition) {
      final StructType elementType = childrendToStruct(compositeDefinition.getChildren());
      return new StructField(
          compositeDefinition.getName(),
          compositeDefinition.getCardinality() < 0
          ? new ArrayType(elementType, true)
          : elementType,
          true, Metadata.empty()
      );
    } else {
      throw new IllegalArgumentException("Unsupported child definition: " + childDefinition);
    }
  }

  @Nonnull
  static String omToJson(@Nonnull final Map<Object, Object> objectModel) {
    try {
      return OBJECT_MAPPER.writeValueAsString(objectModel);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  //
  //
  // @Nonnull
  // static ResourceDefinition fromStruct(@Nonnull final String resourcCode,
  //     @Nonnull final StructType resourceSchema) {
  //   return DefResourceDefinition.of(
  //       DefResourceTag.of(resourcCode),
  //       elementsFromTypes(resourceSchema.fields())
  //   );
  // }
  //
  //
  // @Nonnull
  // static List<ChildDefinition> elementsFromTypes(StructField[] fields) {
  //   return Stream.of(fields)
  //       .map(YamlTest::elementFromType)
  //       .toList();
  // }
  //
  //
  // @Nonnull
  // private static ChildDefinition elementFromType(@Nonnull final String name,
  //     @Nonnull final DataType dataType, int cardinality) {
  //   if (dataType instanceof StructType structType) {
  //     return DefCompositeDefinition.of(name, elementsFromTypes(structType.fields()), cardinality);
  //   } else {
  //     switch (dataType.typeName()) {
  //       case "string":
  //         return DefPrimitiveDefinition.of(name, FHIRDefinedType.STRING, cardinality);
  //       case "long":
  //       case "integer":
  //         return DefPrimitiveDefinition.of(name, FHIRDefinedType.INTEGER, cardinality);
  //       case "boolean":
  //         return DefPrimitiveDefinition.of(name, FHIRDefinedType.BOOLEAN, cardinality);
  //       default:
  //         throw new IllegalArgumentException("Unsupported data type: " + dataType);
  //     }
  //   }
  // }
  //
  // @Nonnull
  // private static ChildDefinition elementFromType(@Nonnull final StructField structField) {
  //   final DataType dataType = structField.dataType();
  //   if (dataType instanceof ArrayType arrayType) {
  //     return elementFromType(structField.name(), arrayType.elementType(), -1);
  //   } else {
  //     return elementFromType(structField.name(), dataType, 1);
  //   }
  // }

}
