package au.csiro.pathling.test.yaml;

import static au.csiro.pathling.utilities.Strings.randomAlias;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

import au.csiro.pathling.fhirpath.collection.DecimalCollection;
import au.csiro.pathling.fhirpath.definition.ChildDefinition;
import au.csiro.pathling.fhirpath.definition.ResourceDefinition;
import au.csiro.pathling.fhirpath.definition.def.DefCompositeDefinition;
import au.csiro.pathling.fhirpath.definition.def.DefPrimitiveDefinition;
import au.csiro.pathling.fhirpath.definition.def.DefResourceDefinition;
import au.csiro.pathling.fhirpath.definition.def.DefResourceTag;
import au.csiro.pathling.fhirpath.literal.CodingLiteral;
import au.csiro.pathling.test.helpers.SparkHelpers;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.experimental.UtilityClass;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.AbstractConstruct;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Represent;
import org.yaml.snakeyaml.representer.Representer;

@UtilityClass
public class YamlSupport {

  public class FhirTypedLiteralSerializer extends JsonSerializer<FhirTypedLiteral> {

    @Override
    public void serialize(FhirTypedLiteral fhirLiteral, JsonGenerator gen,
        SerializerProvider serializers) throws IOException {
      if (nonNull(fhirLiteral.getLiteral())) {
        switch (fhirLiteral.getType()) {
          case CODING:
            writeCoding(fhirLiteral, gen);
            break;
          case DATETIME:
          case DATE:
            writeDate(fhirLiteral, gen);
            break;
          case TIME:
            writeTime(fhirLiteral, gen);
            break;
          default:
            throw new IllegalArgumentException("Unsupported FHIR type: " + fhirLiteral.getType());
        }
      } else {
        gen.writeNull();
      }
    }

    private void writeCoding(FhirTypedLiteral fhirLiteral, JsonGenerator gen) throws IOException {
      final Coding coding = CodingLiteral.fromString(fhirLiteral.getLiteral());
      // write as object to gen using low level api
      gen.writeStartObject();
      gen.writeStringField("id", randomAlias());
      gen.writeStringField("system", coding.getSystem());
      gen.writeStringField("version", coding.getVersion());
      gen.writeStringField("code", coding.getCode());
      gen.writeStringField("display", coding.getDisplay());
      gen.writeEndObject();
    }

    private void writeDate(FhirTypedLiteral fhirLiteral, JsonGenerator gen) throws IOException {
      gen.writeString(fhirLiteral.getLiteral().replaceFirst("^@", ""));
    }

    private void writeTime(FhirTypedLiteral fhirLiteral, JsonGenerator gen) throws IOException {
      gen.writeString(fhirLiteral.getLiteral().replaceFirst("^@T", ""));
    }
  }

  @JsonSerialize(using = FhirTypedLiteralSerializer.class)
  @Value(staticConstructor = "of")
  public static class FhirTypedLiteral {

    @Nonnull
    FHIRDefinedType type;
    @Nullable
    String literal;

    @Nonnull
    public String getTag() {
      return FhirTypedLiteral.toTag(type);
    }

    @Nonnull
    public static String toTag(FHIRDefinedType type) {
      return "!fhir." + type.toCode();
    }
  }

  public static class FhirConstructor extends Constructor {

    private static final List<FHIRDefinedType> FHIR_TYPES = List.of(
        FHIRDefinedType.DATETIME,
        FHIRDefinedType.DATE,
        FHIRDefinedType.TIME,
        FHIRDefinedType.CODING
    );

    public FhirConstructor() {
      super(new LoaderOptions());
      // Register a generic handler for FHIR types
      for (FHIRDefinedType type : FHIR_TYPES) {
        this.yamlConstructors.put(new Tag(FhirTypedLiteral.toTag(type)),
            new ConstructFhirTypedLiteral(type));
      }
    }

    @AllArgsConstructor
    private static class ConstructFhirTypedLiteral extends AbstractConstruct {

      private final FHIRDefinedType type;

      @Override
      public Object construct(Node node) {
        if (node instanceof ScalarNode sn) {
          return FhirTypedLiteral.of(type, sn.getValue().isEmpty()
                                           ? null
                                           : sn.getValue());
        }
        throw new IllegalArgumentException("Expected a scalar node for " + type);
      }
    }
  }

  public static class FhirRepresenter extends Representer {

    public FhirRepresenter() {
      super(new DumperOptions());
      this.representers.put(FhirTypedLiteral.class, new RepresentFhirTypedLiteral());
    }

    private class RepresentFhirTypedLiteral implements Represent {

      @Override
      public Node representData(Object data) {
        FhirTypedLiteral fhirLiteral = (FhirTypedLiteral) data;
        return representScalar(new Tag(fhirLiteral.getTag()), fhirLiteral.getLiteral());
      }
    }
  }

  static final Yaml YAML = new Yaml(new FhirConstructor(), new FhirRepresenter());
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static Map<FHIRDefinedType, DataType> FHIR_TO_SQL = Map.of(
      FHIRDefinedType.STRING, DataTypes.StringType,
      FHIRDefinedType.INTEGER, DataTypes.IntegerType,
      FHIRDefinedType.BOOLEAN, DataTypes.BooleanType,
      FHIRDefinedType.DECIMAL, DecimalCollection.DECIMAL_TYPE,
      FHIRDefinedType.DATETIME, DataTypes.StringType,
      FHIRDefinedType.DATE, DataTypes.StringType,
      FHIRDefinedType.TIME, DataTypes.StringType,
      FHIRDefinedType.CODING, SparkHelpers.codingStructType(),
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
        .map(clazz -> Map.class.isAssignableFrom(clazz)
                      ? Map.class
                      : clazz)
        .collect(Collectors.toUnmodifiableSet());

    if (types.size() == 1) {
      if (types.contains(Map.class)) {
        final Map<Object, Object> mergedValues = values.stream()
            .filter(Objects::nonNull)
            .map(Map.class::cast)
            .reduce(new HashMap(), (acc, m) -> {
              acc.putAll(m);
              return acc;
            });
        return elementFromValue(key, mergedValues, -1);

      } else {
        return elementFromValue(key, values.get(0), -1);
      }
    } else if (types.size() > 1) {
      // TODO: represnet this as a string for now
      return elementFromValue(key, values.get(0), -1);
      //throw new IllegalArgumentException("Unsupported list with multiple types: " + types);
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
    } else if (value instanceof FhirTypedLiteral typedLiteral) {
      // Use the FHIRDefinedType directly from the typed literal
      return DefPrimitiveDefinition.of(key, typedLiteral.getType(), cardinality);
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
