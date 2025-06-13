package au.csiro.pathling.test.yaml;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.CodingCollection;
import au.csiro.pathling.fhirpath.collection.DecimalCollection;
import au.csiro.pathling.fhirpath.collection.IntegerCollection;
import au.csiro.pathling.fhirpath.collection.StringCollection;
import au.csiro.pathling.fhirpath.definition.ChildDefinition;
import au.csiro.pathling.fhirpath.definition.ResourceDefinition;
import au.csiro.pathling.fhirpath.definition.def.DefChoiceDefinition;
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
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.experimental.UtilityClass;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.DecimalType;
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

  public static final String FHIR_TYPE_ANNOTATION = "__FHIR_TYPE__";
  public static final String CHOICE_ANNOTATION = "__CHOICE__";

  static boolean isAnnotation(@Nonnull final Object key) {
    return key.toString().startsWith("__");
  }

  public class FhirTypedLiteralSerializer extends JsonSerializer<FhirTypedLiteral> {

    @Override
    public void serialize(FhirTypedLiteral fhirLiteral, JsonGenerator gen,
        SerializerProvider serializers) throws IOException {
      if (nonNull(fhirLiteral.getLiteral())) {
        @Nonnull final String literalValue = requireNonNull(fhirLiteral.getLiteral());
        switch (fhirLiteral.getType()) {
          case NULL -> gen.writeNull();
          case INTEGER, DECIMAL, BOOLEAN -> gen.writeRawValue(literalValue);
          case STRING -> gen.writeString(StringCollection.parseStringLiteral(literalValue));
          case CODING -> writeCoding(literalValue, gen);
          default ->
              throw new IllegalArgumentException("Unsupported FHIR type: " + fhirLiteral.getType());
        }
      } else {
        gen.writeNull();
      }
    }

    private void writeCoding(@Nonnull final String codingLiteral,
        @Nonnull final JsonGenerator gen) throws IOException {
      final Coding coding = CodingLiteral.fromString(codingLiteral);
      // write as object to gen using low level api
      gen.writeStartObject();
      gen.writeStringField("system", coding.getSystem());
      gen.writeStringField("version", coding.getVersion());
      gen.writeStringField("code", coding.getCode());
      gen.writeStringField("display", coding.getDisplay());
      gen.writeEndObject();
    }
  }

  public static class FhirConstructor extends Constructor {

    public FhirConstructor() {
      super(new LoaderOptions());
      // Register a generic handler for FHIR types
      for (FHIRDefinedType type : FHIR_TO_SQL.keySet()) {
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

  private static final Map<FHIRDefinedType, DataType> FHIR_TO_SQL = Map.of(
      FHIRDefinedType.STRING, DataTypes.StringType,
      FHIRDefinedType.URI, DataTypes.StringType,
      FHIRDefinedType.CODE, DataTypes.StringType,
      FHIRDefinedType.INTEGER, DataTypes.IntegerType,
      FHIRDefinedType.BOOLEAN, DataTypes.BooleanType,
      FHIRDefinedType.DECIMAL, DecimalCollection.DECIMAL_TYPE,
      FHIRDefinedType.CODING, SparkHelpers.codingStructType(),
      FHIRDefinedType.NULL, DataTypes.NullType
  );
  static final Yaml YAML = new Yaml(new FhirConstructor(), new FhirRepresenter());
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();


  /**
   * Creates a struct Column from a Map.
   *
   * @param map The map to convert
   * @return A struct Column
   */
  @Nonnull
  private static Column createStructFromMap(@Nonnull final Map<Object, Object> map) {
    List<Column> fields = new ArrayList<>();

    for (Map.Entry<Object, Object> entry : map.entrySet()) {
      String key = entry.getKey().toString();
      Object value = entry.getValue();

      Column fieldColumn = valueToColumn(value);
      if (fieldColumn != null) {
        fields.add(fieldColumn.alias(key));
      }
    }

    return org.apache.spark.sql.functions.struct(
        fields.toArray(new Column[0])
    );
  }

  /**
   * Converts a value to a Spark Column.
   *
   * @param value The value to convert
   * @return A Spark Column representing the value
   */
  @Nullable
  public static Column valueToColumn(@Nullable final Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof FhirTypedLiteral typedLiteral) {
      return typedLiteralToColumn(typedLiteral);
    } else if (value instanceof String stringValue) {
      return StringCollection.fromValue(stringValue).getColumnValue();
    } else if (value instanceof Integer intValue) {
      return IntegerCollection.fromValue(intValue).getColumnValue();
    } else if (value instanceof Boolean boolValue) {
      return BooleanCollection.fromValue(boolValue).getColumnValue();
    } else if (value instanceof Double doubleValue) {
      try {
        return DecimalCollection.fromValue(new DecimalType(doubleValue)).getColumnValue();
      } catch (Exception e) {
        throw new IllegalArgumentException("Failed to convert decimal value: " + doubleValue, e);
      }
    } else if (value instanceof Map<?, ?> mapValue) {
      @SuppressWarnings("unchecked")
      Map<Object, Object> objectMap = (Map<Object, Object>) mapValue;
      return createStructFromMap(objectMap);
    } else if (value instanceof List<?> listValue) {
      return listToColumn(listValue);
    } else {
      throw new IllegalArgumentException("Unsupported data type: " + value.getClass().getName());
    }
  }

  /**
   * Converts a typed literal to a Spark Column.
   *
   * @param typedLiteral The typed literal to convert
   * @return A Spark Column representing the typed literal
   */
  @Nonnull
  private static Column typedLiteralToColumn(@Nonnull final FhirTypedLiteral typedLiteral) {
    if (typedLiteral.getLiteral() == null) {
      return org.apache.spark.sql.functions.lit(null);
    }

    try {
      return switch (typedLiteral.getType()) {
        case CODING -> CodingCollection.fromLiteral(typedLiteral.getLiteral()).getColumnValue();
        default -> throw new IllegalArgumentException(
            "Unsupported FHIR type: " + typedLiteral.getType());
      };
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to convert typed literal: " + typedLiteral, e);
    }
  }

  /**
   * Converts a list to a Spark Column.
   *
   * @param list The list to convert
   * @return A Spark Column representing the list
   */
  @Nullable
  private static Column listToColumn(@Nonnull final List<?> list) {
    if (list.isEmpty()) {
      return null;
    }

    // Get the first non-null element to determine the type
    Object firstNonNull = list.stream()
        .filter(Objects::nonNull)
        .findFirst()
        .orElse(null);

    if (firstNonNull == null) {
      return null;
    }

    List<Column> columns = new ArrayList<>();
    for (Object item : list) {
      Column itemColumn = valueToColumn(item);
      if (itemColumn != null) {
        columns.add(itemColumn);
      }
    }

    if (columns.isEmpty()) {
      return null;
    }

    return org.apache.spark.sql.functions.array(columns.toArray(new Column[0]));
  }

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

    final List<ChildDefinition> children = data.entrySet().stream()
        .filter(entry -> !isAnnotation(entry.getKey()))
        .map(entry -> elementFromYaml(entry.getKey().toString(), entry.getValue()))
        .toList();
    return Optional.ofNullable(data.get(CHOICE_ANNOTATION))
        .map(name -> List.<ChildDefinition>of(DefChoiceDefinition.of(name.toString(), children)))
        .orElse(children);
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
            .reduce(new HashMap<>(), (acc, m) -> {
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
      if (typedLiteral.getType() == FHIRDefinedType.CODING) {
        return CodingCollection.createDefinition(key, cardinality);
      } else {
        return DefPrimitiveDefinition.of(key, typedLiteral.getType(), cardinality);
      }
    } else if (value instanceof String) {
      return DefPrimitiveDefinition.of(key, FHIRDefinedType.STRING, cardinality);
    } else if (value instanceof Integer) {
      return DefPrimitiveDefinition.of(key, FHIRDefinedType.INTEGER, cardinality);
    } else if (value instanceof Boolean) {
      return DefPrimitiveDefinition.of(key, FHIRDefinedType.BOOLEAN, cardinality);
    } else if (value instanceof Double) {
      return DefPrimitiveDefinition.of(key, FHIRDefinedType.DECIMAL, cardinality);
    } else if (value instanceof Map<?, ?> map) {
      return Optional.ofNullable(map.get(FHIR_TYPE_ANNOTATION))
          .map(Object::toString)
          .map(FHIRDefinedType::fromCode)
          .map(fhirType -> DefCompositeDefinition.of(key,
              elementsFromYaml((Map<Object, Object>) map),
              cardinality, fhirType))
          .orElseGet(() -> DefCompositeDefinition.backbone(key,
              elementsFromYaml((Map<Object, Object>) map),
              cardinality));
    } else {
      throw new IllegalArgumentException(
          "Unsupported data type: " + value + " (" + value.getClass()
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
            .flatMap(YamlSupport::elementToStructField)
            .toArray(StructField[]::new)
    );
  }

  private static Stream<StructField> elementToStructField(ChildDefinition childDefinition) {
    if (childDefinition instanceof DefPrimitiveDefinition primitiveDefinition) {
      final DataType elementType = requireNonNull(
          FHIR_TO_SQL.get(primitiveDefinition.getType()),
          "No SQL type for " + primitiveDefinition.getFhirType());
      return Stream.of(new StructField(
          primitiveDefinition.getName(),
          primitiveDefinition.getCardinality() < 0
          ? new ArrayType(elementType, true)
          : elementType,
          true, Metadata.empty()
      ));
    } else if (childDefinition instanceof DefCompositeDefinition compositeDefinition) {

      StructType predefinedType = (StructType) FHIR_TO_SQL.get(compositeDefinition.getType());
      final StructType elementType = predefinedType != null
                                     ? predefinedType
                                     : childrendToStruct(compositeDefinition.getChildren());
      return Stream.of(new StructField(
          compositeDefinition.getName(),
          compositeDefinition.getCardinality() < 0
          ? new ArrayType(elementType, true)
          : elementType,
          true, Metadata.empty()
      ));
    } else if (childDefinition instanceof DefChoiceDefinition choiceDefinition) {
      final StructType elementType = childrendToStruct(
          choiceDefinition.getChoices().stream().filter(c -> !c.getName().startsWith("_"))
              .collect(Collectors.toList()));
      return Stream.of(elementType.fields());
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
}
