/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import au.csiro.pathling.fhirpath.definition.defaults.DefaultChoiceDefinition;
import au.csiro.pathling.fhirpath.definition.defaults.DefaultCompositeDefinition;
import au.csiro.pathling.fhirpath.definition.defaults.DefaultPrimitiveDefinition;
import au.csiro.pathling.fhirpath.definition.defaults.DefaultResourceDefinition;
import au.csiro.pathling.fhirpath.definition.defaults.DefaultResourceTag;
import au.csiro.pathling.fhirpath.encoding.CodingSchema;
import au.csiro.pathling.fhirpath.literal.CodingLiteral;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
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

  private static boolean isAnnotation(@Nonnull final Object key) {
    return key.toString().startsWith("__");
  }

  public class FhirTypedLiteralSerializer extends JsonSerializer<FhirTypedLiteral> {

    @Override
    public void serialize(
        final FhirTypedLiteral fhirLiteral,
        final JsonGenerator gen,
        final SerializerProvider serializers)
        throws IOException {
      if (nonNull(fhirLiteral.getLiteral())) {
        @Nonnull final String literalValue = requireNonNull(fhirLiteral.getLiteral());
        switch (fhirLiteral.getType()) {
          case NULL -> gen.writeNull();
          case INTEGER, DECIMAL, BOOLEAN -> gen.writeRawValue(literalValue);
          case TIME, DATE, DATETIME -> gen.writeString(literalValue);
          case STRING -> gen.writeString(StringCollection.parseStringLiteral(literalValue));
          case CODING -> writeCoding(literalValue, gen);
          default ->
              throw new IllegalArgumentException("Unsupported FHIR type: " + fhirLiteral.getType());
        }
      } else {
        gen.writeNull();
      }
    }

    private void writeCoding(
        @Nonnull final CharSequence codingLiteral, @Nonnull final JsonGenerator gen)
        throws IOException {
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
      for (final FHIRDefinedType type : FHIR_TO_SQL.keySet()) {
        this.yamlConstructors.put(
            new Tag(FhirTypedLiteral.toTag(type)), new ConstructFhirTypedLiteral(type));
      }
    }

    @AllArgsConstructor
    private static class ConstructFhirTypedLiteral extends AbstractConstruct {

      private final FHIRDefinedType type;

      @Override
      public Object construct(final Node node) {
        if (node instanceof final ScalarNode sn) {
          return FhirTypedLiteral.of(type, sn.getValue().isEmpty() ? null : sn.getValue());
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
      public Node representData(final Object data) {
        final FhirTypedLiteral fhirLiteral = (FhirTypedLiteral) data;
        return representScalar(new Tag(fhirLiteral.getTag()), fhirLiteral.getLiteral());
      }
    }
  }

  private static final Map<FHIRDefinedType, DataType> FHIR_TO_SQL =
      Map.ofEntries(
          Map.entry(FHIRDefinedType.STRING, DataTypes.StringType),
          Map.entry(FHIRDefinedType.URI, DataTypes.StringType),
          Map.entry(FHIRDefinedType.CODE, DataTypes.StringType),
          Map.entry(FHIRDefinedType.INTEGER, DataTypes.IntegerType),
          Map.entry(FHIRDefinedType.BOOLEAN, DataTypes.BooleanType),
          Map.entry(FHIRDefinedType.DECIMAL, DecimalCollection.DECIMAL_TYPE),
          Map.entry(FHIRDefinedType.CODING, CodingSchema.codingStructType()),
          Map.entry(FHIRDefinedType.TIME, DataTypes.StringType),
          Map.entry(FHIRDefinedType.DATETIME, DataTypes.StringType),
          Map.entry(FHIRDefinedType.DATE, DataTypes.StringType),
          Map.entry(FHIRDefinedType.NULL, DataTypes.NullType));

  public static final Yaml YAML = new Yaml(new FhirConstructor(), new FhirRepresenter());
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Creates a struct Column from a Map.
   *
   * @param map The map to convert
   * @return A struct Column
   */
  @Nonnull
  private static Column createStructFromMap(@Nonnull final Map<Object, Object> map) {
    final List<Column> fields = new ArrayList<>();

    for (final Map.Entry<Object, Object> entry : map.entrySet()) {
      final String key = entry.getKey().toString();
      final Object value = entry.getValue();

      final Column fieldColumn = valueToColumn(value);
      if (fieldColumn != null) {
        fields.add(fieldColumn.alias(key));
      }
    }

    return org.apache.spark.sql.functions.struct(fields.toArray(new Column[0]));
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
    } else if (value instanceof final FhirTypedLiteral typedLiteral) {
      return typedLiteralToColumn(typedLiteral);
    } else if (value instanceof final String stringValue) {
      return StringCollection.fromValue(stringValue).getColumnValue();
    } else if (value instanceof final Integer intValue) {
      return IntegerCollection.fromValue(intValue).getColumnValue();
    } else if (value instanceof final Boolean boolValue) {
      return BooleanCollection.fromValue(boolValue).getColumnValue();
    } else if (value instanceof final Double doubleValue) {
      try {
        return DecimalCollection.fromValue(new DecimalType(doubleValue)).getColumnValue();
      } catch (final Exception e) {
        throw new IllegalArgumentException("Failed to convert decimal value: " + doubleValue, e);
      }
    } else if (value instanceof final Map<?, ?> mapValue) {
      @SuppressWarnings("unchecked")
      final Map<Object, Object> objectMap = (Map<Object, Object>) mapValue;
      return createStructFromMap(objectMap);
    } else if (value instanceof final List<?> listValue) {
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
        default ->
            throw new IllegalArgumentException("Unsupported FHIR type: " + typedLiteral.getType());
      };
    } catch (final Exception e) {
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
  private static Column listToColumn(@Nonnull final Collection<?> list) {
    if (list.isEmpty()) {
      return null;
    }

    // Get the first non-null element to determine the type
    final Object firstNonNull = list.stream().filter(Objects::nonNull).findFirst().orElse(null);

    if (firstNonNull == null) {
      return null;
    }

    final List<Column> columns = new ArrayList<>();
    for (final Object item : list) {
      final Column itemColumn = valueToColumn(item);
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
  public static ResourceDefinition yamlToDefinition(
      @Nonnull final String resourceCode, @Nonnull final Map<Object, Object> data) {
    final List<ChildDefinition> definedFields = elementsFromYaml(data);
    final Set<String> definedFieldNames =
        definedFields.stream()
            .map(ChildDefinition::getName)
            .collect(Collectors.toUnmodifiableSet());

    return DefaultResourceDefinition.of(
        DefaultResourceTag.of(resourceCode),
        Stream.concat(
                Stream.of(
                        DefaultPrimitiveDefinition.single("id", FHIRDefinedType.STRING),
                        DefaultPrimitiveDefinition.single("id_versioned", FHIRDefinedType.STRING))
                    .filter(field -> !definedFieldNames.contains(field.getName())),
                definedFields.stream())
            .toList());
  }

  @Nonnull
  static List<ChildDefinition> elementsFromYaml(@Nonnull final Map<Object, Object> data) {

    final List<ChildDefinition> children =
        data.entrySet().stream()
            .filter(entry -> !isAnnotation(entry.getKey()))
            .map(entry -> elementFromYaml(entry.getKey().toString(), entry.getValue()))
            .toList();
    return Optional.ofNullable(data.get(CHOICE_ANNOTATION))
        .map(
            name -> List.<ChildDefinition>of(DefaultChoiceDefinition.of(name.toString(), children)))
        .orElse(children);
  }

  public static ChildDefinition elementFromYaml(final String key, final Object value) {
    if (value instanceof final List<?> list) {
      return elementFromValues(key, list);
    } else {
      return elementFromValue(key, value, 1);
    }
  }

  @Nonnull
  @SuppressWarnings("unchecked")
  static ChildDefinition elementFromValues(
      @Nonnull final String key, @Nonnull final List<?> values) {

    final List<?> nonNullValues = values.stream().filter(Objects::nonNull).toList();

    // Get the set of unique types from the list of values.
    final Set<Class<?>> types =
        nonNullValues.stream()
            .map(Object::getClass)
            .map(
                clazz ->
                    Map.class.isAssignableFrom(clazz)
                        // We treat all maps as the same type.
                        ? Map.class
                        : clazz)
            .collect(Collectors.toUnmodifiableSet());

    // If there is only one type, we can make some assumptions about the data.
    if (types.size() == 1) {
      // If the type is a map, we merge all the maps into one. This is useful for when a
      // choice contains a complex type, and the examples are split across multiple lines.
      if (types.contains(Map.class)) {
        final Map<?, ?> mergedValues =
            nonNullValues.stream()
                .map(Map.class::cast)
                .reduce(
                    new HashMap<>(),
                    (acc, m) -> {
                      acc.putAll(m);
                      return acc;
                    });
        return elementFromValue(key, mergedValues, -1);

      } else {
        // If there is only one type, and it's not a map, we just use the first value as the
        // representative.
        return elementFromValue(key, nonNullValues.get(0), -1);
      }
    } else if (types.size() > 1) {
      // If there are multiple types, we just use the first value as the representative.
      return elementFromValue(key, nonNullValues.get(0), -1);
    } else {
      // If there are no types, it means the list is empty or contains only nulls.
      return elementFromValue(key, null, -1);
    }
  }

  @Nonnull
  @SuppressWarnings("unchecked")
  private static ChildDefinition elementFromValue(
      @Nonnull final String key, @Nullable final Object value, final int cardinality) {
    if (isNull(value)) {
      return DefaultPrimitiveDefinition.of(key, FHIRDefinedType.NULL, cardinality);
    } else if (value instanceof final FhirTypedLiteral typedLiteral) {
      // Use the FHIRDefinedType directly from the typed literal
      if (typedLiteral.getType() == FHIRDefinedType.CODING) {
        return CodingCollection.createDefinition(key, cardinality);
      } else {
        return DefaultPrimitiveDefinition.of(key, typedLiteral.getType(), cardinality);
      }
    } else if (value instanceof String) {
      return DefaultPrimitiveDefinition.of(key, FHIRDefinedType.STRING, cardinality);
    } else if (value instanceof Integer) {
      return DefaultPrimitiveDefinition.of(key, FHIRDefinedType.INTEGER, cardinality);
    } else if (value instanceof Boolean) {
      return DefaultPrimitiveDefinition.of(key, FHIRDefinedType.BOOLEAN, cardinality);
    } else if (value instanceof Double) {
      return DefaultPrimitiveDefinition.of(key, FHIRDefinedType.DECIMAL, cardinality);
    } else if (value instanceof final Map<?, ?> map) {
      return Optional.ofNullable(map.get(FHIR_TYPE_ANNOTATION))
          .map(Object::toString)
          .map(FHIRDefinedType::fromCode)
          .map(
              fhirType ->
                  DefaultCompositeDefinition.of(
                      key, elementsFromYaml((Map<Object, Object>) map), cardinality, fhirType))
          .orElseGet(
              () ->
                  DefaultCompositeDefinition.backbone(
                      key, elementsFromYaml((Map<Object, Object>) map), cardinality));
    } else {
      throw new IllegalArgumentException(
          "Unsupported data type: " + value + " (" + value.getClass().getName() + ")");
    }
  }

  @Nonnull
  public static StructType definitionToStruct(
      @Nonnull final DefaultResourceDefinition resourceDefinition) {
    return childrenToStruct(resourceDefinition.getChildren());
  }

  @Nonnull
  public static StructType childrenToStruct(
      @Nonnull final Collection<ChildDefinition> childDefinitions) {
    return new StructType(
        childDefinitions.stream()
            .flatMap(YamlSupport::elementToStructField)
            .toArray(StructField[]::new));
  }

  private static Stream<StructField> elementToStructField(final ChildDefinition childDefinition) {
    if (childDefinition instanceof final DefaultPrimitiveDefinition primitiveDefinition) {
      final DataType elementType =
          requireNonNull(
              FHIR_TO_SQL.get(primitiveDefinition.getType()),
              "No SQL type for " + primitiveDefinition.getFhirType());
      return Stream.of(
          new StructField(
              primitiveDefinition.getName(),
              primitiveDefinition.getCardinality() < 0
                  ? new ArrayType(elementType, true)
                  : elementType,
              true,
              Metadata.empty()));
    } else if (childDefinition instanceof final DefaultCompositeDefinition compositeDefinition) {

      final StructType predefinedType = (StructType) FHIR_TO_SQL.get(compositeDefinition.getType());
      final StructType elementType =
          predefinedType != null
              ? predefinedType
              : childrenToStruct(compositeDefinition.getChildren());
      return Stream.of(
          new StructField(
              compositeDefinition.getName(),
              compositeDefinition.getCardinality() < 0
                  ? new ArrayType(elementType, true)
                  : elementType,
              true,
              Metadata.empty()));
    } else if (childDefinition instanceof final DefaultChoiceDefinition choiceDefinition) {
      final StructType elementType =
          childrenToStruct(
              choiceDefinition.getChoices().stream()
                  .filter(c -> !c.getName().startsWith("_"))
                  .toList());
      return Stream.of(elementType.fields());
    } else {
      throw new IllegalArgumentException("Unsupported child definition: " + childDefinition);
    }
  }

  @Nonnull
  public static String omToJson(@Nonnull final Map<Object, Object> objectModel) {
    try {
      return OBJECT_MAPPER.writeValueAsString(objectModel);
    } catch (final JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
