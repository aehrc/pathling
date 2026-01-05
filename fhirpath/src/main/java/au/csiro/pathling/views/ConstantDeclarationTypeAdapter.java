/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.views;

import au.csiro.pathling.errors.InvalidUserInputError;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.TypeAdapter;
import com.google.gson.internal.Streams;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.r4.model.Base64BinaryType;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.CanonicalType;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.DateType;
import org.hl7.fhir.r4.model.DecimalType;
import org.hl7.fhir.r4.model.IdType;
import org.hl7.fhir.r4.model.InstantType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.OidType;
import org.hl7.fhir.r4.model.PositiveIntType;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.TimeType;
import org.hl7.fhir.r4.model.UnsignedIntType;
import org.hl7.fhir.r4.model.UriType;
import org.hl7.fhir.r4.model.UrlType;
import org.hl7.fhir.r4.model.UuidType;

/**
 * This adapter handles serialisation and deserialisation of {@link ConstantDeclaration} objects.
 * When reading, it looks at the structure and decides which type of value it is, then instantiates
 * the appropriate HAPI class to represent it. When writing, it determines the type suffix from the
 * HAPI class and writes the value in its native JSON type.
 *
 * @author John Grimes
 */
public class ConstantDeclarationTypeAdapter extends TypeAdapter<ConstantDeclaration> {

  private static final String VALUE_PREFIX = "value";

  private static final Map<String, ValueType> typeMap =
      new Builder<String, ValueType>()
          .put("Base64Binary", new ValueType(Base64BinaryType.class, String.class))
          .put("Boolean", new ValueType(BooleanType.class, Boolean.class))
          .put("Canonical", new ValueType(CanonicalType.class, String.class))
          .put("Code", new ValueType(CodeType.class, String.class))
          .put("Date", new ValueType(DateType.class, String.class))
          .put("DateTime", new ValueType(DateTimeType.class, String.class))
          .put("Decimal", new ValueType(DecimalType.class, BigDecimal.class))
          .put("Id", new ValueType(IdType.class, String.class))
          .put("Instant", new ValueType(InstantType.class, String.class))
          .put("Integer", new ValueType(IntegerType.class, int.class))
          .put("Integer64", new ValueType(IntegerType.class, Long.class))
          .put("Oid", new ValueType(OidType.class, String.class))
          .put("String", new ValueType(StringType.class, String.class))
          .put("PositiveInt", new ValueType(PositiveIntType.class, int.class))
          .put("Time", new ValueType(TimeType.class, String.class))
          .put("UnsignedInt", new ValueType(UnsignedIntType.class, int.class))
          .put("Uri", new ValueType(UriType.class, String.class))
          .put("Url", new ValueType(UrlType.class, String.class))
          .put("Uuid", new ValueType(UuidType.class, String.class))
          .build();

  // Reverse mapping from HAPI class to type name for serialisation.
  private static final Map<Class<? extends IBase>, String> classToTypeName =
      typeMap.entrySet().stream()
          .collect(
              java.util.stream.Collectors.toMap(
                  e -> e.getValue().typeClass(),
                  Map.Entry::getKey,
                  // Handle duplicate classes (Integer64 uses IntegerType) - keep first.
                  (a, b) -> a));

  @Override
  public ConstantDeclaration read(final JsonReader in) throws IOException {
    // Parse the input JSON.
    final JsonElement jsonElement = Streams.parse(in);
    final JsonObject jsonObject = jsonElement.getAsJsonObject();

    // Extract the name of the constant from the JSON object.
    final String name;
    if (jsonObject.has("name")) {
      name = jsonObject.get("name").getAsString();
    } else {
      throw new InvalidUserInputError("Constant name is required");
    }

    // Extract the value of the constant from the JSON object.
    final IBase value;
    final List<String> valueKeys =
        jsonObject.keySet().stream().filter(key -> key.startsWith(VALUE_PREFIX)).toList();
    if (valueKeys.size() != 1) {
      throw new InvalidUserInputError("Constant must have one value");
    }

    // Determine the type of the constant value.
    final String valueKey = valueKeys.getFirst();
    final String typeName = valueKey.replace(VALUE_PREFIX, "");
    final ValueType valueType = typeMap.get(typeName);
    if (valueType == null) {
      throw new InvalidUserInputError("Unsupported constant type: " + valueKey);
    }
    // Get the value of the constant based on its type.
    value = getValue(jsonObject, valueKey, valueType);

    // Return a new object containing the extracted name and value.
    return new ConstantDeclaration(name, value);
  }

  @Override
  public void write(final JsonWriter out, final ConstantDeclaration constant) throws IOException {
    out.beginObject();
    out.name("name").value(constant.getName());

    // Determine the type suffix and write the value.
    final IBase value = constant.getValue();
    final String typeSuffix = getTypeSuffix(value);
    out.name(VALUE_PREFIX + typeSuffix);
    writeValue(out, value);

    out.endObject();
  }

  /** Gets the type suffix for a FHIR primitive value (e.g., "String", "Boolean", "Integer"). */
  @Nonnull
  private static String getTypeSuffix(@Nonnull final IBase value) {
    final String typeName = classToTypeName.get(value.getClass());
    if (typeName == null) {
      throw new InvalidUserInputError(
          "Unsupported constant type: " + value.getClass().getSimpleName());
    }
    return typeName;
  }

  /**
   * Writes the primitive value to the JSON output in its native JSON type. Note that
   * PositiveIntType and UnsignedIntType extend IntegerType, so they are handled by the IntegerType
   * case.
   */
  private static void writeValue(@Nonnull final JsonWriter out, @Nonnull final IBase value)
      throws IOException {
    switch (value) {
      case final BooleanType b -> out.value(b.getValue());
      case final IntegerType i -> out.value(i.getValue());
      case final DecimalType d -> out.value(d.getValue());
      case final org.hl7.fhir.r4.model.PrimitiveType<?> p -> out.value(p.getValueAsString());
      default ->
          throw new InvalidUserInputError(
              "Cannot serialise constant: " + value.getClass().getSimpleName());
    }
  }

  @Nonnull
  private static IBase getValue(
      @Nonnull final JsonObject jsonObject,
      @Nonnull final String valueKey,
      @Nonnull final ValueType valueType) {
    final IBase value;
    try {
      // Get the constructor of the value type class.
      final Constructor<? extends IBase> constructor =
          valueType.typeClass().getDeclaredConstructor(valueType.inputClass());
      final Object valueObject;
      // Determine the input class and get the value accordingly.
      if (valueType.inputClass() == Boolean.class) {
        valueObject = jsonObject.get(valueKey).getAsBoolean();
      } else if (valueType.inputClass() == int.class) {
        valueObject = jsonObject.get(valueKey).getAsInt();
      } else if (valueType.inputClass() == Long.class) {
        valueObject = jsonObject.get(valueKey).getAsLong();
      } else if (valueType.inputClass() == BigDecimal.class) {
        valueObject = jsonObject.get(valueKey).getAsBigDecimal();
      } else {
        valueObject = jsonObject.get(valueKey).getAsString();
      }
      // Create a new instance of the value type with the extracted value.
      value = constructor.newInstance(valueObject);
    } catch (final NoSuchMethodException
        | InvocationTargetException
        | InstantiationException
        | IllegalAccessException e) {
      // Throw a runtime exception if the value cannot be instantiated.
      throw new ConstantConstructionException(e);
    }
    // Return the instantiated value.
    return value;
  }

  private record ValueType(
      @Nonnull Class<? extends IBase> typeClass, @Nonnull Class<?> inputClass) {}
}
