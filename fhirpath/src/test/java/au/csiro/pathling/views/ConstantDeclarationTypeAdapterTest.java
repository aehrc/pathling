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

package au.csiro.pathling.views;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.gson.Gson;
import java.util.stream.Stream;
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
import org.hl7.fhir.r4.model.PrimitiveType;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.TimeType;
import org.hl7.fhir.r4.model.UnsignedIntType;
import org.hl7.fhir.r4.model.UriType;
import org.hl7.fhir.r4.model.UrlType;
import org.hl7.fhir.r4.model.UuidType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for {@link ConstantDeclarationTypeAdapter}, verifying round-trip serialisation and
 * deserialisation of all supported FHIR primitive types.
 *
 * @author John Grimes
 */
class ConstantDeclarationTypeAdapterTest {

  Gson gson;

  @BeforeEach
  void setUp() {
    gson = ViewDefinitionGson.create();
  }

  /**
   * Tests that each supported constant type can be serialised to JSON and deserialised back to an
   * equivalent object.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("constantTypes")
  void roundTripSerialisation(
      final String description, final String name, final IBase value, final String expectedJson) {
    final ConstantDeclaration original = new ConstantDeclaration(name, value);

    // Serialise to JSON.
    final String json = gson.toJson(original);
    assertEquals(expectedJson, json);

    // Deserialise back to object.
    final ConstantDeclaration restored = gson.fromJson(json, ConstantDeclaration.class);
    assertEquals(original.getName(), restored.getName());

    // Compare values using HAPI's equalsDeep for proper comparison.
    final PrimitiveType<?> originalValue = (PrimitiveType<?>) original.getValue();
    final PrimitiveType<?> restoredValue = (PrimitiveType<?>) restored.getValue();
    assertEquals(
        originalValue.getValueAsString(),
        restoredValue.getValueAsString(),
        "Value should match after round-trip");
  }

  static Stream<Arguments> constantTypes() {
    return Stream.of(
        // Boolean type.
        Arguments.of(
            "Boolean (true)",
            "boolTrue",
            new BooleanType(true),
            "{\"name\":\"boolTrue\",\"valueBoolean\":true}"),
        Arguments.of(
            "Boolean (false)",
            "boolFalse",
            new BooleanType(false),
            "{\"name\":\"boolFalse\",\"valueBoolean\":false}"),

        // String types.
        Arguments.of(
            "String",
            "strConst",
            new StringType("hello world"),
            "{\"name\":\"strConst\",\"valueString\":\"hello world\"}"),
        Arguments.of(
            "Code",
            "codeConst",
            new CodeType("active"),
            "{\"name\":\"codeConst\",\"valueCode\":\"active\"}"),
        Arguments.of(
            "Id",
            "idConst",
            new IdType("patient-123"),
            "{\"name\":\"idConst\",\"valueId\":\"patient-123\"}"),
        Arguments.of(
            "Uri",
            "uriConst",
            new UriType("http://example.org"),
            "{\"name\":\"uriConst\",\"valueUri\":\"http://example.org\"}"),
        Arguments.of(
            "Url",
            "urlConst",
            new UrlType("https://example.org/page"),
            "{\"name\":\"urlConst\",\"valueUrl\":\"https://example.org/page\"}"),
        Arguments.of(
            "Canonical",
            "canonicalConst",
            new CanonicalType("http://hl7.org/fhir/StructureDefinition/Patient"),
            "{\"name\":\"canonicalConst\",\"valueCanonical\":\"http://hl7.org/fhir/StructureDefinition/Patient\"}"),
        Arguments.of(
            "Uuid",
            "uuidConst",
            new UuidType("urn:uuid:c757873d-ec9a-4326-a141-556f43239520"),
            "{\"name\":\"uuidConst\",\"valueUuid\":\"urn:uuid:c757873d-ec9a-4326-a141-556f43239520\"}"),
        Arguments.of(
            "Oid",
            "oidConst",
            new OidType("urn:oid:1.2.3.4.5"),
            "{\"name\":\"oidConst\",\"valueOid\":\"urn:oid:1.2.3.4.5\"}"),
        Arguments.of(
            "Base64Binary",
            "base64Const",
            new Base64BinaryType("SGVsbG8gV29ybGQ="),
            "{\"name\":\"base64Const\",\"valueBase64Binary\":\"SGVsbG8gV29ybGQ=\"}"),

        // Numeric types.
        Arguments.of(
            "Integer",
            "intConst",
            new IntegerType(42),
            "{\"name\":\"intConst\",\"valueInteger\":42}"),
        Arguments.of(
            "Integer (negative)",
            "negIntConst",
            new IntegerType(-100),
            "{\"name\":\"negIntConst\",\"valueInteger\":-100}"),
        Arguments.of(
            "PositiveInt",
            "posIntConst",
            new PositiveIntType(1),
            "{\"name\":\"posIntConst\",\"valuePositiveInt\":1}"),
        Arguments.of(
            "UnsignedInt",
            "unsignedConst",
            new UnsignedIntType(0),
            "{\"name\":\"unsignedConst\",\"valueUnsignedInt\":0}"),
        Arguments.of(
            "Decimal",
            "decConst",
            new DecimalType("3.14159"),
            "{\"name\":\"decConst\",\"valueDecimal\":3.14159}"),

        // Temporal types.
        Arguments.of(
            "Date",
            "dateConst",
            new DateType("2024-01-15"),
            "{\"name\":\"dateConst\",\"valueDate\":\"2024-01-15\"}"),
        Arguments.of(
            "DateTime",
            "dateTimeConst",
            new DateTimeType("2024-01-15T10:30:00Z"),
            "{\"name\":\"dateTimeConst\",\"valueDateTime\":\"2024-01-15T10:30:00Z\"}"),
        Arguments.of(
            "Time",
            "timeConst",
            new TimeType("14:30:00"),
            "{\"name\":\"timeConst\",\"valueTime\":\"14:30:00\"}"),
        Arguments.of(
            "Instant",
            "instantConst",
            new InstantType("2024-01-15T10:30:00.000Z"),
            "{\"name\":\"instantConst\",\"valueInstant\":\"2024-01-15T10:30:00.000Z\"}"));
  }
}
