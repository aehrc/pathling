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

package au.csiro.pathling.io.transform;

import jakarta.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * The converter for each FHIR primitive type, keyed by the type's code.
 *
 * <p>Every primitive has an entry of its own and there is no default. Most carry text through
 * unchanged today, but the types they carry diverge as soon as anything is stored beside one of
 * them, and a table with a default branch has to be restructured before that can land (decision
 * 68).
 */
public final class PrimitiveConverters {

  /**
   * The integral types a JSON integer may arrive as. Inference gives a long, and the narrower types
   * are what a dataset built with an explicit schema reasonably carries; every one of them widens
   * to the stored type without loss (decision 70).
   */
  @Nonnull
  private static final DataType[] INTEGRAL = {
    DataTypes.ByteType, DataTypes.ShortType, DataTypes.IntegerType, DataTypes.LongType
  };

  /**
   * A decimal is inferred as a double, or as a long where every value in a file is integral, which
   * is conformant FHIR and is not leniency. Where every value is integral and one is beyond the
   * range of a long, inference gives a decimal of scale zero instead, which is accepted at any
   * precision. The narrower integral types are accepted too, because an integer is an exact decimal
   * (decision 70). A float is not, because it has already rounded most decimal fractions before the
   * transform sees them. It is stored as the text of that number, so it is numerically equal to the
   * source but not necessarily textually identical to it, and returned as a double so that the JSON
   * writer emits a bare number.
   */
  @Nonnull
  private static final PrimitiveConverter DECIMAL =
      PrimitiveConverter.of(
          type ->
              DataTypes.DoubleType.equals(type)
                  || type instanceof DecimalType
                  || Set.of(INTEGRAL).contains(type),
          DataTypes.StringType,
          value -> value.try_cast(DataTypes.StringType),
          value -> value.cast(DataTypes.DoubleType));

  /**
   * A base64Binary value is stored as the bytes it encodes, which is the Parquet on FHIR mapping.
   * Encoding it again breaks the output into lines unless told otherwise, so the line breaks are
   * removed; whitespace the source carried inside the value is not restored.
   */
  @Nonnull
  private static final PrimitiveConverter BASE64_BINARY =
      PrimitiveConverter.of(
          DataTypes.StringType::equals,
          DataTypes.BinaryType,
          value -> functions.try_to_binary(value, functions.lit("base64")),
          value -> functions.regexp_replace(functions.base64(value), "[\\r\\n]", ""));

  @Nonnull
  private static final Map<String, PrimitiveConverter> CONVERTERS =
      Map.ofEntries(
          Map.entry(
              "boolean", PrimitiveConverter.casting(DataTypes.BooleanType, DataTypes.BooleanType)),
          Map.entry("integer", PrimitiveConverter.casting(DataTypes.IntegerType, INTEGRAL)),
          Map.entry("unsignedInt", PrimitiveConverter.casting(DataTypes.IntegerType, INTEGRAL)),
          Map.entry("positiveInt", PrimitiveConverter.casting(DataTypes.IntegerType, INTEGRAL)),
          Map.entry("integer64", PrimitiveConverter.casting(DataTypes.LongType, INTEGRAL)),
          Map.entry("decimal", DECIMAL),
          Map.entry("date", text()),
          Map.entry("dateTime", text()),
          Map.entry("instant", text()),
          Map.entry("time", text()),
          Map.entry("string", text()),
          Map.entry("code", text()),
          Map.entry("uri", text()),
          Map.entry("url", text()),
          Map.entry("canonical", text()),
          Map.entry("oid", text()),
          Map.entry("uuid", text()),
          Map.entry("id", text()),
          Map.entry("markdown", text()),
          Map.entry("base64Binary", BASE64_BINARY),
          Map.entry("xhtml", text()));

  private PrimitiveConverters() {}

  /**
   * Returns the converter for a FHIR type.
   *
   * @param type the FHIR type
   * @return the converter, or empty where the type is not a primitive
   */
  @Nonnull
  public static Optional<PrimitiveConverter> forType(@Nonnull final FHIRDefinedType type) {
    return forCode(type.toCode());
  }

  /**
   * Returns the converter for the code of a FHIR type.
   *
   * @param code the code of the FHIR type
   * @return the converter, or empty where the type is not a primitive
   */
  @Nonnull
  public static Optional<PrimitiveConverter> forCode(@Nonnull final String code) {
    return Optional.ofNullable(CONVERTERS.get(code));
  }

  /** Returns a converter for a type whose values are text in JSON and in storage alike. */
  @Nonnull
  private static PrimitiveConverter text() {
    return PrimitiveConverter.casting(DataTypes.StringType, DataTypes.StringType);
  }
}
