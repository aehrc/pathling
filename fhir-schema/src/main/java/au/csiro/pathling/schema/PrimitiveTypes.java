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

package au.csiro.pathling.schema;

import jakarta.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;

/**
 * The storage type of each FHIR primitive type.
 *
 * <p>A type absent from this mapping is treated as complex, and is represented as a structure of
 * its elements rather than as a leaf.
 *
 * <p>Three of these deserve note. A decimal is stored as text, as the Parquet on FHIR specification
 * requires, and the numeric annotation supplies the value where one is needed; the text is that of
 * the number as read, so it is numerically equal to the source rather than textually identical
 * (decision 68). A date or time is stored in the lexical form of the source, because the stated
 * precision is part of the value. And a base64Binary value is stored as the bytes it encodes, which
 * is the Parquet on FHIR mapping of it to binary with no logical type.
 */
public final class PrimitiveTypes {

  @Nonnull
  private static final Map<String, DataType> TYPES =
      Map.ofEntries(
          Map.entry("boolean", DataTypes.BooleanType),
          Map.entry("integer", DataTypes.IntegerType),
          Map.entry("unsignedInt", DataTypes.IntegerType),
          Map.entry("positiveInt", DataTypes.IntegerType),
          Map.entry("integer64", DataTypes.LongType),
          Map.entry("decimal", DataTypes.StringType),
          Map.entry("date", DataTypes.StringType),
          Map.entry("dateTime", DataTypes.StringType),
          Map.entry("instant", DataTypes.StringType),
          Map.entry("time", DataTypes.StringType),
          Map.entry("string", DataTypes.StringType),
          Map.entry("code", DataTypes.StringType),
          Map.entry("uri", DataTypes.StringType),
          Map.entry("url", DataTypes.StringType),
          Map.entry("canonical", DataTypes.StringType),
          Map.entry("oid", DataTypes.StringType),
          Map.entry("uuid", DataTypes.StringType),
          Map.entry("id", DataTypes.StringType),
          Map.entry("markdown", DataTypes.StringType),
          Map.entry("base64Binary", DataTypes.BinaryType),
          Map.entry("xhtml", DataTypes.StringType));

  private PrimitiveTypes() {}

  /**
   * Returns the storage type of a FHIR primitive type.
   *
   * @param type the FHIR type
   * @return the storage type, or empty where the type is not a primitive
   */
  @Nonnull
  public static Optional<DataType> storageTypeOf(@Nonnull final FHIRDefinedType type) {
    return Optional.ofNullable(TYPES.get(type.toCode()));
  }

  /**
   * Returns whether a FHIR type is a primitive, and is therefore stored as a leaf rather than as a
   * structure.
   *
   * @param type the FHIR type
   * @return true where the type is a primitive
   */
  public static boolean isPrimitive(@Nonnull final FHIRDefinedType type) {
    return TYPES.containsKey(type.toCode());
  }
}
