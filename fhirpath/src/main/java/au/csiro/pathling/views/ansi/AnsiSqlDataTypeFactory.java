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

package au.csiro.pathling.views.ansi;

import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

/**
 * Factory class for creating Spark SQL DataTypes from ANSI SQL type definitions.
 */
@SuppressWarnings("unused")
@Slf4j
public class AnsiSqlDataTypeFactory {

  /**
   * Create a character type.
   *
   * @return the Spark SQL DataType for character types
   */
  @Nonnull
  public DataType createCharacter() {
    return DataTypes.StringType;
  }

  /**
   * Create a character type with specified length.
   *
   * @param length the length of the character type
   * @return the Spark SQL DataType for character types
   */
  @Nonnull
  public DataType createCharacter(final int length) {
    // Spark doesn't differentiate character lengths, always returns StringType
    log.warn(
        "CHAR({}) cannot be precisely represented in Spark SQL, using StringType without length constraint",
        length);
    return DataTypes.StringType;
  }

  /**
   * Create a varchar type.
   *
   * @return the Spark SQL DataType for varchar types
   */
  @Nonnull
  public DataType createVarchar() {
    return DataTypes.StringType;
  }

  /**
   * Create a varchar type with specified length.
   *
   * @param length the maximum length of the varchar type
   * @return the Spark SQL DataType for varchar types
   */
  @Nonnull
  public DataType createVarchar(final int length) {
    // Spark doesn't differentiate varchar lengths, always returns StringType
    log.warn(
        "VARCHAR({}) cannot be precisely represented in Spark SQL, using StringType without length constraint",
        length);
    return DataTypes.StringType;
  }

  /**
   * Create a smallint type.
   *
   * @return the Spark SQL DataType for smallint
   */
  @Nonnull
  public DataType createSmallInt() {
    return DataTypes.ShortType;
  }

  /**
   * Create an integer type.
   *
   * @return the Spark SQL DataType for integer
   */
  @Nonnull
  public DataType createInteger() {
    return DataTypes.IntegerType;
  }

  /**
   * Create a bigint type.
   *
   * @return the Spark SQL DataType for bigint
   */
  @Nonnull
  public DataType createBigInt() {
    return DataTypes.LongType;
  }

  /**
   * Create a real type.
   *
   * @return the Spark SQL DataType for real
   */
  @Nonnull
  public DataType createReal() {
    return DataTypes.FloatType;
  }

  /**
   * Create a double precision type.
   *
   * @return the Spark SQL DataType for double precision
   */
  @Nonnull
  public DataType createDouble() {
    return DataTypes.DoubleType;
  }

  /**
   * Create a float type with no precision specified.
   *
   * @return the Spark SQL DataType for float
   */
  @Nonnull
  public DataType createFloat() {
    // Default FLOAT with no precision to DOUBLE
    return DataTypes.DoubleType;
  }

  /**
   * Create a float type with specified precision.
   *
   * @param precision the precision value
   * @return the Spark SQL DataType for float
   */
  @Nonnull
  public DataType createFloat(final int precision) {
    // ANSI SQL standard: FLOAT(p) maps to REAL if p <= 24, otherwise DOUBLE PRECISION
    return precision <= 24
           ? DataTypes.FloatType
           : DataTypes.DoubleType;
  }

  /**
   * Create a numeric/decimal type with default precision and scale.
   *
   * @return the Spark SQL DataType for numeric/decimal
   */
  @Nonnull
  public DataType createDecimal() {
    return DataTypes.createDecimalType();
  }

  /**
   * Create a numeric/decimal type with specified precision.
   *
   * @param precision the precision value
   * @return the Spark SQL DataType for numeric/decimal
   */
  @Nonnull
  public DataType createDecimal(final int precision) {
    return DataTypes.createDecimalType(precision, 0);
  }

  /**
   * Create a numeric/decimal type with specified precision and scale.
   *
   * @param precision the precision value
   * @param scale the scale value
   * @return the Spark SQL DataType for numeric/decimal
   */
  @Nonnull
  public DataType createDecimal(final int precision, final int scale) {
    return DataTypes.createDecimalType(precision, scale);
  }

  /**
   * Create a boolean type.
   *
   * @return the Spark SQL DataType for boolean
   */
  @Nonnull
  public DataType createBoolean() {
    return DataTypes.BooleanType;
  }

  /**
   * Create a binary type.
   *
   * @return the Spark SQL DataType for binary
   */
  @Nonnull
  public DataType createBinary() {
    return DataTypes.BinaryType;
  }

  /**
   * Create a binary type with specified length.
   *
   * @param length the length of the binary type
   * @return the Spark SQL DataType for binary
   */
  @Nonnull
  public DataType createBinary(final int length) {
    // Spark doesn't differentiate binary lengths, always returns BinaryType
    log.warn(
        "BINARY({}) cannot be precisely represented in Spark SQL, using BinaryType without length constraint",
        length);
    return DataTypes.BinaryType;
  }

  /**
   * Create a varbinary type.
   *
   * @return the Spark SQL DataType for varbinary
   */
  @Nonnull
  public DataType createVarbinary() {
    return DataTypes.BinaryType;
  }

  /**
   * Create a varbinary type with specified length.
   *
   * @param length the maximum length of the varbinary type
   * @return the Spark SQL DataType for varbinary
   */
  @Nonnull
  public DataType createVarbinary(final int length) {
    // Spark doesn't differentiate varbinary lengths, always returns BinaryType
    log.warn(
        "VARBINARY({}) cannot be precisely represented in Spark SQL, using BinaryType without length constraint",
        length);
    return DataTypes.BinaryType;
  }

  /**
   * Create a date type.
   *
   * @return the Spark SQL DataType for date
   */
  @Nonnull
  public DataType createDate() {
    return DataTypes.DateType;
  }

  /**
   * Create a timestamp type.
   *
   * @return the Spark SQL DataType for timestamp
   */
  @Nonnull
  public DataType createTimestamp() {
    return DataTypes.TimestampNTZType;
  }

  /**
   * Create a timestamp type with specified precision.
   *
   * @param precision the precision value
   * @return the Spark SQL DataType for timestamp
   */
  @Nonnull
  public DataType createTimestamp(final int precision) {
    // Spark doesn't support timestamp precision, always returns TimestampType
    log.warn(
        "TIMESTAMP({}) precision cannot be precisely represented in Spark SQL, using TimestampNTZType",
        precision);
    return DataTypes.TimestampNTZType;
  }

  /**
   * Create a timestamp type with time zone.
   *
   * @return the Spark SQL DataType for timestamp with time zone
   */
  @Nonnull
  public DataType createTimestampWithTimeZone() {
    // Spark doesn't have a direct timestamp with time zone type
    log.warn(
        "TIMESTAMP WITH TIME ZONE cannot be precisely represented in Spark SQL, using TimestampType");
    return DataTypes.TimestampType;
  }

  /**
   * Create a timestamp type with time zone and specified precision.
   *
   * @param precision the precision value
   * @return the Spark SQL DataType for timestamp with time zone
   */
  @Nonnull
  public DataType createTimestampWithTimeZone(final int precision) {
    // Spark doesn't have a direct timestamp with time zone type
    log.warn(
        "TIMESTAMP({}) WITH TIME ZONE cannot be precisely represented in Spark SQL, using TimestampType",
        precision);
    return DataTypes.TimestampType;
  }

  /**
   * Create an interval type.
   *
   * @return the Spark SQL DataType for interval
   */
  @Nonnull
  public DataType createInterval() {
    // Spark doesn't have a direct interval type, use string
    log.warn("INTERVAL cannot be precisely represented in Spark SQL, using StringType");
    return DataTypes.StringType;
  }

  /**
   * Create a row type with the specified fields.
   *
   * @param fields the list of name-type pairs
   * @return the Spark SQL DataType for row
   */
  @Nonnull
  public DataType createRow(@Nonnull final List<Pair<String, DataType>> fields) {
    return DataTypes.createStructType(
        fields.stream()
            .map(field -> DataTypes.createStructField(field.getLeft(), field.getRight(), true))
            .toArray(StructField[]::new)
    );
  }

  /**
   * Create an array type with the specified element type.
   *
   * @param elementType the element type
   * @return the Spark SQL DataType for array
   */
  @Nonnull
  public DataType createArray(@Nonnull final DataType elementType) {
    return DataTypes.createArrayType(elementType);
  }
}
