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

package au.csiro.pathling.terminology.store;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

/**
 * A read-only, column-name addressable view over a single row read from a terminology store table.
 *
 * <p>Instances are only valid within the callback that receives them; the underlying Delta Kernel
 * row is backed by columnar batch memory that is reused as iteration proceeds, so values must be
 * extracted before the callback returns.
 *
 * @author John Grimes
 */
public class TerminologyStoreRow {

  @Nonnull private final Row row;

  TerminologyStoreRow(@Nonnull final Row row) {
    this.row = row;
  }

  /**
   * Returns whether the named column is null in this row.
   *
   * @param column the column name
   * @return true if the value is null
   */
  public boolean isNull(@Nonnull final String column) {
    return row.isNullAt(ordinal(column));
  }

  /**
   * Returns the value of a string column, or null if the value is null.
   *
   * @param column the column name
   * @return the string value, or null
   */
  @Nullable
  public String getString(@Nonnull final String column) {
    final int ordinal = ordinal(column);
    return row.isNullAt(ordinal) ? null : row.getString(ordinal);
  }

  /**
   * Returns the value of an integer column.
   *
   * @param column the column name
   * @return the integer value
   */
  public int getInt(@Nonnull final String column) {
    return row.getInt(ordinal(column));
  }

  /**
   * Returns the value of a long column.
   *
   * @param column the column name
   * @return the long value
   */
  public long getLong(@Nonnull final String column) {
    return row.getLong(ordinal(column));
  }

  /**
   * Returns the value of a boolean column.
   *
   * @param column the column name
   * @return the boolean value
   */
  public boolean getBoolean(@Nonnull final String column) {
    return row.getBoolean(ordinal(column));
  }

  /**
   * Returns the value of a timestamp column as an {@link Instant}, or null if the value is null.
   *
   * @param column the column name
   * @return the instant, or null
   */
  @Nullable
  public Instant getInstant(@Nonnull final String column) {
    final int ordinal = ordinal(column);
    if (row.isNullAt(ordinal)) {
      return null;
    }
    // Delta stores timestamps as microseconds since the epoch.
    return Instant.EPOCH.plus(row.getLong(ordinal), ChronoUnit.MICROS);
  }

  /**
   * Returns the value of a {@code map<string, string>} column, or null if the value is null.
   *
   * @param column the column name
   * @return the map, or null
   */
  @Nullable
  public Map<String, String> getStringMap(@Nonnull final String column) {
    final int ordinal = ordinal(column);
    if (row.isNullAt(ordinal)) {
      return null;
    }
    final MapValue mapValue = row.getMap(ordinal);
    final ColumnVector keys = mapValue.getKeys();
    final ColumnVector values = mapValue.getValues();
    final Map<String, String> result = new HashMap<>(mapValue.getSize());
    for (int i = 0; i < mapValue.getSize(); i++) {
      result.put(keys.getString(i), values.isNullAt(i) ? null : values.getString(i));
    }
    return result;
  }

  private int ordinal(@Nonnull final String column) {
    final int ordinal = row.getSchema().indexOf(column);
    if (ordinal < 0) {
      throw new IllegalArgumentException("Column not present in row: " + column);
    }
    return ordinal;
  }
}
