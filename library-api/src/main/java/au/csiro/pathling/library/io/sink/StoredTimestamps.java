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

package au.csiro.pathling.library.io.sink;

import static au.csiro.pathling.io.TimestampStorage.OUTPUT_TIMESTAMP_TYPE;
import static au.csiro.pathling.io.TimestampStorage.TIMESTAMP_MICROS;
import static au.csiro.pathling.io.TimestampStorage.writesMicros;

import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;

/**
 * Refuses to write timestamps to Parquet other than as {@code INT64} microseconds adjusted to UTC,
 * which is how the range annotations of a date are stored (decision 138). The stored layout holds
 * no timestamps but the range annotations, so a dataset that holds a timestamp carries them.
 */
final class StoredTimestamps {

  private StoredTimestamps() {}

  /**
   * Throws where any of the datasets holds a timestamp and the session does not write timestamps to
   * Parquet as {@code INT64} microseconds, before anything is written.
   *
   * @param spark the session the datasets are written from
   * @param datasets the datasets to be written, by resource type
   * @throws IllegalStateException if a dataset holds a timestamp the session would write otherwise
   */
  static void requireMicros(
      @Nonnull final SparkSession spark, @Nonnull final Map<String, Dataset<Row>> datasets) {
    if (writesMicros(spark)) {
      return;
    }
    final String value = spark.conf().get(OUTPUT_TIMESTAMP_TYPE);
    datasets.forEach(
        (resourceType, dataset) -> {
          if (holdsTimestamp(dataset.schema())) {
            throw new IllegalStateException(
                "The "
                    + resourceType
                    + " data carries the range annotations of dates, which Pathling stores as"
                    + " INT64 TIMESTAMP(MICROS), but the Spark session writes timestamps to"
                    + " Parquet as "
                    + value
                    + " (`"
                    + OUTPUT_TIMESTAMP_TYPE
                    + "`). Set `"
                    + OUTPUT_TIMESTAMP_TYPE
                    + "="
                    + TIMESTAMP_MICROS
                    + "` on the session before writing.");
          }
        });
  }

  /** Returns whether a type is, or holds, a timestamp. */
  private static boolean holdsTimestamp(@Nonnull final DataType type) {
    if (type instanceof TimestampType) {
      return true;
    }
    if (type instanceof final StructType struct) {
      return Arrays.stream(struct.fields())
          .map(StructField::dataType)
          .anyMatch(StoredTimestamps::holdsTimestamp);
    }
    if (type instanceof final ArrayType array) {
      return holdsTimestamp(array.elementType());
    }
    if (type instanceof final MapType map) {
      return holdsTimestamp(map.keyType()) || holdsTimestamp(map.valueType());
    }
    return false;
  }
}
