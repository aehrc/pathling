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

package au.csiro.pathling.io;

import jakarta.annotation.Nonnull;
import org.apache.spark.sql.SparkSession;

/**
 * How Spark writes timestamps to Parquet, which for this layout must be as {@code INT64}
 * microseconds adjusted to UTC, the type the range annotations of a date are stored as (decision
 * 138).
 */
public final class TimestampStorage {

  /** The Spark setting that says how timestamps are written to Parquet. */
  @Nonnull
  public static final String OUTPUT_TIMESTAMP_TYPE = "spark.sql.parquet.outputTimestampType";

  /** The value of {@link #OUTPUT_TIMESTAMP_TYPE} that writes them as the layout stores them. */
  @Nonnull public static final String TIMESTAMP_MICROS = "TIMESTAMP_MICROS";

  private TimestampStorage() {}

  /**
   * Returns whether a session writes timestamps to Parquet as {@code INT64} microseconds, as the
   * layout stores the range annotations of a date.
   *
   * @param spark the session
   * @return whether the session writes timestamps as the layout stores them
   */
  public static boolean writesMicros(@Nonnull final SparkSession spark) {
    return TIMESTAMP_MICROS.equalsIgnoreCase(spark.conf().get(OUTPUT_TIMESTAMP_TYPE));
  }
}
