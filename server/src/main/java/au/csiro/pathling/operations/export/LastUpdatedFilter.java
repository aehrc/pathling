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

package au.csiro.pathling.operations.export;

import static org.apache.spark.sql.functions.lit;

import jakarta.annotation.Nonnull;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.BiFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.InstantType;

/**
 * Filters stored resources on {@code meta.lastUpdated}, for the {@code _since} and {@code _until}
 * parameters of the export operations. A resource without a {@code meta.lastUpdated} value is
 * always kept, as its time of last update is unknown.
 *
 * <p>Stored datasets carry only the elements their data populates, so a dataset may have no {@code
 * meta.lastUpdated} column at all, in which case every resource is kept. The value is stored as
 * text in its lexical form, so it is compared as a timestamp rather than as a string, which would
 * misorder values stated in different time zones.
 *
 * @author John Grimes
 */
public final class LastUpdatedFilter {

  private static final String META = "meta";

  private static final String LAST_UPDATED = "lastUpdated";

  private LastUpdatedFilter() {}

  /**
   * Keeps the resources last updated at or after the given instant.
   *
   * @param resources the stored resources
   * @param since the earliest time of last update to keep
   * @return the resources updated at or after the instant, or with no time of last update
   */
  @Nonnull
  public static Dataset<Row> since(
      @Nonnull final Dataset<Row> resources, @Nonnull final InstantType since) {
    return filter(resources, since, Column::geq);
  }

  /**
   * Keeps the resources last updated at or before the given instant.
   *
   * @param resources the stored resources
   * @param until the latest time of last update to keep
   * @return the resources updated at or before the instant, or with no time of last update
   */
  @Nonnull
  public static Dataset<Row> until(
      @Nonnull final Dataset<Row> resources, @Nonnull final InstantType until) {
    return filter(resources, until, Column::leq);
  }

  @Nonnull
  private static Dataset<Row> filter(
      @Nonnull final Dataset<Row> resources,
      @Nonnull final InstantType bound,
      @Nonnull final BiFunction<Column, Object, Column> comparison) {
    final Timestamp boundValue = new Timestamp(bound.getValue().getTime());
    return lastUpdated(resources)
        .map(
            lastUpdated ->
                resources.filter(
                    lastUpdated.isNull().or(comparison.apply(lastUpdated, lit(boundValue)))))
        .orElse(resources);
  }

  /**
   * Returns {@code meta.lastUpdated} as a timestamp, or empty if the dataset has no such column.
   */
  @Nonnull
  private static Optional<Column> lastUpdated(@Nonnull final Dataset<Row> resources) {
    return Arrays.stream(resources.schema().fields())
        .filter(field -> field.name().equals(META))
        .map(StructField::dataType)
        .filter(StructType.class::isInstance)
        .map(StructType.class::cast)
        .filter(meta -> Arrays.asList(meta.fieldNames()).contains(LAST_UPDATED))
        .findFirst()
        .map(meta -> resources.col(META).getField(LAST_UPDATED).cast(DataTypes.TimestampType));
  }
}
