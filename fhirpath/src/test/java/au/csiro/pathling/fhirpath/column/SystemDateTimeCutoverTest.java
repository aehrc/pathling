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

package au.csiro.pathling.fhirpath.column;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.unix_micros;
import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.schema.LayoutFields;
import au.csiro.pathling.schema.annotation.kinds.DateRangeKind;
import au.csiro.pathling.test.SpringBootUnitTest;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.TimeZone;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests that the range of a date before the Gregorian cutover is the true instant, on the proleptic
 * Gregorian calendar (decision 138), whatever the time zone of the JVM: computed from the text, as
 * a literal, and as stored in Parquet and read back.
 */
@SpringBootUnitTest
class SystemDateTimeCutoverTest {

  private static final String DATE = "1500-02-03";

  private static final long EXPECTED_START =
      ChronoUnit.MICROS.between(Instant.EPOCH, Instant.parse("1500-02-03T00:00:00Z"));

  private static final long EXPECTED_END =
      ChronoUnit.MICROS.between(Instant.EPOCH, Instant.parse("1500-02-03T23:59:59.999999Z"));

  @Autowired SparkSession spark;

  @TempDir Path tempDir;

  @ParameterizedTest
  @ValueSource(strings = {"UTC", "Australia/Brisbane", "America/Los_Angeles"})
  void theRangeIsTheTrueInstantInEveryTimeZone(@Nonnull final String zone) {
    final TimeZone original = TimeZone.getDefault();
    final String outputType = spark.conf().get("spark.sql.parquet.outputTimestampType");
    try {
      TimeZone.setDefault(TimeZone.getTimeZone(zone));
      spark.conf().set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS");

      final Row computed =
          spark
              .createDataset(List.of(DATE), Encoders.STRING())
              .select(micros(DateRangeKind.INSTANCE.compute(col("value"))))
              .first();
      assertBounds(computed);

      final Row literal =
          spark
              .range(1)
              .select(micros(SystemDateTime.range(SystemDateTime.fromLiteral(DATE))))
              .first();
      assertBounds(literal);

      final String path = tempDir.resolve(zone.replace('/', '_')).toString();
      spark
          .createDataset(List.of(DATE), Encoders.STRING())
          .select(DateRangeKind.INSTANCE.compute(col("value")).alias("range"))
          .write()
          .parquet(path);
      final Row stored = spark.read().parquet(path).select(micros(col("range"))).first();
      assertBounds(stored);
    } finally {
      TimeZone.setDefault(original);
      spark.conf().set("spark.sql.parquet.outputTimestampType", outputType);
    }
  }

  @Nonnull
  private static Column[] micros(@Nonnull final Column range) {
    return new Column[] {
      unix_micros(range.getField(LayoutFields.START_ANNOTATION)),
      unix_micros(range.getField(LayoutFields.END_ANNOTATION))
    };
  }

  private static void assertBounds(@Nonnull final Row row) {
    assertThat(row.getLong(0)).isEqualTo(EXPECTED_START);
    assertThat(row.getLong(1)).isEqualTo(EXPECTED_END);
  }
}
