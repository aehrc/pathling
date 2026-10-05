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

package au.csiro.pathling.config;

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.util.LogCapture;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import jakarta.annotation.Nonnull;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ServerConfiguration#logConfiguration()}, covering the startup log line
 * emitted for each configured external table.
 *
 * @author John Grimes
 */
class ServerConfigurationTest {

  @Test
  void logsOneInfoLinePerExternalTable() {
    // Operators learn which tables are reachable from SQL from the startup log, so each entry must
    // be recorded with everything needed to recognise it: url, path and format.
    final ServerConfiguration config = new ServerConfiguration();
    config
        .getSqlQuery()
        .setExternalTables(
            List.of(
                externalTable(
                    "https://example.org/data/refsets", "s3a://bucket/reference/refsets", "delta"),
                externalTable(
                    "https://example.org/data/postcodes", "file:///data/postcodes", "parquet")));

    try (final LogCapture capture = LogCapture.forClass(ServerConfiguration.class)) {
      config.logConfiguration();

      final List<ILoggingEvent> infoEvents =
          capture.events().stream().filter(event -> event.getLevel() == Level.INFO).toList();
      assertThat(infoEvents).hasSize(2);
      assertThat(infoEvents.get(0).getFormattedMessage())
          .contains("https://example.org/data/refsets")
          .contains("s3a://bucket/reference/refsets")
          .contains("delta");
      assertThat(infoEvents.get(1).getFormattedMessage())
          .contains("https://example.org/data/postcodes")
          .contains("file:///data/postcodes")
          .contains("parquet");
    }
  }

  @Test
  void logsNothingAtInfoWhenNoExternalTablesAreConfigured() {
    final ServerConfiguration config = new ServerConfiguration();

    try (final LogCapture capture = LogCapture.forClass(ServerConfiguration.class)) {
      config.logConfiguration();

      assertThat(capture.events()).noneMatch(event -> event.getLevel() == Level.INFO);
    }
  }

  @Nonnull
  private static ExternalTableConfiguration externalTable(
      @Nonnull final String url, @Nonnull final String path, @Nonnull final String format) {
    final ExternalTableConfiguration entry = new ExternalTableConfiguration();
    entry.setUrl(url);
    entry.setPath(path);
    entry.setFormat(format);
    return entry;
  }
}
