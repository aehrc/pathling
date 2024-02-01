/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.export;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class BulkExportServiceTest {


  @Test
  void computeTimeToSleep() {
    final BulkExportService bulkExportService = new BulkExportService(
        Mockito.mock(HttpClient.class), URI.create("http://example.com"));

    assertEquals(bulkExportService.minPoolingTimeout,
        bulkExportService.computeTimeToSleep(Optional.empty(), Duration.ZERO));

    assertEquals(bulkExportService.minPoolingTimeout,
        bulkExportService.computeTimeToSleep(Optional.of(Duration.ZERO), Duration.ZERO));

    assertEquals(bulkExportService.minPoolingTimeout,
        bulkExportService.computeTimeToSleep(Optional.of(Duration.ofSeconds(5)), Duration.ZERO));

    assertEquals(Duration.ofSeconds(5),
        bulkExportService.computeTimeToSleep(Optional.of(Duration.ofSeconds(5)),
            Duration.ofSeconds(100)));

    assertEquals(bulkExportService.maxPoolingTimeout,
        bulkExportService.computeTimeToSleep(Optional.of(Duration.ofSeconds(15)),
            Duration.ofSeconds(100)));

    assertEquals(Duration.ofSeconds(5),
        bulkExportService.computeTimeToSleep(Optional.of(Duration.ofSeconds(15)),
            Duration.ofSeconds(5)));
  }
}
