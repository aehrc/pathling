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

import static org.junit.jupiter.api.Assertions.assertEquals;

import au.csiro.pathling.export.BulkExportResponse.ResourceElement;
import au.csiro.pathling.export.download.UrlDownloadTemplate.UrlDownloadEntry;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import au.csiro.pathling.export.fs.FileStore.FileHandle;
import org.junit.jupiter.api.Test;

public class BulkExportClientTest {

  BulkExportClient client = BulkExportClient.builder()
      .withFhirEndpointUrl("http://example.com")
      .withOutputDir("output-dir")
      .build();
  
  @Test
  void testMapsMultiPartResourceToSeparateFiles() {

    final BulkExportResponse response = BulkExportResponse.builder()
        .request("fake-request")
        .output(List.of(
            new ResourceElement("Condition", "http:/foo.bar/1", 10),
            new ResourceElement("Condition", "http:/foo.bar/2", 10),
            new ResourceElement("Condition", "http:/foo.bar/3", 10)
        ))
        .deleted(Collections.emptyList())
        .error(Collections.emptyList())
        .build();

    final List<UrlDownloadEntry> downloadUrls = client.getUrlDownloadEntries(
        response, FileHandle.ofLocal("output-dir"));

    assertEquals(
        List.of(
            new UrlDownloadEntry(URI.create("http:/foo.bar/1"), FileHandle.ofLocal("output-dir/Condition_0000.ndjson")),
            new UrlDownloadEntry(URI.create("http:/foo.bar/2"), FileHandle.ofLocal("output-dir/Condition_0001.ndjson")),
            new UrlDownloadEntry(URI.create("http:/foo.bar/3"), FileHandle.ofLocal("output-dir/Condition_0002.ndjson"))
        ),
        downloadUrls
    );
  }
  
  @Test
  void testMapsDifferentResourceToSeparateFiles() {

    final BulkExportClient client = BulkExportClient.builder()
        .withFhirEndpointUrl("http://example.com")
        .withOutputDir("output-dir")
        .withOutputExtension("xjson")
        .build();    
    
    final BulkExportResponse response = BulkExportResponse.builder()
        .request("fake-request")
        .output(List.of(
            new ResourceElement("Patient", "http:/foo.bar/1", 10),
            new ResourceElement("Condition", "http:/foo.bar/2", 10),
            new ResourceElement("Observation", "http:/foo.bar/3", 10)
        ))
        .deleted(Collections.emptyList())
        .error(Collections.emptyList())
        .build();

    final List<UrlDownloadEntry> downloadUrls = client.getUrlDownloadEntries(
        response, FileHandle.ofLocal("output-dir"));

    assertEquals(
        List.of(
            new UrlDownloadEntry(URI.create("http:/foo.bar/1"), FileHandle.ofLocal("output-dir/Patient_0000.xjson")),
            new UrlDownloadEntry(URI.create("http:/foo.bar/2"), FileHandle.ofLocal("output-dir/Condition_0000.xjson")),
            new UrlDownloadEntry(URI.create("http:/foo.bar/3"), FileHandle.ofLocal("output-dir/Observation_0000.xjson"))
        ),
        downloadUrls
    );
  }
}