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

package au.csiro.pathling.operations.bulkimport;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;

/**
 * Tests for ImportManifest and ImportManifestInput JSON serialisation/deserialisation.
 *
 * @author Felix Naumann
 * @author John Grimes
 */
class ImportManifestTest {

  private ObjectMapper objectMapper;

  @BeforeEach
  void setUp() {
    objectMapper = new ObjectMapper();
  }

  // ========================================
  // ImportManifest Serialisation Tests
  // ========================================

  @Test
  void test_importManifest_serialisation() throws Exception {
    // Given
    final ImportManifest manifest =
        new ImportManifest(
            "application/fhir+ndjson",
            "https://example.org/source",
            List.of(
                new ImportManifestInput("Patient", "s3://bucket/patients.ndjson"),
                new ImportManifestInput("Observation", "s3://bucket/observations.ndjson")),
            "merge");

    // When
    final String json = objectMapper.writeValueAsString(manifest);

    // Then
    final String expectedJson =
        """
        {
          "inputFormat": "application/fhir+ndjson",
          "inputSource": "https://example.org/source",
          "input": [
            {
              "type": "Patient",
              "url": "s3://bucket/patients.ndjson"
            },
            {
              "type": "Observation",
              "url": "s3://bucket/observations.ndjson"
            }
          ],
          "saveMode": "merge"
        }
        """;
    JSONAssert.assertEquals(expectedJson, json, JSONCompareMode.STRICT);
  }

  @Test
  void test_importManifest_serialisation_without_mode() throws Exception {
    // Given
    final ImportManifest manifest =
        new ImportManifest(
            "application/fhir+ndjson",
            "https://example.org/source",
            List.of(new ImportManifestInput("Patient", "s3://bucket/patients.ndjson")),
            null);

    // When
    final String json = objectMapper.writeValueAsString(manifest);

    // Then
    final String expectedJson =
        """
        {
          "inputFormat": "application/fhir+ndjson",
          "inputSource": "https://example.org/source",
          "input": [
            {
              "type": "Patient",
              "url": "s3://bucket/patients.ndjson"
            }
          ],
          "saveMode": null
        }
        """;
    JSONAssert.assertEquals(expectedJson, json, JSONCompareMode.STRICT);
  }

  @Test
  void importManifestSerialisationWithoutInputSource() throws Exception {
    // inputSource is optional per the SMART Bulk Data Import spec.
    // Given
    final ImportManifest manifest =
        new ImportManifest(
            "application/fhir+ndjson",
            null,
            List.of(new ImportManifestInput("Patient", "s3://bucket/patients.ndjson")),
            "overwrite");

    // When
    final String json = objectMapper.writeValueAsString(manifest);

    // Then
    final String expectedJson =
        """
        {
          "inputFormat": "application/fhir+ndjson",
          "inputSource": null,
          "input": [
            {
              "type": "Patient",
              "url": "s3://bucket/patients.ndjson"
            }
          ],
          "saveMode": "overwrite"
        }
        """;
    JSONAssert.assertEquals(expectedJson, json, JSONCompareMode.STRICT);
  }

  // ========================================
  // ImportManifest Deserialisation Tests
  // ========================================

  @Test
  void test_importManifest_deserialisation() throws Exception {
    // Given
    final String json =
        """
        {
          "inputFormat": "application/fhir+ndjson",
          "inputSource": "https://example.org/source",
          "input": [
            {
              "type": "Patient",
              "url": "s3://bucket/patients.ndjson"
            },
            {
              "type": "Observation",
              "url": "s3://bucket/observations.ndjson"
            }
          ],
          "saveMode": "merge"
        }
        """;

    // When
    final ImportManifest manifest = objectMapper.readValue(json, ImportManifest.class);

    // Then
    assertThat(manifest).isNotNull();
    assertThat(manifest.inputFormat()).isEqualTo("application/fhir+ndjson");
    assertThat(manifest.inputSource()).isEqualTo("https://example.org/source");
    assertThat(manifest.input()).hasSize(2);
    assertThat(manifest.input().get(0).type()).isEqualTo("Patient");
    assertThat(manifest.input().get(0).url()).isEqualTo("s3://bucket/patients.ndjson");
    assertThat(manifest.input().get(1).type()).isEqualTo("Observation");
    assertThat(manifest.input().get(1).url()).isEqualTo("s3://bucket/observations.ndjson");
    assertThat(manifest.saveMode()).isEqualTo("merge");
  }

  @Test
  void test_importManifest_deserialisation_without_mode() throws Exception {
    // Given
    final String json =
        """
        {
          "inputFormat": "application/fhir+ndjson",
          "inputSource": "https://example.org/source",
          "input": [
            {
              "type": "Patient",
              "url": "s3://bucket/patients.ndjson"
            }
          ]
        }
        """;

    // When
    final ImportManifest manifest = objectMapper.readValue(json, ImportManifest.class);

    // Then
    assertThat(manifest).isNotNull();
    assertThat(manifest.inputFormat()).isEqualTo("application/fhir+ndjson");
    assertThat(manifest.inputSource()).isEqualTo("https://example.org/source");
    assertThat(manifest.input()).hasSize(1);
    assertThat(manifest.saveMode()).isNull();
  }

  @Test
  void importManifestDeserialisationWithoutInputSource() throws Exception {
    // inputSource is optional per the SMART Bulk Data Import spec.
    // Given
    final String json =
        """
        {
          "inputFormat": "application/fhir+ndjson",
          "input": [
            {
              "type": "Patient",
              "url": "s3://bucket/patients.ndjson"
            }
          ]
        }
        """;

    // When
    final ImportManifest manifest = objectMapper.readValue(json, ImportManifest.class);

    // Then
    assertThat(manifest).isNotNull();
    assertThat(manifest.inputFormat()).isEqualTo("application/fhir+ndjson");
    assertThat(manifest.inputSource()).isNull();
    assertThat(manifest.input()).hasSize(1);
  }

  @Test
  void test_importManifest_deserialisation_with_parquet_format() throws Exception {
    // Given
    final String json =
        """
        {
          "inputFormat": "application/parquet",
          "inputSource": "https://example.org/source",
          "input": [
            {
              "type": "Patient",
              "url": "s3://bucket/patients.parquet"
            }
          ]
        }
        """;

    // When
    final ImportManifest manifest = objectMapper.readValue(json, ImportManifest.class);

    // Then
    assertThat(manifest.inputFormat()).isEqualTo("application/parquet");
  }

  @Test
  void test_importManifest_deserialisation_with_delta_format() throws Exception {
    // Given
    final String json =
        """
        {
          "inputFormat": "application/delta",
          "inputSource": "https://example.org/source",
          "input": [
            {
              "type": "Patient",
              "url": "s3://bucket/patients"
            }
          ]
        }
        """;

    // When
    final ImportManifest manifest = objectMapper.readValue(json, ImportManifest.class);

    // Then
    assertThat(manifest.inputFormat()).isEqualTo("application/delta");
  }

  // ========================================
  // ImportManifestInput Serialisation Tests
  // ========================================

  @Test
  void test_importManifestInput_serialisation() throws Exception {
    // Given
    final ImportManifestInput input =
        new ImportManifestInput("Patient", "s3://bucket/patients.ndjson");

    // When
    final String json = objectMapper.writeValueAsString(input);

    // Then
    final String expectedJson =
        """
        {
          "type": "Patient",
          "url": "s3://bucket/patients.ndjson"
        }
        """;
    JSONAssert.assertEquals(expectedJson, json, JSONCompareMode.STRICT);
  }

  // ========================================
  // ImportManifestInput Deserialisation Tests
  // ========================================

  @Test
  void test_importManifestInput_deserialisation() throws Exception {
    // Given
    final String json =
        """
        {
          "type": "Patient",
          "url": "s3://bucket/patients.ndjson"
        }
        """;

    // When
    final ImportManifestInput input = objectMapper.readValue(json, ImportManifestInput.class);

    // Then
    assertThat(input).isNotNull();
    assertThat(input.type()).isEqualTo("Patient");
    assertThat(input.url()).isEqualTo("s3://bucket/patients.ndjson");
  }

  // ========================================
  // Round-trip Tests
  // ========================================

  @Test
  void test_importManifest_roundtrip() throws Exception {
    // Given
    final ImportManifest original =
        new ImportManifest(
            "application/fhir+ndjson",
            "https://example.org/source",
            List.of(
                new ImportManifestInput("Patient", "s3://bucket/patients.ndjson"),
                new ImportManifestInput("Observation", "s3://bucket/observations.ndjson")),
            "append");

    // When - serialise and deserialise.
    final String json = objectMapper.writeValueAsString(original);
    final ImportManifest roundtrip = objectMapper.readValue(json, ImportManifest.class);

    // Then
    assertThat(roundtrip.inputFormat()).isEqualTo(original.inputFormat());
    assertThat(roundtrip.inputSource()).isEqualTo(original.inputSource());
    assertThat(roundtrip.input()).hasSize(original.input().size());
    assertThat(roundtrip.input().get(0).type()).isEqualTo(original.input().get(0).type());
    assertThat(roundtrip.input().get(0).url()).isEqualTo(original.input().get(0).url());
    assertThat(roundtrip.saveMode()).isEqualTo(original.saveMode());
  }

  @Test
  void test_importManifestInput_roundtrip() throws Exception {
    // Given
    final ImportManifestInput original =
        new ImportManifestInput("Observation", "s3://bucket/observations.ndjson");

    // When - serialise and deserialise.
    final String json = objectMapper.writeValueAsString(original);
    final ImportManifestInput roundtrip = objectMapper.readValue(json, ImportManifestInput.class);

    // Then
    assertThat(roundtrip.type()).isEqualTo(original.type());
    assertThat(roundtrip.url()).isEqualTo(original.url());
  }
}
