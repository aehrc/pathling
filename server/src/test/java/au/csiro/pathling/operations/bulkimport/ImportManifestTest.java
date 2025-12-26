package au.csiro.pathling.operations.bulkimport;

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.operations.bulkimport.ImportManifest;
import au.csiro.pathling.operations.bulkimport.ImportManifestInput;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for ImportManifest and ImportManifestInput JSON serialisation/deserialisation.
 *
 * @author Felix Naumann
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
    final ImportManifest manifest = new ImportManifest(
        "application/fhir+ndjson",
        "https://example.org/source",
        List.of(
            new ImportManifestInput("Patient", "s3://bucket/patients.ndjson"),
            new ImportManifestInput("Observation", "s3://bucket/observations.ndjson")
        ),
        "merge"
    );

    // When
    final String json = objectMapper.writeValueAsString(manifest);

    // Then
    assertThat(json).isNotNull();
    assertThat(json).contains("\"inputFormat\":\"application/fhir+ndjson\"");
    assertThat(json).contains("\"inputSource\":\"https://example.org/source\"");
    assertThat(json).contains("\"input\":");
    assertThat(json).contains("\"mode\":\"merge\"");
  }

  @Test
  void test_importManifest_serialisation_without_mode() throws Exception {
    // Given
    final ImportManifest manifest = new ImportManifest(
        "application/fhir+ndjson",
        "https://example.org/source",
        List.of(new ImportManifestInput("Patient", "s3://bucket/patients.ndjson")),
        null
    );

    // When
    final String json = objectMapper.writeValueAsString(manifest);

    // Then
    assertThat(json).isNotNull();
    assertThat(json).contains("\"inputFormat\":\"application/fhir+ndjson\"");
    assertThat(json).contains("\"inputSource\":\"https://example.org/source\"");
    assertThat(json).contains("\"input\":");
  }

  @Test
  void importManifestSerialisationWithoutInputSource() throws Exception {
    // inputSource is optional per the SMART Bulk Data Import spec.
    // Given
    final ImportManifest manifest = new ImportManifest(
        "application/fhir+ndjson",
        null,
        List.of(new ImportManifestInput("Patient", "s3://bucket/patients.ndjson")),
        "overwrite"
    );

    // When
    final String json = objectMapper.writeValueAsString(manifest);

    // Then
    assertThat(json).isNotNull();
    assertThat(json).contains("\"inputFormat\":\"application/fhir+ndjson\"");
    assertThat(json).contains("\"input\":");
    assertThat(json).contains("\"mode\":\"overwrite\"");
    // inputSource should either be absent or null.
    assertThat(json).satisfiesAnyOf(
        j -> assertThat(j).doesNotContain("\"inputSource\""),
        j -> assertThat(j).contains("\"inputSource\":null")
    );
  }

  // ========================================
  // ImportManifest Deserialisation Tests
  // ========================================

  @Test
  void test_importManifest_deserialisation() throws Exception {
    // Given
    final String json = """
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
          "mode": "merge"
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
    assertThat(manifest.mode()).isEqualTo("merge");
  }

  @Test
  void test_importManifest_deserialisation_without_mode() throws Exception {
    // Given
    final String json = """
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
    assertThat(manifest.mode()).isNull();
  }

  @Test
  void importManifestDeserialisationWithoutInputSource() throws Exception {
    // inputSource is optional per the SMART Bulk Data Import spec.
    // Given
    final String json = """
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
    final String json = """
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
    final String json = """
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
    final ImportManifestInput input = new ImportManifestInput(
        "Patient",
        "s3://bucket/patients.ndjson"
    );

    // When
    final String json = objectMapper.writeValueAsString(input);

    // Then
    assertThat(json).isNotNull();
    assertThat(json).contains("\"type\":\"Patient\"");
    assertThat(json).contains("\"url\":\"s3://bucket/patients.ndjson\"");
  }

  // ========================================
  // ImportManifestInput Deserialisation Tests
  // ========================================

  @Test
  void test_importManifestInput_deserialisation() throws Exception {
    // Given
    final String json = """
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
    final ImportManifest original = new ImportManifest(
        "application/fhir+ndjson",
        "https://example.org/source",
        List.of(
            new ImportManifestInput("Patient", "s3://bucket/patients.ndjson"),
            new ImportManifestInput("Observation", "s3://bucket/observations.ndjson")
        ),
        "append"
    );

    // When - serialise and deserialise
    final String json = objectMapper.writeValueAsString(original);
    final ImportManifest roundtrip = objectMapper.readValue(json, ImportManifest.class);

    // Then
    assertThat(roundtrip.inputFormat()).isEqualTo(original.inputFormat());
    assertThat(roundtrip.inputSource()).isEqualTo(original.inputSource());
    assertThat(roundtrip.input()).hasSize(original.input().size());
    assertThat(roundtrip.input().get(0).type()).isEqualTo(original.input().get(0).type());
    assertThat(roundtrip.input().get(0).url()).isEqualTo(original.input().get(0).url());
    assertThat(roundtrip.mode()).isEqualTo(original.mode());
  }

  @Test
  void test_importManifestInput_roundtrip() throws Exception {
    // Given
    final ImportManifestInput original = new ImportManifestInput(
        "Observation",
        "s3://bucket/observations.ndjson"
    );

    // When - serialise and deserialise
    final String json = objectMapper.writeValueAsString(original);
    final ImportManifestInput roundtrip = objectMapper.readValue(json, ImportManifestInput.class);

    // Then
    assertThat(roundtrip.type()).isEqualTo(original.type());
    assertThat(roundtrip.url()).isEqualTo(original.url());
  }
}
