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

package au.csiro.pathling.operations.view;

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.util.TestDataSetup;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.skyscreamer.jsonassert.Customization;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.skyscreamer.jsonassert.comparator.CustomComparator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.EntityExchangeResult;
import org.springframework.test.web.reactive.server.WebTestClient;

/**
 * Integration tests for create, read and update of ViewDefinition resources over HTTP, covering the
 * retention of every root element defined by the SQL on FHIR ViewDefinition StructureDefinition.
 *
 * @author John Grimes
 */
@Slf4j
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"integration-test"})
class ViewDefinitionCrudIT {

  private static final String MINIMAL_VIEW_ID = "smoking-status";

  private static final String FULL_METADATA_VIEW_ID = "au-core-patient";

  private static final String TITLE = "Smoking Status (AU Core)";

  /** A minimal ViewDefinition, as posted by the reproduction in the issue. */
  private static final String MINIMAL_VIEW =
      """
      {
        "resourceType": "ViewDefinition",
        "id": "smoking-status",
        "name": "smoking_status",
        "status": "draft",
        "resource": "Observation",
        "select": [
          {
            "column": [
              {
                "path": "getResourceKey()",
                "name": "id",
                "type": "string"
              }
            ]
          }
        ]
      }
      """;

  /** The minimal ViewDefinition with a title added, as in the second step of the reproduction. */
  private static final String TITLED_VIEW =
      """
      {
        "resourceType": "ViewDefinition",
        "id": "smoking-status",
        "name": "smoking_status",
        "title": "Smoking Status (AU Core)",
        "status": "draft",
        "resource": "Observation",
        "select": [
          {
            "column": [
              {
                "path": "getResourceKey()",
                "name": "id",
                "type": "string"
              }
            ]
          }
        ]
      }
      """;

  /**
   * A ViewDefinition populating every root element, with the keys in the order the
   * StructureDefinition declares them. It projects Patient so that it can also be executed against
   * the test warehouse. A {@code meta} element is included because HAPI stamps a profile tag onto
   * custom resource types when it serialises them.
   */
  private static final String FULL_METADATA_VIEW =
      """
      {
        "resourceType": "ViewDefinition",
        "id": "au-core-patient",
        "meta": {
          "profile": ["http://hl7.org/fhir/uv/sql-on-fhir/StructureDefinition/ViewDefinition"]
        },
        "url": "https://aehrc.csiro.au/fhir/ViewDefinition/au-core-patient",
        "identifier": [
          {
            "use": "official",
            "system": "https://aehrc.csiro.au/fhir/view-definition",
            "value": "au-core-patient"
          },
          {
            "use": "secondary",
            "type": {
              "coding": [
                {
                  "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                  "code": "OID",
                  "display": "Object Identifier"
                }
              ],
              "text": "Object identifier"
            },
            "system": "urn:ietf:rfc:3986",
            "value": "urn:oid:1.2.36.1.2001.1005.99",
            "period": {
              "start": "2026-01-01",
              "end": "2028-12-31"
            }
          }
        ],
        "version": "1.2.0",
        "versionAlgorithmString": "semver",
        "name": "au_core_patient",
        "title": "AU Core patient",
        "status": "active",
        "experimental": false,
        "date": "2026-09-07T14:30:00+10:00",
        "publisher": "Australian e-Health Research Centre",
        "contact": [
          {
            "name": "AU Core support",
            "telecom": [
              {
                "system": "email",
                "value": "support@aehrc.csiro.au",
                "use": "work",
                "rank": 1,
                "period": {
                  "start": "2026-01-01T00:00:00+10:00"
                }
              },
              {
                "system": "url",
                "value": "https://aehrc.csiro.au",
                "use": "work"
              }
            ]
          }
        ],
        "description": "A flattened view of AU Core patients.\\n\\nOne row is produced for each patient, carrying the logical identifier and the family name.",
        "useContext": [
          {
            "code": {
              "system": "http://terminology.hl7.org/CodeSystem/usage-context-type",
              "code": "focus",
              "display": "Clinical Focus"
            },
            "valueCodeableConcept": {
              "coding": [
                {
                  "system": "http://snomed.info/sct",
                  "code": "184216000",
                  "display": "Patient record type"
                }
              ],
              "text": "Patient demographics"
            }
          },
          {
            "code": {
              "system": "http://terminology.hl7.org/CodeSystem/usage-context-type",
              "code": "age",
              "display": "Age Range"
            },
            "valueQuantity": {
              "value": 18,
              "comparator": ">=",
              "unit": "years",
              "system": "http://unitsofmeasure.org",
              "code": "a"
            }
          },
          {
            "code": {
              "system": "http://terminology.hl7.org/CodeSystem/usage-context-type",
              "code": "age",
              "display": "Age Range"
            },
            "valueRange": {
              "low": {
                "value": 18,
                "unit": "years",
                "system": "http://unitsofmeasure.org",
                "code": "a"
              },
              "high": {
                "value": 120,
                "unit": "years",
                "system": "http://unitsofmeasure.org",
                "code": "a"
              }
            }
          },
          {
            "code": {
              "system": "http://terminology.hl7.org/CodeSystem/usage-context-type",
              "code": "program",
              "display": "Program"
            },
            "valueReference": {
              "reference": "Organization/aehrc",
              "display": "Australian e-Health Research Centre"
            }
          }
        ],
        "jurisdiction": [
          {
            "coding": [
              {
                "system": "urn:iso:std:iso:3166",
                "code": "AU",
                "display": "Australia"
              },
              {
                "system": "urn:iso:std:iso:3166:-2",
                "code": "AU-QLD",
                "display": "Queensland"
              }
            ],
            "text": "Australia"
          }
        ],
        "purpose": "To support population health reporting on patient demographics.\\n\\nThe view is intended for analytical use over an AU Core conformant data set.",
        "copyright": "Copyright 2026 Commonwealth Scientific and Industrial Research Organisation (CSIRO).\\n\\nLicensed under the Apache License, Version 2.0.",
        "copyrightLabel": "CC-BY-4.0",
        "approvalDate": "2026-03-01",
        "lastReviewDate": "2026-08",
        "effectivePeriod": {
          "start": "2026-03-01",
          "end": "2027-03-01"
        },
        "topic": [
          {
            "coding": [
              {
                "system": "http://terminology.hl7.org/CodeSystem/definition-topic",
                "code": "assessment",
                "display": "Assessment"
              },
              {
                "system": "http://snomed.info/sct",
                "code": "184216000",
                "display": "Patient record type"
              }
            ],
            "text": "Patient demographics"
          }
        ],
        "author": [
          {
            "name": "John Grimes",
            "telecom": [
              {
                "system": "email",
                "value": "john.grimes@csiro.au",
                "use": "work"
              }
            ]
          }
        ],
        "editor": [
          {
            "name": "AU Core editorial group",
            "telecom": [
              {
                "system": "email",
                "value": "editors@aehrc.csiro.au",
                "use": "work"
              }
            ]
          }
        ],
        "reviewer": [
          {
            "name": "AU Core review panel",
            "telecom": [
              {
                "system": "email",
                "value": "reviewers@aehrc.csiro.au",
                "use": "work"
              }
            ]
          }
        ],
        "endorser": [
          {
            "name": "HL7 Australia",
            "telecom": [
              {
                "system": "url",
                "value": "https://hl7.org.au",
                "use": "work"
              }
            ]
          }
        ],
        "relatedArtifact": [
          {
            "type": "documentation",
            "label": "AU Core Patient",
            "display": "AU Core Patient profile guidance",
            "citation": "HL7 Australia. AU Core Implementation Guide, version 2.0.0.",
            "url": "https://hl7.org.au/fhir/core/StructureDefinition-au-core-patient.html",
            "document": {
              "contentType": "application/pdf",
              "url": "https://aehrc.csiro.au/docs/patient-view.pdf",
              "title": "Patient view notes"
            }
          },
          {
            "type": "derived-from",
            "label": "AU Core Patient",
            "resource": "http://hl7.org.au/fhir/core/StructureDefinition/au-core-patient"
          }
        ],
        "resource": "Patient",
        "profile": [
          "http://hl7.org.au/fhir/core/StructureDefinition/au-core-patient",
          "http://hl7.org/fhir/StructureDefinition/Patient"
        ],
        "fhirVersion": ["4.0.1"],
        "constant": [
          {
            "name": "family_required",
            "valueBoolean": true
          }
        ],
        "select": [
          {
            "column": [
              {
                "path": "id",
                "name": "id",
                "type": "id"
              },
              {
                "path": "name.first().family",
                "name": "family_name",
                "type": "string"
              }
            ]
          }
        ],
        "where": [
          {
            "path": "name.family.exists() = %family_required",
            "description": "Patients with a family name only."
          }
        ]
      }
      """;

  @LocalServerPort int port;

  @Autowired WebTestClient webTestClient;

  @TempDir private static Path warehouseDir;

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir.toAbsolutePath());
  }

  @BeforeEach
  void setup() {
    TestDataSetup.copyTestDataToTempDir(warehouseDir);
    webTestClient =
        webTestClient
            .mutate()
            .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(100 * 1024 * 1024))
            // Writing to and then querying a Delta table takes longer than the 5s default allows.
            .responseTimeout(Duration.ofSeconds(60))
            .build();
  }

  @AfterEach
  void cleanup() throws IOException {
    FileUtils.cleanDirectory(warehouseDir.toFile());
  }

  @Test
  void putAddingTitleIsReturnedByPutAndGet() {
    // A view without a title is stored, then the same view is stored again with a title added.
    putView(MINIMAL_VIEW_ID, MINIMAL_VIEW);
    getView(MINIMAL_VIEW_ID);

    // The title must be reflected in the response to the update.
    webTestClient
        .put()
        .uri(viewUri(MINIMAL_VIEW_ID))
        .header("Content-Type", "application/fhir+json")
        .header("Accept", "application/fhir+json")
        .bodyValue(TITLED_VIEW)
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.title")
        .isEqualTo(TITLE);

    // The title must also be returned by a subsequent read.
    webTestClient
        .get()
        .uri(viewUri(MINIMAL_VIEW_ID))
        .header("Accept", "application/fhir+json")
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.title")
        .isEqualTo(TITLE);
  }

  @Test
  void postFullMetadataReadsBackEqual() throws JSONException {
    final String created =
        body(
            webTestClient
                .post()
                .uri("http://localhost:" + port + "/fhir/ViewDefinition")
                .header("Content-Type", "application/fhir+json")
                .header("Accept", "application/fhir+json")
                .bodyValue(FULL_METADATA_VIEW)
                .exchange()
                .expectStatus()
                .isCreated()
                .expectBody()
                .returnResult());

    // The response to the create must carry every element of the request, bar the server-assigned
    // identifier.
    assertEqualsIgnoringId(FULL_METADATA_VIEW, created);

    // A read of the created resource must return the same content.
    final String assignedId = new JSONObject(created).getString("id");
    assertEqualsIgnoringId(FULL_METADATA_VIEW, getView(assignedId));
  }

  @Test
  void putFullMetadataReadsBackEqual() throws JSONException {
    // The response to the update must carry every element of the request.
    assertEqualsIgnoringId(FULL_METADATA_VIEW, putView(FULL_METADATA_VIEW_ID, FULL_METADATA_VIEW));

    // A read of the updated resource must return the same content.
    assertEqualsIgnoringId(FULL_METADATA_VIEW, getView(FULL_METADATA_VIEW_ID));
  }

  @Test
  void sqlRunOnStoredViewWithMetadataSucceeds() {
    putView(FULL_METADATA_VIEW_ID, FULL_METADATA_VIEW);

    final EntityExchangeResult<byte[]> result =
        webTestClient
            .get()
            .uri(
                "http://localhost:"
                    + port
                    + "/fhir/$sql-run?subjectReference=ViewDefinition/"
                    + FULL_METADATA_VIEW_ID
                    + "&_format=csv")
            .header("Accept", "text/csv")
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody()
            .returnResult();
    final String csv = body(result);

    final List<String> lines = csv.lines().filter(line -> !line.isBlank()).toList();
    assertThat(lines).isNotEmpty();
    assertThat(lines.get(0)).isEqualTo("id,family_name");
  }

  /**
   * Compares two ViewDefinitions, allowing the logical identifier and the profile tag that HAPI
   * stamps onto a custom resource type to differ.
   */
  private static void assertEqualsIgnoringId(
      @Nonnull final String expected, @Nonnull final String actual) throws JSONException {
    JSONAssert.assertEquals(
        expected,
        actual,
        new CustomComparator(
            JSONCompareMode.NON_EXTENSIBLE,
            new Customization("id", (one, two) -> true),
            new Customization("meta", (one, two) -> true)));
  }

  /** Stores the given view at the given identifier, returning the response body. */
  @Nonnull
  private String putView(@Nonnull final String id, @Nonnull final String view) {
    return body(
        webTestClient
            .put()
            .uri(viewUri(id))
            .header("Content-Type", "application/fhir+json")
            .header("Accept", "application/fhir+json")
            .bodyValue(view)
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody()
            .returnResult());
  }

  /** Reads the view with the given identifier, returning the response body. */
  @Nonnull
  private String getView(@Nonnull final String id) {
    return body(
        webTestClient
            .get()
            .uri(viewUri(id))
            .header("Accept", "application/fhir+json")
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody()
            .returnResult());
  }

  @Nonnull
  private String viewUri(@Nonnull final String id) {
    return "http://localhost:" + port + "/fhir/ViewDefinition/" + id;
  }

  @Nonnull
  private static String body(@Nonnull final EntityExchangeResult<byte[]> result) {
    return new String(
        Objects.requireNonNull(result.getResponseBodyContent()), StandardCharsets.UTF_8);
  }
}
