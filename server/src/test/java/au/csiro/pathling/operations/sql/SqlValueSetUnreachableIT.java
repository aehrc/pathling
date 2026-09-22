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

package au.csiro.pathling.operations.sql;

import static au.csiro.pathling.operations.sqlquery.SqlLibraryParser.LIBRARY_TYPE_SYSTEM;
import static au.csiro.pathling.operations.sqlquery.SqlLibraryParser.SQL_QUERY_TYPE_CODE;
import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.operations.sqlquery.SqlQueryOutputFormat;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import org.hl7.fhir.r4.model.Attachment;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;
import org.hl7.fhir.r4.model.Library;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.OperationOutcomeIssueComponent;
import org.hl7.fhir.r4.model.RelatedArtifact;
import org.hl7.fhir.r4.model.RelatedArtifact.RelatedArtifactType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Integration test for User Story 4 scenario 8 of value set dependencies (spec 061): the configured
 * terminology server cannot be reached. A value set dependency not supplied inline is then a {@code
 * 422} naming the label, the canonical URL and the configured server URL, and nothing more about
 * the server or the failure, on both {@code $sql-run} and a {@code $sql-export} kick-off, which
 * creates no job.
 *
 * <p>A separate class from {@link SqlValueSetIT} because {@code pathling.terminology.serverUrl} is
 * fixed for the life of the application context: the URL here points at a port that was free when
 * the context started and that nothing listens on. Client retries are disabled so the request fails
 * on its first connection attempt.
 *
 * <p>Backed by {@link SqlValueSetTestConfiguration} for the stored ViewDefinition and the Condition
 * data.
 *
 * @author John Grimes
 */
@Tag("IntegrationTest")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles({"integration-test"})
@Import(SqlValueSetTestConfiguration.class)
class SqlValueSetUnreachableIT extends AbstractAsyncExportIT {

  /** The configured terminology server URL, on a port nothing listens on. */
  private static String serverUrl;

  @Autowired private FhirContext fhirContext;

  private IParser jsonParser;

  @DynamicPropertySource
  static void configureProperties(final DynamicPropertyRegistry registry) {
    final Path warehouseDir =
        Path.of("src/test/resources/test-data/bulk/fhir/delta").toAbsolutePath();
    registry.add("pathling.storage.warehouseUrl", () -> "file://" + warehouseDir);
    serverUrl = "http://localhost:" + closedPort() + "/fhir";
    registry.add("pathling.terminology.serverUrl", () -> serverUrl);
    registry.add("pathling.terminology.cache.enabled", () -> "false");
    registry.add("pathling.terminology.client.retryEnabled", () -> "false");
  }

  /**
   * A port the operating system handed out and that was released again, so nothing listens on it.
   */
  private static int closedPort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @BeforeEach
  void setUp() {
    jsonParser = fhirContext.newJsonParser();
  }

  @Test
  void unreachableServerIsA422NamingTheValueSetAndTheConfiguredUrlOnly() {
    final String body =
        postExpectStatus(
            parametersJson(
                sqlQueryLibrary(
                    "SELECT * FROM cvd_codes",
                    Map.of("cvd_codes", SqlValueSetTestConfiguration.CVD_URL))),
            422);

    assertUnreachableIssue(body);
  }

  @Test
  void kickOffAgainstAnUnreachableServerIsA422AndCreatesNoJob() {
    final int jobsBefore = jobCount();

    final byte[] payload =
        kickOff(
                systemLevelUri(),
                parameters(
                    subject(
                        simpleParam("name", "valueString", "inline"),
                        resourcePart(
                            "subjectResource",
                            resourceMap(
                                sqlQueryLibrary(
                                    "SELECT * FROM cvd_codes",
                                    Map.of("cvd_codes", SqlValueSetTestConfiguration.CVD_URL)))))))
            .expectStatus()
            .isEqualTo(422)
            .expectHeader()
            .doesNotExist("Content-Location")
            .expectBody()
            .returnResult()
            .getResponseBodyContent();

    assertUnreachableIssue(
        new String(payload == null ? new byte[0] : payload, StandardCharsets.UTF_8));
    assertThat(jobCount()).as("A rejected kick-off must not register a job").isEqualTo(jobsBefore);
  }

  /**
   * Asserts the single issue names the subject, the label, the canonical URL and the configured
   * server URL, and discloses nothing else about the server or the connection failure.
   */
  private void assertUnreachableIssue(@Nonnull final String body) {
    final OperationOutcome outcome = (OperationOutcome) jsonParser.parseResource(body);
    assertThat(outcome.getIssue()).hasSize(1);
    final OperationOutcomeIssueComponent issue = outcome.getIssueFirstRep();
    assertThat(issue.getExpression())
        .extracting(value -> value.getValue())
        .containsExactly(SubjectResolver.SUBJECT_EXPRESSION);
    assertThat(issue.getDiagnostics())
        .isEqualTo(
            "The membership of the value set for label 'cvd_codes' (canonical URL '"
                + SqlValueSetTestConfiguration.CVD_URL
                + "') could not be determined: terminology server "
                + serverUrl
                + " could not be reached");
    assertThat(body)
        .doesNotContain(
            "Connection refused", "ConnectException", "HttpHostConnectException", "127.0.0.1");
  }

  // -------------------------------------------------------------------------
  // Request helpers
  // -------------------------------------------------------------------------

  /** The number of jobs the {@code $jobs} listing currently holds. */
  private int jobCount() {
    final byte[] body =
        webTestClient
            .get()
            .uri("http://localhost:" + port + "/fhir/$jobs")
            .header("Accept", "application/fhir+json")
            .exchange()
            .expectStatus()
            .isOk()
            .expectBody()
            .returnResult()
            .getResponseBodyContent();
    return paramsByName(parse(body), "job").size();
  }

  @Nonnull
  private String postExpectStatus(@Nonnull final String body, final int status) {
    final byte[] payload =
        webTestClient
            .post()
            .uri("http://localhost:" + port + "/fhir/$sql-run")
            .header("Content-Type", "application/fhir+json")
            .header("Accept", SqlQueryOutputFormat.NDJSON.getContentType())
            .bodyValue(body)
            .exchange()
            .expectStatus()
            .isEqualTo(status)
            .expectBody()
            .returnResult()
            .getResponseBodyContent();
    return payload == null ? "" : new String(payload, StandardCharsets.UTF_8);
  }

  /** Builds an inline SQLQuery Library with the given depends-on dependencies (label to URL). */
  @Nonnull
  private static Library sqlQueryLibrary(
      @Nonnull final String sql, @Nonnull final Map<String, String> dependenciesByLabel) {
    final Library library = new Library();
    library.setStatus(PublicationStatus.ACTIVE);
    library.setType(
        new CodeableConcept()
            .addCoding(new Coding().setSystem(LIBRARY_TYPE_SYSTEM).setCode(SQL_QUERY_TYPE_CODE)));
    final Attachment content = new Attachment();
    content.setContentType("application/sql");
    content.setData(sql.getBytes(StandardCharsets.UTF_8));
    library.addContent(content);
    dependenciesByLabel.forEach(
        (label, resource) ->
            library.addRelatedArtifact(
                new RelatedArtifact()
                    .setType(RelatedArtifactType.DEPENDSON)
                    .setLabel(label)
                    .setResource(resource)));
    return library;
  }

  /** Encodes a resource as the generic JSON map the Gson-built request bodies carry. */
  @Nonnull
  @SuppressWarnings("unchecked")
  private Map<String, Object> resourceMap(@Nonnull final org.hl7.fhir.r4.model.Resource resource) {
    return gson.fromJson(jsonParser.encodeResourceToString(resource), Map.class);
  }

  /** Wraps the Library as the {@code subjectResource} of a {@code $sql-run} Parameters body. */
  @Nonnull
  private String parametersJson(@Nonnull final Library library) {
    return gson.toJson(
        parameters(
            resourcePart("subjectResource", resourceMap(library)),
            simpleParam("_format", "valueString", SqlQueryOutputFormat.NDJSON.getCode())));
  }
}
