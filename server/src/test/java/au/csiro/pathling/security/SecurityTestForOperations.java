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

package au.csiro.pathling.security;

import static java.util.Objects.requireNonNull;
import static org.mockito.Mockito.when;

import au.csiro.pathling.async.Job;
import au.csiro.pathling.async.Job.JobTag;
import au.csiro.pathling.async.JobRegistry;
import au.csiro.pathling.async.RequestTagFactory;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.DataSourceBuilder;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.operations.bulkexport.ExportExecutor;
import au.csiro.pathling.operations.bulkexport.ExportOperationHelper;
import au.csiro.pathling.operations.bulkexport.ExportOperationValidator;
import au.csiro.pathling.operations.bulkexport.ExportOutputFormat;
import au.csiro.pathling.operations.bulkexport.ExportRequest;
import au.csiro.pathling.operations.bulkexport.ExportRequest.ExportLevel;
import au.csiro.pathling.operations.bulkexport.ExportResult;
import au.csiro.pathling.operations.bulkexport.ExportResultProvider;
import au.csiro.pathling.operations.bulkexport.ExportResultRegistry;
import au.csiro.pathling.operations.bulkexport.SystemExportProvider;
import au.csiro.pathling.operations.compartment.PatientCompartmentService;
import au.csiro.pathling.util.TestDataSetup;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.UnaryOperator;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Parameters;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

@ActiveProfiles({"unit-test"})
@Tag("IntegrationTest")
@TestPropertySource(properties = {"pathling.async.enabled=false"})
abstract class SecurityTestForOperations<T> extends SecurityTest {

  public static final String ADMIN_USER = "admin";

  public static final UnaryOperator<String> ERROR_MSG =
      "Missing authority: 'pathling:%s'"::formatted;

  protected SystemExportProvider exportProvider;

  @MockBean protected ServletRequestDetails requestDetails;

  @MockBean protected JobRegistry jobRegistry;

  @Autowired protected RequestTagFactory requestTagFactory;

  @MockBean protected Job<T> job;

  @Autowired private PathlingContext pathlingContext;

  @Autowired private FhirContext fhirContext;

  @Autowired private SparkSession sparkSession;

  @Autowired private ExportOperationValidator exportOperationValidator;

  @Autowired private ServerConfiguration serverConfiguration;

  @Autowired private ExportResultRegistry exportResultRegistry;

  @Autowired private ExportResultProvider exportResultProvider;

  @Autowired private PatientCompartmentService patientCompartmentService;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setup() {
    when(requestDetails.getCompleteUrl()).thenReturn("test-url");
    when(requestDetails.getHeaders("Prefer")).thenReturn(List.of("respond-async"));
    when(requestDetails.getHeader("Prefer")).thenReturn("respond-async");
    final Authentication auth = SecurityContextHolder.getContext().getAuthentication();
    final JobTag tag = requestTagFactory.createTag(requestDetails, auth);
    when(jobRegistry.get(tag)).thenReturn((Job<Object>) job);
    when(job.getId()).thenReturn(UUID.randomUUID().toString());
  }

  MockHttpServletResponse performExportResult(
      final String jobId, final String file, @Nullable final String ownerId) {
    exportResultRegistry.put(jobId, new ExportResult(Optional.ofNullable(ownerId)));
    final MockHttpServletResponse response = new MockHttpServletResponse();
    exportResultProvider.result(jobId, file, response);
    return response;
  }

  JsonNode performExport() {
    return performExport(ADMIN_USER, List.of(), false);
  }

  JsonNode performLenientExport() {
    return performExport(ADMIN_USER, List.of(), true);
  }

  JsonNode performExport(final String ownerId) {
    return performExport(ownerId, List.of(), false);
  }

  JsonNode performExport(final String ownerId, final List<String> type, final boolean lenient) {
    return performExport(exportProvider, ownerId, type, lenient);
  }

  @SuppressWarnings("unchecked")
  JsonNode performExport(
      final SystemExportProvider exportProvider,
      final String ownerId,
      final List<String> type,
      final boolean lenient) {
    when(job.getOwnerId()).thenReturn(Optional.ofNullable(ownerId));
    final String lenientHeader = "handling=" + lenient;
    when(requestDetails.getHeader("Prefer")).thenReturn(lenientHeader + "," + "prefer-async");
    when(requestDetails.getHeaders("Prefer")).thenReturn(List.of(lenientHeader, "prefer-async"));

    final ExportRequest exportRequest =
        new ExportRequest(
            "test-req",
            "http://localhost:8080/fhir",
            ExportOutputFormat.NDJSON,
            null,
            null,
            type,
            List.of(),
            lenient,
            ExportLevel.SYSTEM,
            Set.of());
    when(job.getPreAsyncValidationResult()).thenReturn((T) exportRequest);

    final Parameters answer =
        exportProvider.export(
            requireNonNull(exportRequest.outputFormat()).toString(),
            exportRequest.since(),
            exportRequest.until(),
            type,
            null,
            requestDetails);
    try {
      // Convert Parameters to JSON using FHIR context.
      final String json = fhirContext.newJsonParser().encodeResourceToString(answer);
      return new ObjectMapper().readTree(json);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected SystemExportProvider setupScenario(final Path tempDir, final String... resourceTypes) {
    TestDataSetup.copyTestDataToTempDir(tempDir, resourceTypes);
    final QueryableDataSource deltaLake =
        new DataSourceBuilder(pathlingContext).delta("file://" + tempDir);

    final ExportExecutor executor =
        new ExportExecutor(
            pathlingContext,
            deltaLake,
            fhirContext,
            sparkSession,
            tempDir.resolve("delta").toString(),
            serverConfiguration,
            patientCompartmentService);

    final ExportOperationHelper helper =
        new ExportOperationHelper(
            executor, jobRegistry, requestTagFactory, exportResultRegistry, serverConfiguration);

    return new SystemExportProvider(exportOperationValidator, helper);
  }

  protected void switchToUser(final String username, final String... authorities) {
    final Jwt jwt =
        Jwt.withTokenValue("mock-token").header("alg", "none").claim("sub", username).build();

    final List<GrantedAuthority> grantedAuthorities =
        AuthorityUtils.createAuthorityList(authorities);

    final JwtAuthenticationToken authentication =
        new JwtAuthenticationToken(jwt, grantedAuthorities);

    SecurityContextHolder.getContext().setAuthentication(authentication);
  }

  /**
   * Extracts output URLs from the Parameters JSON structure. The Parameters resource has a
   * "parameter" array with named entries. This method finds all "output" parameters and extracts
   * their URL parts.
   *
   * @param manifest the Parameters JSON node
   * @return list of output URL strings
   */
  protected List<String> getOutputUrls(final JsonNode manifest) {
    final JsonNode parameters = manifest.get("parameter");
    if (parameters == null || !parameters.isArray()) {
      return List.of();
    }
    return java.util.stream.StreamSupport.stream(parameters.spliterator(), false)
        .filter(p -> "output".equals(p.path("name").asText()))
        .map(p -> p.path("part"))
        .filter(JsonNode::isArray)
        .flatMap(parts -> java.util.stream.StreamSupport.stream(parts.spliterator(), false))
        .filter(part -> "url".equals(part.path("name").asText()))
        .map(part -> part.path("valueUri").asText())
        .filter(url -> !url.isEmpty())
        .toList();
  }
}
