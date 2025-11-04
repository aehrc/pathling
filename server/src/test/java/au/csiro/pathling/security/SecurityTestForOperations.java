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
import au.csiro.pathling.operations.bulkexport.ExportOperationValidator;
import au.csiro.pathling.operations.bulkexport.ExportOutputFormat;
import au.csiro.pathling.operations.bulkexport.ExportProvider;
import au.csiro.pathling.operations.bulkexport.ExportRequest;
import au.csiro.pathling.operations.bulkexport.ExportResult;
import au.csiro.pathling.operations.bulkexport.ExportResultProvider;
import au.csiro.pathling.operations.bulkexport.ExportResultRegistry;
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
import java.util.UUID;
import java.util.function.UnaryOperator;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Binary;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
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

@ActiveProfiles({"core", "server", "unit-test"})
@Tag("IntegrationTest")
@TestPropertySource(properties = {"pathling.async.enabled=false"})
abstract class SecurityTestForOperations<T> extends SecurityTest {

  public static final String ADMIN_USER = "admin";

  public static final UnaryOperator<String> ERROR_MSG = "Missing authority: 'pathling:%s'"::formatted;
  public static final String PATHLING_READ_MSG = ERROR_MSG.apply("read");
  public static final String PATHLING_WRITE_MSG = ERROR_MSG.apply("write");

  protected ExportProvider exportProvider;

  @MockBean
  protected ServletRequestDetails requestDetails;

  @MockBean
  protected JobRegistry jobRegistry;
  @Autowired
  protected RequestTagFactory requestTagFactory;

  @MockBean
  protected Job<T> job;
  @Autowired
  private PathlingContext pathlingContext;
  @Autowired
  private FhirContext fhirContext;
  @Autowired
  private SparkSession sparkSession;
  @Autowired
  private ExportOperationValidator exportOperationValidator;
  @Autowired
  private ServerConfiguration serverConfiguration;
  @Autowired
  private ExportResultRegistry exportResultRegistry;
  @Autowired
  private ExportResultProvider exportResultProvider;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setup() {
    when(requestDetails.getCompleteUrl()).thenReturn("test-url");
    when(requestDetails.getHeaders("Prefer")).thenReturn(List.of("respond-async"));
    when(requestDetails.getHeader("Prefer")).thenReturn("respond-async");
    Authentication auth = SecurityContextHolder.getContext().getAuthentication();
    JobTag tag = requestTagFactory.createTag(requestDetails, auth);
    when(jobRegistry.get(tag)).thenReturn((Job<Object>) job);
    when(job.getId()).thenReturn(UUID.randomUUID().toString());
  }

  MockHttpServletResponse perform_export_result(String jobId, String file,
      @Nullable String ownerId) {
    exportResultRegistry.put(jobId, new ExportResult(Optional.ofNullable(ownerId)));
    MockHttpServletResponse response = new MockHttpServletResponse();
    exportResultProvider.result(jobId, file, response);
    return response;
  }

  JsonNode perform_export() {
    return perform_export(ADMIN_USER, List.of(), false);
  }

  JsonNode perform_lenient_export() {
    return perform_export(ADMIN_USER, List.of(), true);
  }

  JsonNode perform_export(String ownerId) {
    return perform_export(ownerId, List.of(), false);
  }

  JsonNode perform_export(String ownerId, List<String> type, boolean lenient) {
    return perform_export(exportProvider, ownerId, type, lenient);
  }

  @SuppressWarnings("unchecked")
  JsonNode perform_export(ExportProvider exportProvider, String ownerId, List<String> type,
      boolean lenient) {
    when(job.getOwnerId()).thenReturn(Optional.ofNullable(ownerId));
    String lenientHeader = "handling=" + lenient;
    when(requestDetails.getHeader("Prefer")).thenReturn(lenientHeader + "," + "prefer-async");
    when(requestDetails.getHeaders("Prefer")).thenReturn(List.of(lenientHeader, "prefer-async"));

    ExportRequest exportRequest = new ExportRequest(
        "test-req",
        ExportOutputFormat.ND_JSON,
        null,
        null,
        type.stream().map(ResourceType::fromCode).toList(),
        List.of(),
        lenient
    );
    when(job.getPreAsyncValidationResult()).thenReturn((T) exportRequest);

    Binary answer = exportProvider.export(
        exportRequest.outputFormat().toString(),
        exportRequest.since(),
        exportRequest.until(),
        type,
        null,
        requestDetails
    );
    try {
      return new ObjectMapper().readTree(answer.getData());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected ExportProvider setup_scenario(Path tempDir, String... resourceTypes) {
    TestDataSetup.staticCopyTestDataToTempDir(tempDir, resourceTypes);
    QueryableDataSource deltaLake = new DataSourceBuilder(pathlingContext)
        .delta("file://" + tempDir.toString());

    ExportExecutor executor = new ExportExecutor(
        pathlingContext,
        deltaLake,
        fhirContext,
        sparkSession,
        tempDir.resolve("delta").toString(),
        serverConfiguration
    );

    return new ExportProvider(
        executor,
        exportOperationValidator,
        jobRegistry,
        requestTagFactory,
        exportResultRegistry
    );
  }

  protected void switchToUser(String username, String... authorities) {
    Jwt jwt = Jwt.withTokenValue("mock-token")
        .header("alg", "none")
        .claim("sub", username)
        .build();

    List<GrantedAuthority> grantedAuthorities =
        AuthorityUtils.createAuthorityList(authorities);

    JwtAuthenticationToken authentication =
        new JwtAuthenticationToken(jwt, grantedAuthorities);

    SecurityContextHolder.getContext().setAuthentication(authentication);
  }
}
