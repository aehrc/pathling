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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import au.csiro.pathling.async.AsyncJobContext;
import au.csiro.pathling.async.Job;
import au.csiro.pathling.async.JobRegistry;
import au.csiro.pathling.async.PreAsyncValidation;
import au.csiro.pathling.async.RequestTagFactory;
import au.csiro.pathling.config.AuthorizationConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.operations.bulkexport.ExportResultRegistry;
import au.csiro.pathling.operations.export.ExportFileWriter;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;

/**
 * Unit tests for the job ownership check that {@link SqlExportSupport#runExport} applies before an
 * export runs. The job is supplied through the async job context, as it is when the export runs on
 * the async worker.
 *
 * @author John Grimes
 */
class SqlExportSupportOwnershipTest {

  private static final String JOB_ID = "abc12345-1234-1234-8234-123456789012";

  private SqlExportExecutor executor;
  private ExportResultRegistry exportResultRegistry;
  private AuthorizationConfiguration authConfig;
  private SqlExportSupport support;

  @BeforeEach
  void setUp() {
    executor = mock(SqlExportExecutor.class);
    exportResultRegistry = mock(ExportResultRegistry.class);
    final ServerConfiguration serverConfiguration = mock(ServerConfiguration.class);
    authConfig = mock(AuthorizationConfiguration.class);
    when(serverConfiguration.getAuth()).thenReturn(authConfig);
    support =
        new SqlExportSupport(
            executor,
            mock(JobRegistry.class),
            mock(RequestTagFactory.class),
            exportResultRegistry,
            serverConfiguration,
            mock(ExportFileWriter.class));
  }

  @AfterEach
  void tearDown() {
    AsyncJobContext.clear();
    SecurityContextHolder.clearContext();
  }

  @Test
  void rejectsCallerWithoutSubjectWhenAuthEnabled() {
    // A job started by a caller whose token had no subject records no owner. A caller without a
    // subject must not be treated as that owner, so nothing is exported (aehrc/pathling#2986).
    when(authConfig.isEnabled()).thenReturn(true);
    startJob(Optional.empty());
    authenticate(
        Jwt.withTokenValue("token").header("alg", "none").claim("scope", "system").build());

    final ServletRequestDetails requestDetails = mock(ServletRequestDetails.class);
    final PreAsyncValidation<SqlExportRequest> validation = validation();
    assertThatThrownBy(() -> support.runExport(requestDetails, validation))
        .isExactlyInstanceOf(AccessDeniedError.class)
        .hasMessageStartingWith("The requested result is not owned by the current user");
    verifyNoInteractions(executor);
    verify(exportResultRegistry, never()).put(any(), any());
  }

  @Test
  void runsForOwnerWhenAuthEnabled() {
    // The caller whose subject is recorded as the job's owner may run the export.
    when(authConfig.isEnabled()).thenReturn(true);
    final SqlExportRequest request = startJob(Optional.of("alice"));
    authenticate(Jwt.withTokenValue("token").header("alg", "none").claim("sub", "alice").build());
    when(executor.execute(request, JOB_ID)).thenReturn(List.of());

    assertThat(support.runExport(mock(ServletRequestDetails.class), validation())).isNotNull();
    verify(executor).execute(request, JOB_ID);
  }

  @Test
  void runsWithoutSubjectWhenAuthDisabled() {
    // With authorisation disabled no ownership is enforced, and the caller has no token at all.
    when(authConfig.isEnabled()).thenReturn(false);
    final SqlExportRequest request = startJob(Optional.empty());
    when(executor.execute(request, JOB_ID)).thenReturn(List.of());

    assertThat(support.runExport(mock(ServletRequestDetails.class), validation())).isNotNull();
    verify(executor).execute(request, JOB_ID);
  }

  /**
   * Places a running job with the given owner in the async job context.
   *
   * @param ownerId the owner recorded against the job
   * @return the request the job will export
   */
  @Nonnull
  private static SqlExportRequest startJob(@Nonnull final Optional<String> ownerId) {
    final SqlExportRequest request =
        new SqlExportRequest(
            "http://localhost/fhir/$sql-export",
            "http://localhost/fhir",
            List.of(),
            null,
            SqlExportFormat.NDJSON,
            false,
            Set.of(),
            null);
    final Job<SqlExportRequest> job =
        new Job<>(JOB_ID, "sql-export", new CompletableFuture<>(), ownerId);
    job.setPreAsyncValidationResult(request);
    AsyncJobContext.setCurrentJob(job);
    return request;
  }

  @SuppressWarnings("unchecked")
  @Nonnull
  private static PreAsyncValidation<SqlExportRequest> validation() {
    // The job comes from the async context, so the fallback lookup never consults this.
    return mock(PreAsyncValidation.class);
  }

  private static void authenticate(@Nonnull final Jwt jwt) {
    SecurityContextHolder.getContext()
        .setAuthentication(
            new JwtAuthenticationToken(
                jwt, AuthorityUtils.createAuthorityList("pathling:sql-export")));
  }
}
