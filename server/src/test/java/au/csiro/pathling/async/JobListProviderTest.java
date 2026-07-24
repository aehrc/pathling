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

package au.csiro.pathling.async;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.config.AuthorizationConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.errors.AccessDeniedError;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;

/**
 * Tests for {@link JobListProvider}, covering response mapping, ordering, owner filtering and the
 * operation authority check.
 *
 * @author John Grimes
 */
class JobListProviderTest {

  private static final String FHIR_SERVER_BASE = "https://server.example.org/fhir";
  private static final String OWNER_A = "user-a";
  private static final String OWNER_B = "user-b";

  private JobRegistry jobRegistry;
  private ServerConfiguration configuration;
  private AuthorizationConfiguration authConfig;
  private JobListProvider provider;
  private ServletRequestDetails requestDetails;
  private MockHttpServletResponse response;

  @BeforeEach
  void setUp() {
    jobRegistry = new JobRegistry();

    configuration = mock(ServerConfiguration.class);
    authConfig = mock(AuthorizationConfiguration.class);
    when(configuration.getAuth()).thenReturn(authConfig);
    // Auth is disabled by default; individual tests enable it as needed.
    when(authConfig.isEnabled()).thenReturn(false);

    provider = new JobListProvider(configuration, jobRegistry);

    requestDetails = mock(ServletRequestDetails.class);
    when(requestDetails.getFhirServerBase()).thenReturn(FHIR_SERVER_BASE);

    response = new MockHttpServletResponse();
  }

  @AfterEach
  void tearDown() {
    SecurityContextHolder.clearContext();
  }

  // ---------------------------------------------------------------------------
  // Response mapping (auth disabled).
  // ---------------------------------------------------------------------------

  @Test
  void mapsInProgressJobWithAllParts() {
    final CompletableFuture<IBaseResource> future = new CompletableFuture<>();
    final Job<IBaseResource> job = new Job<>(fixedId(), "export", future, Optional.empty());
    job.incrementTotalStages();
    job.incrementTotalStages();
    job.incrementCompletedStages();
    jobRegistry.register(job);

    final Parameters result = provider.jobs(requestDetails, response);
    final List<ParametersParameterComponent> jobs = jobParameters(result);

    assertThat(jobs).hasSize(1);
    final ParametersParameterComponent jobParam = jobs.getFirst();
    assertThat(part(jobParam, "id")).isEqualTo(job.getId());
    assertThat(part(jobParam, "operation")).isEqualTo("export");
    assertThat(part(jobParam, "status")).isEqualTo("in-progress");
    assertThat(part(jobParam, "progress")).isEqualTo("50");
    assertThat(part(jobParam, "startTime")).isNotNull();
    assertThat(part(jobParam, "url")).isEqualTo(FHIR_SERVER_BASE + "/$job?id=" + job.getId());
  }

  @Test
  void mapsCompletedJobWithoutProgress() {
    final CompletableFuture<IBaseResource> future = new CompletableFuture<>();
    future.complete(new Parameters());
    final Job<IBaseResource> job = new Job<>(fixedId(), "import", future, Optional.empty());
    jobRegistry.register(job);

    final ParametersParameterComponent jobParam =
        jobParameters(provider.jobs(requestDetails, response)).getFirst();
    assertThat(part(jobParam, "status")).isEqualTo("completed");
    assertThat(part(jobParam, "progress")).isNull();
  }

  @Test
  void mapsFailedJobWithoutProgress() {
    final CompletableFuture<IBaseResource> future = new CompletableFuture<>();
    future.completeExceptionally(new RuntimeException("boom"));
    final Job<IBaseResource> job = new Job<>(fixedId(), "export", future, Optional.empty());
    jobRegistry.register(job);

    final ParametersParameterComponent jobParam =
        jobParameters(provider.jobs(requestDetails, response)).getFirst();
    assertThat(part(jobParam, "status")).isEqualTo("failed");
    assertThat(part(jobParam, "progress")).isNull();
  }

  @Test
  void omitsProgressForInProgressJobWithNoKnownStages() {
    // An in-progress job with zero total stages must not divide by zero, and must omit progress.
    final CompletableFuture<IBaseResource> future = new CompletableFuture<>();
    final Job<IBaseResource> job = new Job<>(fixedId(), "export", future, Optional.empty());
    jobRegistry.register(job);

    final ParametersParameterComponent jobParam =
        jobParameters(provider.jobs(requestDetails, response)).getFirst();
    assertThat(part(jobParam, "status")).isEqualTo("in-progress");
    assertThat(part(jobParam, "progress")).isNull();
  }

  @Test
  void returnsEmptyParametersWhenNoJobs() {
    final Parameters result = provider.jobs(requestDetails, response);
    assertThat(jobParameters(result)).isEmpty();
  }

  @Test
  void ordersJobsNewestFirst() {
    // Three real jobs registered in sequence; the response must be sorted by start time descending.
    final List<Job<?>> created =
        List.of(
            registerRunningJob("export"),
            registerRunningJob("import"),
            registerRunningJob("export"));

    final List<String> expectedIdsNewestFirst =
        created.stream()
            .sorted((left, right) -> right.getStartTime().compareTo(left.getStartTime()))
            .map(Job::getId)
            .toList();

    final List<String> actualIds =
        jobParameters(provider.jobs(requestDetails, response)).stream()
            .map(jobParam -> part(jobParam, "id"))
            .toList();

    assertThat(actualIds).isEqualTo(expectedIdsNewestFirst);
    // The most recently created job must appear first.
    assertThat(actualIds.getFirst()).isEqualTo(created.getLast().getId());
  }

  @Test
  void returnsAllJobsWhenAuthDisabled() {
    registerRunningJob("export", Optional.of(OWNER_A));
    registerRunningJob("import", Optional.of(OWNER_B));
    registerRunningJob("export", Optional.empty());

    assertThat(jobParameters(provider.jobs(requestDetails, response))).hasSize(3);
  }

  // ---------------------------------------------------------------------------
  // Owner filtering and authority (auth enabled).
  // ---------------------------------------------------------------------------

  @Test
  void filtersToCallerOwnedJobsWhenAuthEnabled() {
    when(authConfig.isEnabled()).thenReturn(true);
    final Job<?> ownedByA = registerRunningJob("export", Optional.of(OWNER_A));
    registerRunningJob("import", Optional.of(OWNER_B));
    setAuthenticatedUser(OWNER_A, "pathling:jobs");

    final List<ParametersParameterComponent> jobs =
        jobParameters(provider.jobs(requestDetails, response));

    assertThat(jobs).hasSize(1);
    assertThat(part(jobs.getFirst(), "id")).isEqualTo(ownedByA.getId());
  }

  @Test
  void returnsEmptyListWhenCallerHasNoSubject() {
    when(authConfig.isEnabled()).thenReturn(true);
    registerRunningJob("export", Optional.of(OWNER_A));
    registerRunningJob("import", Optional.empty());
    setAuthenticatedUserWithoutSubject("pathling:jobs");

    assertThat(jobParameters(provider.jobs(requestDetails, response))).isEmpty();
  }

  @Test
  void rejectsCallerWithoutJobsAuthorityWhenAuthEnabled() {
    when(authConfig.isEnabled()).thenReturn(true);
    registerRunningJob("export", Optional.of(OWNER_A));
    setAuthenticatedUser(OWNER_A, "pathling:export");

    assertThatThrownBy(() -> provider.jobs(requestDetails, response))
        .isInstanceOf(AccessDeniedError.class);
  }

  @Test
  void allowsCallerWithJobsAuthorityWhenAuthEnabled() {
    when(authConfig.isEnabled()).thenReturn(true);
    registerRunningJob("export", Optional.of(OWNER_A));
    setAuthenticatedUser(OWNER_A, "pathling:jobs");

    assertThatCode(() -> provider.jobs(requestDetails, response)).doesNotThrowAnyException();
  }

  // ---------------------------------------------------------------------------
  // Helpers.
  // ---------------------------------------------------------------------------

  @Nonnull
  private static String fixedId() {
    return UUID.randomUUID().toString();
  }

  @Nonnull
  private Job<?> registerRunningJob(@Nonnull final String operation) {
    return registerRunningJob(operation, Optional.empty());
  }

  @Nonnull
  private Job<?> registerRunningJob(
      @Nonnull final String operation, @Nonnull final Optional<String> ownerId) {
    final CompletableFuture<IBaseResource> future = new CompletableFuture<>();
    final Job<IBaseResource> job = new Job<>(fixedId(), operation, future, ownerId);
    jobRegistry.register(job);
    return job;
  }

  @Nonnull
  private static List<ParametersParameterComponent> jobParameters(
      @Nonnull final Parameters result) {
    return result.getParameter().stream().filter(param -> "job".equals(param.getName())).toList();
  }

  private static String part(
      @Nonnull final ParametersParameterComponent jobParam, @Nonnull final String name) {
    return jobParam.getPart().stream()
        .filter(part -> name.equals(part.getName()))
        .map(part -> part.getValue().primitiveValue())
        .findFirst()
        .orElse(null);
  }

  private void setAuthenticatedUser(
      @Nonnull final String username, @Nonnull final String... authorities) {
    final Jwt jwt =
        Jwt.withTokenValue("mock-token").header("alg", "none").claim("sub", username).build();
    setAuthentication(jwt, authorities);
  }

  private void setAuthenticatedUserWithoutSubject(@Nonnull final String... authorities) {
    // A token that carries a claim but no subject, mimicking a client-credentials token.
    final Jwt jwt =
        Jwt.withTokenValue("mock-token").header("alg", "none").claim("scope", "system").build();
    setAuthentication(jwt, authorities);
  }

  private void setAuthentication(@Nonnull final Jwt jwt, @Nonnull final String... authorities) {
    final List<GrantedAuthority> grantedAuthorities =
        AuthorityUtils.createAuthorityList(authorities);
    SecurityContextHolder.getContext()
        .setAuthentication(new JwtAuthenticationToken(jwt, grantedAuthorities));
  }
}
