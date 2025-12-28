/*
 * Copyright 2025 Commonwealth Scientific and Industrial Research
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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.config.AuthorizationConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.errors.AccessDeniedError;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;

/**
 * Tests for JobProvider ownership validation.
 *
 * @author John Grimes
 */
class JobProviderSecurityTest {

  private static final String OWNER_USER = "owner-user";
  private static final String OTHER_USER = "other-user";

  private JobRegistry jobRegistry;
  private JobProvider jobProvider;
  private MockHttpServletRequest request;
  private MockHttpServletResponse response;

  @BeforeEach
  void setUp() {
    jobRegistry = new JobRegistry();

    // Create ServerConfiguration with auth enabled.
    final ServerConfiguration serverConfiguration = mock(ServerConfiguration.class);
    final AuthorizationConfiguration authConfig = mock(AuthorizationConfiguration.class);
    when(serverConfiguration.getAuth()).thenReturn(authConfig);
    when(authConfig.isEnabled()).thenReturn(true);

    // Create mock SparkSession.
    final SparkSession sparkSession = mock(SparkSession.class);

    jobProvider = new JobProvider(serverConfiguration, jobRegistry, sparkSession, "/tmp/test");

    request = new MockHttpServletRequest();
    response = new MockHttpServletResponse();
  }

  @Test
  void rejectsAccessWhenDifferentUserPollsJob() {
    // Create a job owned by OWNER_USER.
    final String jobId = UUID.randomUUID().toString();
    final CompletableFuture<IBaseResource> future = new CompletableFuture<>();
    final Job<IBaseResource> job = new Job<>(jobId, "export", future, Optional.of(OWNER_USER));
    jobRegistry.register(job);

    // Switch to OTHER_USER with valid export authority.
    setAuthenticatedUser(OTHER_USER, "pathling:export");

    // Attempt to poll the job - should be rejected.
    assertThatThrownBy(() -> jobProvider.job(jobId, request, response))
        .isExactlyInstanceOf(AccessDeniedError.class)
        .hasMessage("The requested job is not owned by the current user");
  }

  @Test
  void allowsAccessWhenSameUserPollsJob() {
    // Create a job owned by OWNER_USER.
    final String jobId = UUID.randomUUID().toString();
    final CompletableFuture<IBaseResource> future = new CompletableFuture<>();
    final Job<IBaseResource> job = new Job<>(jobId, "export", future, Optional.of(OWNER_USER));
    jobRegistry.register(job);

    // Authenticate as OWNER_USER with valid export authority.
    setAuthenticatedUser(OWNER_USER, "pathling:export");

    // Attempt to poll the job - should succeed (throws ProcessingNotCompletedException since job
    // is not done, not AccessDeniedError).
    assertThatThrownBy(() -> jobProvider.job(jobId, request, response))
        .isInstanceOf(ProcessingNotCompletedException.class);
  }

  private void setAuthenticatedUser(
      @Nonnull final String username, @Nonnull final String... authorities) {
    final Jwt jwt =
        Jwt.withTokenValue("mock-token").header("alg", "none").claim("sub", username).build();
    final List<GrantedAuthority> grantedAuthorities =
        AuthorityUtils.createAuthorityList(authorities);
    final JwtAuthenticationToken authentication =
        new JwtAuthenticationToken(jwt, grantedAuthorities);
    SecurityContextHolder.getContext().setAuthentication(authentication);
  }
}
