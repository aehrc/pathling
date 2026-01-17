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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.config.AsyncConfiguration;
import au.csiro.pathling.config.AuthorizationConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Parameters;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

/**
 * Tests for {@link JobProvider} cache header behaviour.
 *
 * @author John Grimes
 */
@Tag("UnitTest")
class JobProviderTest {

  private static final String JOB_ID = "abc12345-1234-1234-8234-123456789012";

  private JobRegistry jobRegistry;
  private JobProvider jobProvider;
  private MockHttpServletRequest request;
  private MockHttpServletResponse response;

  @BeforeEach
  void setUp() {
    jobRegistry = new JobRegistry();
    final ServerConfiguration config = mock(ServerConfiguration.class);
    final AuthorizationConfiguration authConfig = mock(AuthorizationConfiguration.class);
    when(config.getAuth()).thenReturn(authConfig);
    when(authConfig.isEnabled()).thenReturn(false);

    final AsyncConfiguration asyncConfig = mock(AsyncConfiguration.class);
    // Configure a 60-second max-age for testing.
    when(asyncConfig.getCacheMaxAge()).thenReturn(60);
    when(config.getAsync()).thenReturn(asyncConfig);

    final SparkSession spark = mock(SparkSession.class);
    jobProvider = new JobProvider(config, jobRegistry, spark, "/tmp/test");
    request = new MockHttpServletRequest();
    request.setMethod("GET");
    response = new MockHttpServletResponse();
  }

  @Test
  void completedJobSetsCacheControlWithMaxAge() {
    // Completed job responses should have Cache-Control: max-age=60.
    final CompletableFuture<IBaseResource> future =
        CompletableFuture.completedFuture(new Parameters());
    final Job<IBaseResource> job = new Job<>(JOB_ID, "export", future, Optional.empty());
    jobRegistry.register(job);

    jobProvider.job(JOB_ID, request, response);

    assertThat(response.getHeader("Cache-Control")).isEqualTo("max-age=60");
  }

  @Test
  void inProgressJobSetsCacheControlWithMaxAge() {
    // In-progress job responses should also have Cache-Control: max-age=60.
    final CompletableFuture<IBaseResource> future = new CompletableFuture<>();
    final Job<IBaseResource> job = new Job<>(JOB_ID, "export", future, Optional.empty());
    jobRegistry.register(job);

    assertThatThrownBy(() -> jobProvider.job(JOB_ID, request, response))
        .isInstanceOf(ProcessingNotCompletedException.class);

    assertThat(response.getHeader("Cache-Control")).isEqualTo("max-age=60");
  }

  @Test
  void jobResponseHasNoEtag() {
    // Job responses should not set ETag header.
    final CompletableFuture<IBaseResource> future =
        CompletableFuture.completedFuture(new Parameters());
    final Job<IBaseResource> job = new Job<>(JOB_ID, "export", future, Optional.empty());
    jobRegistry.register(job);

    jobProvider.job(JOB_ID, request, response);

    assertThat(response.getHeader("ETag")).isNull();
  }
}
