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

package au.csiro.pathling.async;

import au.csiro.pathling.cache.CacheableDatabase;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.test.SpringBootUnitTest;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.SparkSession;
import org.aspectj.lang.ProceedingJoinPoint;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static au.csiro.pathling.async.RequestTagFactoryTest.createServerConfiguration;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SpringBootUnitTest
public class AsyncAspectTest {

  @MockBean
  private ThreadPoolTaskExecutor threadPoolTaskExecutor;

  @MockBean
  private CacheableDatabase database;

  @MockBean
  StageMap stageMap;

  @MockBean
  ProceedingJoinPoint proceedingJoinPoint;

  @Autowired
  SparkSession spark;

  private JobRegistry jobRegistry;
  private AsyncAspect asyncAspect;

  private final MockHttpServletRequest servletRequest = new MockHttpServletRequest();
  private final MockHttpServletResponse servletResponse = new MockHttpServletResponse();
  private ServletRequestDetails requestDetails;

  private static final IBaseResource RESULT_RESOURCE = mock(IBaseResource.class);
  private static final AsyncSupported ASYNC_SUPPORTED = mock(AsyncSupported.class);
  private static final String FHIR_SERVER_BASE = "http://localhost:8080/fhir";

  // regular expression that matches content location header
  private static final Pattern CONTENT_LOCATION_REGEX = Pattern.compile(
      "([^?]+)\\?id=([\\w\\-]{36})");

  @BeforeEach
  public void setUp() throws Throwable {

    // Wire the asynAspects and it's dependencied
    final ServerConfiguration serverConfiguration = createServerConfiguration(
        List.of("Accept", "Authorization"),
        List.of("Accept"));
    final RequestTagFactory requestTagFactory = new RequestTagFactory(database,
        serverConfiguration);
    jobRegistry = new JobRegistry();
    asyncAspect = new AsyncAspect(threadPoolTaskExecutor, requestTagFactory, jobRegistry, stageMap,
        spark);

    // Initialise mock request and response
    requestDetails = new ServletRequestDetails();
    requestDetails.setServletRequest(servletRequest);
    requestDetails.setServletResponse(servletResponse);
    requestDetails.setFhirServerBase(FHIR_SERVER_BASE);
    requestDetails.setCompleteUrl(FHIR_SERVER_BASE + "/Patient/$aggregate?param=value1");
    requestDetails.setOperation("$aggregate");

    final Object[] args = new Object[]{requestDetails};
    when(proceedingJoinPoint.getArgs()).thenReturn(args);
    when(proceedingJoinPoint.proceed()).thenReturn(RESULT_RESOURCE);
  }

  @Nonnull
  IBaseResource executeRequest() throws Throwable {
    return asyncAspect.maybeExecuteAsynchronously(
        proceedingJoinPoint, ASYNC_SUPPORTED);
  }

  void setAuthenticationPrincipal(@Nonnull final Object principal) {
    // SecurityContextHolder.getContext()
    //     .setAuthentication(new TestingAuthenticationToken(principal, ""));
  }

  @Nonnull
  String assertExecutedAsync() {
    servletRequest.removeHeader("Prefer");
    servletRequest.addHeader("Prefer", "respond-async");
    final ProcessingNotCompletedException ex = assertThrows(ProcessingNotCompletedException.class,
        this::executeRequest);
    assertEquals(202, ex.getStatusCode());
    final String contentLocation = (String) servletResponse.getHeaderValue("Content-Location");
    assertNotNull(contentLocation);
    final Matcher matcher = CONTENT_LOCATION_REGEX.matcher(contentLocation);
    assertTrue(matcher.matches());
    final String requestUrl = matcher.group(1);
    assertEquals(FHIR_SERVER_BASE + "/$job", requestUrl);
    // return job id
    return matcher.group(2);
  }


  @Test
  public void testSynchronousRequestReturnsExpectedResponse() throws Throwable {
    final IBaseResource result = executeRequest();
    assertEquals(RESULT_RESOURCE, result);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAsyncRequestsSchedulesNewJob() {
    // setup thread pool executor to return a mock future
    final Future<IBaseResource> mockFuture = mock(Future.class);
    when(threadPoolTaskExecutor.submit(ArgumentMatchers.<Callable<IBaseResource>>any())).thenReturn(
        mockFuture);

    // setup authentication principal
    // final JwtClaimAccessor mockJwtPrincipal = mock(JwtClaimAccessor.class);
    // when(mockJwtPrincipal.getSubject()).thenReturn("subject1");
    // setAuthenticationPrincipal(mockJwtPrincipal);

    final String jobId = assertExecutedAsync();
    final Job newJob = jobRegistry.get(jobId);
    assertNotNull(newJob);
    assertEquals(jobId, newJob.getId());
    assertEquals(mockFuture, newJob.getResult());
    assertEquals("aggregate", newJob.getOperation());
    // TODO - this assertion fails because auth not implemented yet
    // assertEquals(Optional.of("subject1"), newJob.getOwnerId());
  }

  @Test
  public void testReusesAsynJobIfOnlyWhitelistedHeadersChange() {
    setAuthenticationPrincipal("principal1");
    servletRequest.addHeader("Accept", "value1");
    assertEquals("value1", servletRequest.getHeader("Accept"));
    final String jobId1 = assertExecutedAsync();

    setAuthenticationPrincipal("principal1");
    servletRequest.removeHeader("Accept");
    servletRequest.addHeader("Accept", "value2");
    assertEquals("value2", servletRequest.getHeader("Accept"));
    final String jobId2 = assertExecutedAsync();

    assertEquals(jobId1, jobId2);
  }

  @Test
  public void testCreatesNewAsyncJobWhenSalientHeaderChanges() {
    servletRequest.addHeader("Authorization", "value1");
    assertEquals("value1", servletRequest.getHeader("Authorization"));
    final String jobId1 = assertExecutedAsync();

    servletRequest.removeHeader("Authorization");
    servletRequest.addHeader("Authorization", "value2");
    assertEquals("value2", servletRequest.getHeader("Authorization"));
    final String jobId2 = assertExecutedAsync();
    assertNotEquals(jobId1, jobId2);
  }

  @Test
  public void testCreatesNewAsyncJobWhenDatabaseVersionChanges() {
    when(database.getCacheKey()).thenReturn(Optional.of("key1"));
    final String jobId1 = assertExecutedAsync();
    when(database.getCacheKey()).thenReturn(Optional.of("key2"));
    final String jobId2 = assertExecutedAsync();
    assertNotEquals(jobId1, jobId2);
  }

  @Test
  public void testCreatesNewAsyncJobWhenQueryStringChanges() {
    requestDetails.setCompleteUrl(FHIR_SERVER_BASE + "/Patient/$aggregate?param=value1");
    final String jobId1 = assertExecutedAsync();
    requestDetails.setCompleteUrl(FHIR_SERVER_BASE + "/Patient/$aggregate?param=value2");
    final String jobId2 = assertExecutedAsync();
    assertNotEquals(jobId1, jobId2);
  }

  @Test
  public void testReusesAsyncJobWhenAuthenticationPrincipalChanges() {
    setAuthenticationPrincipal("principal1");
    final String jobId1 = assertExecutedAsync();
    setAuthenticationPrincipal("principal2");
    final String jobId2 = assertExecutedAsync();
    assertEquals(jobId1, jobId2);
  }
}
