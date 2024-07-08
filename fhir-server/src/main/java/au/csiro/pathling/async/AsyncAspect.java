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

import static au.csiro.pathling.security.SecurityAspect.getCurrentUserId;

import au.csiro.pathling.errors.DiagnosticContext;
import au.csiro.pathling.errors.ErrorHandlingInterceptor;
import au.csiro.pathling.errors.ErrorReportingInterceptor;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.OperationOutcome.OperationOutcomeIssueComponent;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.core.annotation.Order;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

/**
 * Intercepts calls to methods annotated with {@link AsyncSupported} to run them asynchronously,
 * allowing the client to poll a job endpoint for progress and the final result.
 *
 * @author John Grimes
 */
@Aspect
@Component
@Profile("server")
@ConditionalOnProperty(prefix = "pathling", name = "async.enabled", havingValue = "true")
@Slf4j
@Order(200)
public class AsyncAspect {

  private static final String ASYNC_HEADER = "Prefer";
  private static final String ASYNC_HEADER_VALUE = "respond-async";

  @Nonnull
  private final ThreadPoolTaskExecutor executor;

  @Nonnull
  private final JobRegistry jobRegistry;

  @Nonnull
  private final RequestTagFactory requestTagFactory;

  @Nonnull
  private final Map<RequestTag, Job> requestTagToJob = new ConcurrentHashMap<>();

  @Nonnull
  private final StageMap stageMap;

  @Nonnull
  private final SparkSession spark;

  /**
   * @param executor used to run asynchronous jobs in the background
   * @param requestTagFactory used to create {@link RequestTag} instances
   * @param jobRegistry the {@link JobRegistry} used to keep track of running jobs
   * @param stageMap the {@link StageMap} used to map stages to job IDs
   * @param spark used for updating the Spark Context with job identity
   */
  public AsyncAspect(@Nonnull final ThreadPoolTaskExecutor executor,
      @Nonnull final RequestTagFactory requestTagFactory,
      @Nonnull final JobRegistry jobRegistry, @Nonnull final StageMap stageMap,
      @Nonnull final SparkSession spark) {
    this.executor = executor;
    this.requestTagFactory = requestTagFactory;
    this.jobRegistry = jobRegistry;
    this.stageMap = stageMap;
    this.spark = spark;
  }

  @Around("@annotation(asyncSupported)")
  protected IBaseResource maybeExecuteAsynchronously(@Nonnull final ProceedingJoinPoint joinPoint,
      @Nonnull final AsyncSupported asyncSupported) throws Throwable {
    final Object[] args = joinPoint.getArgs();
    final ServletRequestDetails requestDetails = getServletRequestDetails(args);
    final HttpServletRequest request = requestDetails.getServletRequest();
    final String prefer = request.getHeader(ASYNC_HEADER);

    if (prefer != null && prefer.equals(ASYNC_HEADER_VALUE)) {
      log.info("Asynchronous processing requested");
      processRequestAsynchronously(joinPoint, requestDetails, spark);
      throw new ProcessingNotCompletedException("Accepted", buildOperationOutcome());
    } else {
      return (IBaseResource) joinPoint.proceed();
    }
  }

  private void processRequestAsynchronously(@Nonnull final ProceedingJoinPoint joinPoint,
      @Nonnull final ServletRequestDetails requestDetails,
      @Nonnull final SparkSession spark) {

    final Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
    final RequestTag requestTag = requestTagFactory.createTag(requestDetails, authentication);
    final Job job = jobRegistry.getOrCreate(requestTag, jobId -> {
      final DiagnosticContext diagnosticContext = DiagnosticContext.fromSentryScope();
      final String operation = requestDetails.getOperation().replaceFirst("\\$", "");
      final Future<IBaseResource> result = executor.submit(() -> {
        try {
          diagnosticContext.configureScope(true);
          SecurityContextHolder.getContext().setAuthentication(authentication);
          spark.sparkContext().setJobGroup(jobId, jobId, true);
          return (IBaseResource) joinPoint.proceed();
        } catch (final Throwable e) {
          // Unwrap the actual exception from the aspect proxy wrapper, if needed.
          final Throwable actualEx = unwrapFromProxy(e);

          // Apply the same processing and filtering as we do for synchronous requests.
          final BaseServerResponseException convertedError = ErrorHandlingInterceptor.convertError(
              actualEx);
          ErrorReportingInterceptor.reportExceptionToSentry(convertedError);
          if (ErrorReportingInterceptor.isReportableException(convertedError)) {
            log.error("Unexpected exception in asynchronous execution.",
                ErrorReportingInterceptor.getReportableError(convertedError));
          } else {
            log.warn("Asynchronous execution failed: {}.",
                ErrorReportingInterceptor.getReportableError(convertedError).getMessage());
          }
          throw new RuntimeException("Problem processing request asynchronously", actualEx);
        } finally {
          cleanUpAfterJob(spark, jobId);
        }
      });
      final Optional<String> ownerId = getCurrentUserId(authentication);
      return new Job(jobId, operation, result, ownerId);
    });

    final HttpServletResponse response = requestDetails.getServletResponse();
    response.setHeader("Content-Location",
        requestDetails.getFhirServerBase() + "/$job?id=" + job.getId());
  }

  @Nonnull
  private HttpServletRequest getRequest(@Nonnull final Object[] args) {
    return (HttpServletRequest) Arrays.stream(args)
        .filter(a -> a instanceof HttpServletRequest)
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException(
            "Method annotated with @AsyncSupported must include a HttpServletRequest parameter"));
  }

  @Nonnull
  private ServletRequestDetails getServletRequestDetails(@Nonnull final Object[] args) {
    return (ServletRequestDetails) Arrays.stream(args)
        .filter(a -> a instanceof ServletRequestDetails)
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException(
            "Method annotated with @AsyncSupported must include a ServletRequestDetails parameter"));
  }

  private void cleanUpAfterJob(@Nonnull final SparkSession spark, @Nonnull final String requestId) {
    spark.sparkContext().clearJobGroup();
    // Clean up the stage mappings.
    final List<Integer> keys = stageMap.entrySet().stream()
        .filter(e -> requestId.equals(e.getValue()))
        .map(Entry::getKey)
        .collect(Collectors.toList());
    stageMap.keySet().removeAll(keys);
    // We can't clean up the entry in the job registry, it needs to stay there so that clients can
    // retrieve the result of completed jobs.
  }

  @Nonnull
  private static Throwable unwrapFromProxy(@Nonnull final Throwable ex) {
    return ex instanceof UndeclaredThrowableException
           ? ((UndeclaredThrowableException) ex).getUndeclaredThrowable()
           : ex;
  }

  @Nonnull
  private static OperationOutcome buildOperationOutcome() {
    final OperationOutcome opOutcome = new OperationOutcome();
    final OperationOutcomeIssueComponent issue = new OperationOutcomeIssueComponent();
    issue.setCode(IssueType.INFORMATIONAL);
    issue.setSeverity(IssueSeverity.INFORMATION);
    issue.setDiagnostics("Job accepted for processing, see the Content-Location header for the "
        + "URL at which status can be queried");
    opOutcome.addIssue(issue);
    return opOutcome;
  }
}
