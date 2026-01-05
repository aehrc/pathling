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

import static au.csiro.pathling.security.SecurityAspect.getCurrentUserId;

import au.csiro.pathling.FhirServer;
import au.csiro.pathling.async.PreAsyncValidation.PreAsyncValidationResult;
import au.csiro.pathling.errors.DiagnosticContext;
import au.csiro.pathling.errors.ErrorHandlingInterceptor;
import au.csiro.pathling.errors.ErrorReportingInterceptor;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
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
@ConditionalOnProperty(prefix = "pathling", name = "async.enabled", havingValue = "true")
@Slf4j
@Order(200)
public class AsyncAspect {

  @Nonnull private final ThreadPoolTaskExecutor executor;

  @Nonnull private final JobRegistry jobRegistry;

  @Nonnull private final RequestTagFactory requestTagFactory;

  @Nonnull private final Map<RequestTag, Job<?>> requestTagToJob = new ConcurrentHashMap<>();

  @Nonnull private final StageMap stageMap;

  @Nonnull private final SparkSession spark;
  private final JobProvider jobProvider;

  /**
   * @param executor used to run asynchronous jobs in the background
   * @param requestTagFactory used to create {@link RequestTag} instances
   * @param jobRegistry the {@link JobRegistry} used to keep track of running jobs
   * @param stageMap the {@link StageMap} used to map stages to job IDs
   * @param spark used for updating the Spark Context with job identity
   */
  public AsyncAspect(
      @Nonnull final ThreadPoolTaskExecutor executor,
      @Nonnull final RequestTagFactory requestTagFactory,
      @Nonnull final JobRegistry jobRegistry,
      @Nonnull final StageMap stageMap,
      @Nonnull final SparkSession spark,
      JobProvider jobProvider) {
    this.executor = executor;
    this.requestTagFactory = requestTagFactory;
    this.jobRegistry = jobRegistry;
    this.stageMap = stageMap;
    this.spark = spark;
    this.jobProvider = jobProvider;
  }

  @Around("@annotation(asyncSupported)")
  protected IBaseResource maybeExecuteAsynchronously(
      @Nonnull final ProceedingJoinPoint joinPoint, @Nonnull final AsyncSupported asyncSupported)
      throws Throwable {
    final Object[] args = joinPoint.getArgs();
    final ServletRequestDetails requestDetails = getServletRequestDetails(args);

    // Run some validation in sync before (and let validation modify headers if desired)
    Object target = joinPoint.getTarget();
    PreAsyncValidationResult<?> result = null;
    if (target instanceof PreAsyncValidation<?> preAsyncValidation) {
      try {
        result =
            preAsyncValidation.preAsyncValidate(
                requestDetails, Arrays.copyOf(args, args.length - 1));
      } catch (InvalidRequestException preValidationException) {
        throw ErrorHandlingInterceptor.convertError(preValidationException);
      }
    }
    if (FhirServer.PREFER_RESPOND_TYPE_HEADER.validValue(requestDetails)) {
      log.info("Asynchronous processing requested");

      if (result == null) {
        // the class containing the async annotation on a method does not implement
        // PreAsyncValidation
        // set some values to prevent NPEs
        result = new PreAsyncValidationResult<>(new Object(), List.of());
      }
      processRequestAsynchronously(joinPoint, requestDetails, result, spark);
      throw new ProcessingNotCompletedException("Accepted", buildOperationOutcome(result));
    } else {
      return (IBaseResource) joinPoint.proceed();
    }
  }

  private void processRequestAsynchronously(
      @Nonnull final ProceedingJoinPoint joinPoint,
      @Nonnull final ServletRequestDetails requestDetails,
      @Nonnull PreAsyncValidationResult<?> preAsyncValidationResult,
      @Nonnull final SparkSession spark) {
    final Authentication authentication = SecurityContextHolder.getContext().getAuthentication();

    // Compute operation-specific cache key if the operation provides it.
    String operationCacheKey = "";
    final Object target = joinPoint.getTarget();
    if (target instanceof PreAsyncValidation<?> && preAsyncValidationResult.result() != null) {
      @SuppressWarnings("unchecked")
      final PreAsyncValidation<Object> preAsyncValidation = (PreAsyncValidation<Object>) target;
      operationCacheKey =
          preAsyncValidation.computeCacheKeyComponent(preAsyncValidationResult.result());
    }

    final RequestTag requestTag =
        requestTagFactory.createTag(requestDetails, authentication, operationCacheKey);
    final Job<?> job =
        jobRegistry.getOrCreate(
            requestTag,
            jobId -> {
              final DiagnosticContext diagnosticContext = DiagnosticContext.fromSentryScope();
              final String operation = requestDetails.getOperation().replaceFirst("\\$", "");
              final Future<IBaseResource> result =
                  executor.submit(
                      () -> {
                        try {
                          diagnosticContext.configureScope(true);
                          SecurityContextHolder.getContext().setAuthentication(authentication);
                          spark.sparkContext().setJobGroup(jobId, jobId, true);

                          // Set the current job in the async context so that the operation can
                          // access it without
                          // needing to look it up from the servlet request (which may have been
                          // recycled).
                          final Job<?> currentJob = jobRegistry.get(jobId);
                          if (currentJob != null) {
                            AsyncJobContext.setCurrentJob(currentJob);
                          }

                          return (IBaseResource) joinPoint.proceed();
                        } catch (final Throwable e) {
                          // Unwrap the actual exception from the aspect proxy wrapper, if needed.
                          final Throwable actualEx = unwrapFromProxy(e);

                          // Apply the same processing and filtering as we do for synchronous
                          // requests.
                          final BaseServerResponseException convertedError =
                              ErrorHandlingInterceptor.convertError(actualEx);
                          ErrorReportingInterceptor.reportExceptionToSentry(convertedError);
                          if (ErrorReportingInterceptor.isReportableException(convertedError)) {
                            log.error(
                                "Unexpected exception in asynchronous execution.",
                                ErrorReportingInterceptor.getReportableError(convertedError));
                          } else {
                            log.warn(
                                "Asynchronous execution failed: {}.",
                                ErrorReportingInterceptor.getReportableError(convertedError)
                                    .getMessage());
                          }
                          // Any (partial) files may be deleted if an unexpected error was thrown
                          // during the processing
                          jobProvider.deleteJobFiles(jobId);
                          throw new IllegalStateException(
                              "Problem processing request asynchronously", actualEx);
                        } finally {
                          AsyncJobContext.clear();
                          cleanUpAfterJob(spark, jobId);
                        }
                      });
              Optional<String> ownerId = getCurrentUserId(authentication);
              final Job<IBaseResource> newJob = new Job<>(jobId, operation, result, ownerId);
              newJob.setPreAsyncValidationResult(preAsyncValidationResult.result());
              return newJob;
            });
    final HttpServletResponse response = requestDetails.getServletResponse();
    response.setHeader(
        "Content-Location", requestDetails.getFhirServerBase() + "/$job?id=" + job.getId());
  }

  @Nonnull
  private HttpServletRequest getRequest(@Nonnull final Object[] args) {
    return (HttpServletRequest)
        Arrays.stream(args)
            .filter(ServletRequestDetails.class::isInstance)
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Method annotated with @AsyncSupported must include a HttpServletRequest"
                            + " parameter"));
  }

  @Nonnull
  private ServletRequestDetails getServletRequestDetails(@Nonnull final Object[] args) {
    return (ServletRequestDetails)
        Arrays.stream(args)
            .filter(ServletRequestDetails.class::isInstance)
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Method annotated with @AsyncSupported must include a ServletRequestDetails"
                            + " parameter"));
  }

  private void cleanUpAfterJob(@Nonnull final SparkSession spark, @Nonnull final String requestId) {
    spark.sparkContext().clearJobGroup();
    // Clean up the stage mappings.
    final List<Integer> keys =
        stageMap.entrySet().stream()
            .filter(e -> requestId.equals(e.getValue()))
            .map(Entry::getKey)
            .toList();
    stageMap.keySet().removeAll(keys);
    // We can't clean up the entry in the job registry, it needs to stay there so that clients can
    // retrieve the result of completed jobs.
  }

  @Nonnull
  private static Throwable unwrapFromProxy(@Nonnull final Throwable ex) {
    return ex instanceof UndeclaredThrowableException undeclaredThrowableException
        ? undeclaredThrowableException.getUndeclaredThrowable()
        : ex;
  }

  @Nonnull
  private static OperationOutcome buildOperationOutcome(PreAsyncValidationResult<?> result) {
    final OperationOutcome opOutcome = new OperationOutcome();
    final OperationOutcomeIssueComponent issue = new OperationOutcomeIssueComponent();
    issue.setCode(IssueType.INFORMATIONAL);
    issue.setSeverity(IssueSeverity.INFORMATION);
    issue.setDiagnostics(
        "Job accepted for processing, see the Content-Location header for the "
            + "URL at which status can be queried");
    opOutcome.addIssue(issue);

    result.warnings().forEach(opOutcome::addIssue);

    return opOutcome;
  }
}
