/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.async;

import static au.csiro.pathling.utilities.Preconditions.checkNotNull;

import ca.uhn.fhir.rest.api.server.RequestDetails;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
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
import org.slf4j.MDC;
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
  private final StageMap stageMap;

  @Nonnull
  private final SparkSession spark;

  /**
   * @param executor used to run asynchronous jobs in the background
   * @param jobRegistry the {@link JobRegistry} used to keep track of running jobs
   * @param stageMap the {@link StageMap} used to map stages to job IDs
   * @param spark used for updating the Spark Context with job identity
   */
  public AsyncAspect(@Nonnull final ThreadPoolTaskExecutor executor,
      @Nonnull final JobRegistry jobRegistry, @Nonnull final StageMap stageMap,
      @Nonnull final SparkSession spark) {
    this.executor = executor;
    this.jobRegistry = jobRegistry;
    this.stageMap = stageMap;
    this.spark = spark;
  }

  @Around("@annotation(asyncSupported)")
  private IBaseResource maybeExecuteAsynchronously(@Nonnull final ProceedingJoinPoint joinPoint,
      @Nonnull final AsyncSupported asyncSupported) throws Throwable {
    final Object[] args = joinPoint.getArgs();
    final HttpServletRequest request = getRequest(args);
    final String prefer = request.getHeader(ASYNC_HEADER);

    if (prefer != null && prefer.equals(ASYNC_HEADER_VALUE)) {
      log.info("Asynchronous processing requested");
      processRequestAsynchronously(joinPoint, args, spark);
      throw new ProcessingNotCompletedException("Accepted", buildOperationOutcome());
    } else {
      return (IBaseResource) joinPoint.proceed(args);
    }
  }

  private void processRequestAsynchronously(@Nonnull final ProceedingJoinPoint joinPoint,
      @Nonnull final Object[] args, @Nonnull final SparkSession spark) {
    final RequestDetails requestDetails = getRequestDetails(args);
    final HttpServletResponse response = getResponse(args);
    final String requestId = requestDetails.getRequestId();
    final Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
    checkNotNull(requestId);
    final String operation = requestDetails.getOperation().replaceFirst("\\$", "");
    final Future<IBaseResource> result = executor.submit(() -> {
      try {
        MDC.put("requestId", requestId);
        SecurityContextHolder.getContext().setAuthentication(authentication);
        spark.sparkContext().setJobGroup(requestId, requestId, true);
        return (IBaseResource) joinPoint.proceed(args);
      } catch (final Throwable e) {
        throw new RuntimeException("Problem processing request asynchronously", e);
      } finally {
        cleanUpAfterJob(spark, requestId);
      }
    });
    jobRegistry.put(requestId, new Job(operation, result));
    response.setHeader("Content-Location",
        requestDetails.getFhirServerBase() + "/$job?id=" + requestId);
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
  private RequestDetails getRequestDetails(@Nonnull final Object[] args) {
    return (RequestDetails) Arrays.stream(args)
        .filter(a -> a instanceof RequestDetails)
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException(
            "Method annotated with @AsyncSupported must include a RequestDetails parameter"));
  }

  @Nonnull
  private HttpServletResponse getResponse(@Nonnull final Object[] args) {
    return (HttpServletResponse) Arrays.stream(args)
        .filter(a -> a instanceof HttpServletResponse)
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException(
            "Method annotated with @AsyncSupported must include a HttpServletResponse parameter"));
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
