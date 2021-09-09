/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.caching;

import static au.csiro.pathling.utilities.Preconditions.checkNotNull;

import au.csiro.pathling.Configuration;
import ca.uhn.fhir.interceptor.api.Hook;
import ca.uhn.fhir.interceptor.api.Interceptor;
import ca.uhn.fhir.interceptor.api.Pointcut;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.server.exceptions.NotModifiedException;
import java.util.Date;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Intercepts requests and validates ETags, skipping processing if possible. Also adds ETags to
 * responses.
 *
 * @author John Grimes
 */
@Component
@Profile("server")
@Interceptor
@Slf4j
public class EntityTagInterceptor {

  private static final List<String> NON_CACHEABLE_OPERATIONS = List.of("$job");

  @Nonnull
  private final EntityTagValidator validator;

  @Nonnull
  private final Configuration configuration;

  /**
   * @param validator an {@link EntityTagValidator} for validating the tags
   * @param configuration a {@link Configuration} that controls the behaviour of validation
   */
  public EntityTagInterceptor(@Nonnull final EntityTagValidator validator,
      @Nonnull final Configuration configuration) {
    this.validator = validator;
    this.configuration = configuration;
  }

  /**
   * Checks for the {@code If-None-Match} header and validates the tag, skipping processing if
   * possible. Also, adds an {@code ETag} header to the response.
   *
   * @param request the servlet request object
   * @param requestDetails the details about the request inferred by HAPI
   * @param response the servlet response object
   */
  @Hook(Pointcut.SERVER_INCOMING_REQUEST_POST_PROCESSED)
  @SuppressWarnings("unused")
  public void checkIncomingTag(@Nullable final HttpServletRequest request,
      @Nullable final RequestDetails requestDetails,
      @Nullable final HttpServletResponse response) {
    checkNotNull(request);
    checkNotNull(response);
    checkNotNull(requestDetails);
    if (requestIsCacheable(request, requestDetails)) {
      final String tagHeader = request.getHeader("If-None-Match");
      if (tagMatches(tagHeader, requestDetails)) {
        log.info("Entity tag validation succeeded, processing not required");
        throw new NotModifiedException("Supplied entity tag matches");
      } else {
        response.setHeader("ETag", getTag(requestDetails));
        response.setHeader("Cache-Control", "must-revalidate");
        response.addHeader("Cache-Control", "max-age=1");
      }
    }
  }

  @Nonnull
  private String getTag(@Nonnull final RequestDetails requestDetails) {
    // If this is an extract operation and we are using S3 for results, send back an ETag based on
    // the current time, rather than the normal method.
    return requestIsExtractWithS3Result(requestDetails)
           ? validator.tagForTime(new Date().getTime())
           : validator.tag();
  }

  private boolean tagMatches(@Nonnull final String tag,
      @Nonnull final RequestDetails requestDetails) {
    if (requestIsExtractWithS3Result(requestDetails)) {
      // If this is an extract operation, we check that the tag is not older than the configured
      // expiry time for S3 URLs, if S3 is configured for results.
      return validator.validWithExpiry(tag,
          configuration.getStorage().getAws().getSignedUrlExpiry(), new Date().getTime());
    } else {
      return validator.matches(tag);
    }
  }

  private boolean requestIsExtractWithS3Result(@Nonnull final RequestDetails requestDetails) {
    final boolean operationExtract = requestDetails.getOperation() != null &&
        requestDetails.getOperation().equals("$extract");
    return configuration.getStorage().getResultUrl().startsWith("s3://") && operationExtract;
  }

  private static boolean requestIsCacheable(@Nonnull final HttpServletRequest request,
      @Nonnull final RequestDetails requestDetails) {
    //noinspection RedundantCollectionOperation
    final boolean operationCacheable = requestDetails.getOperation() == null ||
        !NON_CACHEABLE_OPERATIONS.contains(requestDetails.getOperation());
    return (request.getMethod().equals("GET") || request.getMethod().equals("HEAD"))
        && operationCacheable;
  }

}
