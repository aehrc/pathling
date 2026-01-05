package au.csiro.pathling.async;

import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import org.hl7.fhir.r4.model.OperationOutcome;

/**
 * @author Felix Naumann
 */
public interface PreAsyncValidation<R> {

  /**
   * Run some validation before the async request kicks off. If the code in this method determines
   * that the request is invalid, then instead of a 202, an error is returned immediately.
   *
   * @param servletRequestDetails The details from the initial client request.
   * @param params The operation parameters.
   * @return A result object with the parsed structure and potential warnings.
   * @throws InvalidRequestException When the request is invalid.
   */
  @Nonnull
  PreAsyncValidationResult<R> preAsyncValidate(
      @Nonnull ServletRequestDetails servletRequestDetails, @Nonnull Object[] params)
      throws InvalidRequestException;

  record PreAsyncValidationResult<R>(
      @Nullable R result,
      @Nonnull List<OperationOutcome.OperationOutcomeIssueComponent> warnings) {}

  /**
   * Computes a cache key component from the parsed request. Override this method to include request
   * body parameters in job deduplication for POST operations.
   *
   * <p>The returned string should be deterministic and include all parameters that make this
   * request unique. It should NOT include:
   *
   * <ul>
   *   <li>The original request URL (already in RequestTag.requestUrl)
   *   <li>Server base URL (infrastructure detail, not request-specific)
   * </ul>
   *
   * @param request the parsed request object from preAsyncValidate
   * @return a cache key component, or empty string if no additional keying is needed
   */
  @Nonnull
  default String computeCacheKeyComponent(@Nonnull R request) {
    return "";
  }
}
