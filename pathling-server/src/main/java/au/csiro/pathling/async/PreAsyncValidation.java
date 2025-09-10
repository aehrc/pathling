package au.csiro.pathling.async;

import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import org.hl7.fhir.r4.model.OperationOutcome;

import java.util.List;

/**
 * @author Felix Naumann
 */
public interface PreAsyncValidation<R> {

    /**
     * Run some validation before the async request kicks off. If the code in this method determines that the
     * request is invalid, then instead of a 202, an error is returned immediately.
     * @param servletRequestDetails The details from the initial client request.
     * @return A result object with the parsed structure and potential warnings.
     * @throws InvalidRequestException When the request is invalid.
     */
    PreAsyncValidationResult<R> preAsyncValidate(ServletRequestDetails servletRequestDetails, Object[] params) throws InvalidRequestException;

    record PreAsyncValidationResult<R>(@Nullable R result, @Nonnull List<OperationOutcome.OperationOutcomeIssueComponent> warnings) {}
}
