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

package au.csiro.pathling.errors;

import static java.util.Objects.requireNonNull;
import static org.springframework.http.HttpStatus.SERVICE_UNAVAILABLE;

import au.csiro.pathling.io.SchemaDrift;
import au.csiro.pathling.io.SchemaDriftError;
import ca.uhn.fhir.interceptor.api.Hook;
import ca.uhn.fhir.interceptor.api.Interceptor;
import ca.uhn.fhir.interceptor.api.Pointcut;
import ca.uhn.fhir.parser.DataFormatException;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.client.exceptions.FhirClientConnectionException;
import ca.uhn.fhir.rest.server.exceptions.BaseServerResponseException;
import ca.uhn.fhir.rest.server.exceptions.InternalErrorException;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import com.fasterxml.jackson.core.JsonParseException;
import com.google.common.util.concurrent.UncheckedExecutionException;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Collections;
import org.apache.spark.SparkException;
import org.apache.spark.SparkRuntimeException;
import org.apache.spark.sql.delta.DeltaAnalysisException;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.OperationOutcome.OperationOutcomeIssueComponent;

/**
 * This class unifies exception handling.
 *
 * @author Piotr Szul
 */
@Interceptor
public class ErrorHandlingInterceptor {

  /**
   * The Delta condition raised when a MERGE cannot cast the source struct to the target struct
   * because their fields do not correspond. This is the condition a stored table produces when its
   * schema and this server's encoders disagree in a way Delta will not reconcile, in either
   * direction.
   */
  private static final String DELTA_SCHEMA_MISMATCH_CONDITION =
      "DELTA_UPDATE_SCHEMA_MISMATCH_EXPRESSION";

  /**
   * HAPI hook to convert errors and exceptions to BaseServerResponseException.
   *
   * @param requestDetails the details of the request (HAPI)
   * @param servletRequestDetails further details of the request (HAPI)
   * @param throwable the exception to process
   * @param request the details of the request (Servlet API)
   * @param response the response that will be sent
   * @return an exception of type {@link BaseServerResponseException}
   */
  @Hook(Pointcut.SERVER_PRE_PROCESS_OUTGOING_EXCEPTION)
  @SuppressWarnings("unused")
  public BaseServerResponseException handleOutgoingException(
      @Nullable final RequestDetails requestDetails,
      @Nullable final ServletRequestDetails servletRequestDetails,
      @Nonnull final Throwable throwable,
      @Nullable final HttpServletRequest request,
      @Nullable final HttpServletResponse response) {

    return convertError(
        throwable, requestDetails == null ? null : requestDetails.getResourceName());
  }

  /**
   * Converts an error into a HAPI BaseServerResponseException.
   *
   * @param error an error that could be raised during processing
   * @return a HAPI {@link BaseServerResponseException} that will deliver an appropriate response to
   *     a user of the FHIR API
   */
  @Nonnull
  public static BaseServerResponseException convertError(@Nonnull final Throwable error) {
    return convertError(error, null);
  }

  /**
   * Converts an error into a HAPI BaseServerResponseException, naming the resource type in those
   * messages that describe a problem with a particular type's stored data.
   *
   * @param error an error that could be raised during processing
   * @param resourceType the resource type the request concerns, or null where it is not known. A
   *     Delta schema-mismatch exception does not carry it, so it is supplied by the caller that
   *     does know it.
   * @return a HAPI {@link BaseServerResponseException} that will deliver an appropriate response to
   *     a user of the FHIR API
   */
  @SuppressWarnings("java:S3776") // Complexity is acceptable for centralised exception handling.
  @Nonnull
  public static BaseServerResponseException convertError(
      @Nonnull final Throwable error, @Nullable final String resourceType) {
    try {
      throw error;

    } catch (final SparkException
        | UncheckedExecutionException
        | InternalErrorException
        | InvocationTargetException
        | UndeclaredThrowableException e) {
      // A number of exceptions are being used to wrap the actual cause. In this case we unwrap
      // its cause and pass it back to this same method to be re-evaluated.
      //
      // A SparkException is thrown when an error occurs inside a Spark job.
      //
      // InvocationTargetException wrapped in InternalErrorException is thrown when a non
      // BaseServerResponseException is thrown from a IResourceProvider
      // (see: ca.uhn.fhir.rest.server.method.BaseMethodBinding.invokeServerMethod )
      @Nullable final Throwable cause = e.getCause();
      if (cause != null) {
        return convertError(cause, resourceType);
      } else {
        return internalServerError(e);
      }
    } catch (final DataFormatException e) {
      return convertDataFormatException(e);

    } catch (final FhirClientConnectionException e) {
      // Special case for FhirClientConnectionException
      // return error 503 as per issue #146
      return BaseServerResponseException.newInstance(SERVICE_UNAVAILABLE.value(), e.getMessage());

    } catch (final BaseServerResponseException e) {
      // We pass HAPI exceptions through unaltered unless they do not include a valid HTTP
      // status code.
      if (e.getStatusCode() == 0) {
        return internalServerError(e);
      } else {
        return e;
      }

    } catch (final ResourceNotFoundError e) {
      // Errors relating to resources not found are passed through using the corresponding HAPI
      // exception.
      return new ResourceNotFoundException(e.getMessage());

    } catch (final InvalidUserInputError e) {
      // Errors relating to invalid user input are passed through using the corresponding HAPI
      // exception.
      return new InvalidRequestException(e);
    } catch (final AccessDeniedError e) {
      return buildException(HttpServletResponse.SC_FORBIDDEN, e.getMessage(), IssueType.FORBIDDEN);
    } catch (final SchemaDriftError e) {
      // A drifted, unmigrated table is a server-side deployment state; surface the actionable
      // message instead of the generic "Unexpected error occurred".
      return buildException(
          HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
          requireNonNull(e.getMessage()),
          IssueType.PROCESSING);
    } catch (final SparkRuntimeException e) {
      // SparkRuntimeException with USER_RAISED_EXCEPTION indicates an intentionally raised
      // error (via raise_error() in Spark SQL) that should be surfaced to the client.
      if ("USER_RAISED_EXCEPTION".equals(e.getCondition())) {
        return new InvalidRequestException(e.getMessage());
      }
      // Other SparkRuntimeExceptions might wrap a cause we can convert.
      @Nullable final Throwable cause = e.getCause();
      if (cause != null) {
        return convertError(cause, resourceType);
      }
      return internalServerError(e);
    } catch (final DeltaAnalysisException e) {
      // A stored table that cannot be reconciled with this server's encoders is a deployment state,
      // not a request defect, but the raw Delta message embeds both struct definitions in full and
      // so cannot be returned. Translate the one condition that describes it and let every other
      // Delta condition fall through to the generic message.
      if (!DELTA_SCHEMA_MISMATCH_CONDITION.equals(e.getCondition())) {
        return internalServerError(e);
      }
      return buildException(
          HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
          requireNonNull(describeSchemaMismatch(e, resourceType).getMessage()),
          IssueType.PROCESSING);
    } catch (final Throwable e) { // NO-SONAR we really want to catch everything here
      // Anything else is unexpected and triggers a 500.
      return internalServerError(e);
    }
  }

  /**
   * Describes a Delta schema-mismatch condition in terms of the resource type, the direction of the
   * disagreement and the field paths involved.
   *
   * <p>The exception's two message parameters are the catalog strings of the source and target
   * structs, in that order. Parsing them back into schemas lets the same comparison that reports
   * drift elsewhere derive the field paths, so no exception text reaches the message. The paths are
   * relative to the struct Delta was casting, which is the element that actually differs.
   *
   * <p>A parameter that cannot be parsed - a shape this Delta version does not produce today, but
   * nothing guarantees that - leaves both directions empty, and the resulting message names the
   * condition without field detail rather than failing.
   */
  @Nonnull
  private static SchemaDriftError describeSchemaMismatch(
      @Nonnull final DeltaAnalysisException error, @Nullable final String resourceType) {
    final String[] parameters = error.getMessageParametersArray();
    @Nullable final StructType source = parameters.length > 0 ? parseStruct(parameters[0]) : null;
    @Nullable final StructType target = parameters.length > 1 ? parseStruct(parameters[1]) : null;
    if (source == null || target == null) {
      return new SchemaDriftError(resourceType, Collections.emptySet(), Collections.emptySet());
    }
    return new SchemaDriftError(
        resourceType,
        SchemaDrift.missingFieldPaths(source, target),
        SchemaDrift.excessFieldPaths(source, target));
  }

  /** Parses a struct's catalog string back into a schema, returning null if it is not one. */
  @Nullable
  private static StructType parseStruct(@Nonnull final String catalogString) {
    try {
      return DataType.fromDDL(catalogString) instanceof final StructType struct ? struct : null;
    } catch (final Exception e) {
      // The parameter was not a parseable type; the caller falls back to a message without field
      // detail.
      return null;
    }
  }

  @Nonnull
  private static BaseServerResponseException buildException(
      final int theStatusCode, @Nonnull final String message, @Nonnull final IssueType issueType) {
    final OperationOutcome opOutcome = new OperationOutcome();
    final OperationOutcomeIssueComponent issue = new OperationOutcomeIssueComponent();
    issue.setSeverity(IssueSeverity.ERROR);
    issue.setDiagnostics(message);
    issue.setCode(issueType);
    opOutcome.addIssue(issue);
    final BaseServerResponseException ex =
        BaseServerResponseException.newInstance(theStatusCode, message);
    ex.setOperationOutcome(opOutcome);
    return ex;
  }

  @Nonnull
  private static BaseServerResponseException convertDataFormatException(
      @Nonnull final DataFormatException e) {
    final Throwable cause = e.getCause();
    if (cause == null) {
      // A problem with constructing FHIR from JSON.
      return new InvalidRequestException("Invalid FHIR content: " + e.getMessage());
    } else {
      if (cause instanceof JsonParseException) {
        // A problem with parsing JSON.
        return new InvalidRequestException("Invalid JSON content: " + cause.getMessage());
      } else {
        // An unknown problem with FHIR/JSON content.
        return new InvalidRequestException(
            "Unknown problem while parsing FHIR/JSON content: " + cause.getMessage());
      }
    }
  }

  @Nonnull
  private static InternalErrorException internalServerError(final @Nonnull Throwable error) {
    return error instanceof final InternalErrorException internalErrorException
        ? internalErrorException
        : new InternalErrorException("Unexpected error occurred", error);
  }
}
