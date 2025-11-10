package au.csiro.pathling.operations.bulkexport;

import static au.csiro.pathling.security.SecurityAspect.getCurrentUserId;
import static au.csiro.pathling.utilities.Preconditions.checkArgument;

import au.csiro.pathling.async.AsyncSupported;
import au.csiro.pathling.async.Job;
import au.csiro.pathling.async.JobRegistry;
import au.csiro.pathling.async.PreAsyncValidation;
import au.csiro.pathling.async.RequestTag;
import au.csiro.pathling.async.RequestTagFactory;
import au.csiro.pathling.errors.AccessDeniedError;
import au.csiro.pathling.security.OperationAccess;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Optional;
import org.hl7.fhir.r4.model.Binary;
import org.hl7.fhir.r4.model.InstantType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

/**
 * @author Felix Naumann
 */
@Component
public class ExportProvider implements PreAsyncValidation<ExportRequest> {

  /**
   * The name of the output format parameter.
   */
  public static final String OUTPUT_FORMAT_PARAM_NAME = "_outputFormat";

  /**
   * The name of the since parameter.
   */
  public static final String SINCE_PARAM_NAME = "_since";

  /**
   * The name of the until parameter.
   */
  public static final String UNTIL_PARAM_NAME = "_until";

  /**
   * The name of the type parameter.
   */
  public static final String TYPE_PARAM_NAME = "_type";

  /**
   * The name of the elements parameter.
   */
  public static final String ELEMENTS_PARAM_NAME = "_elements";

  @Nonnull
  private final ExportExecutor exportExecutor;

  @Nonnull
  private final ExportOperationValidator exportOperationValidator;

  @Nonnull
  private final JobRegistry jobRegistry;

  @Nonnull
  private final RequestTagFactory requestTagFactory;

  @Nonnull
  private final ExportResultRegistry exportResultRegistry;

  /**
   * Constructs a new ExportProvider.
   *
   * @param exportExecutor The export executor.
   * @param exportOperationValidator The export operation validator.
   * @param jobRegistry The job registry.
   * @param requestTagFactory The request tag factory.
   * @param exportResultRegistry The export result registry.
   */
  @Autowired
  public ExportProvider(@Nonnull final ExportExecutor exportExecutor,
      @Nonnull final ExportOperationValidator exportOperationValidator,
      @Nonnull final JobRegistry jobRegistry,
      @Nonnull final RequestTagFactory requestTagFactory,
      @Nonnull final ExportResultRegistry exportResultRegistry) {
    this.exportExecutor = exportExecutor;
    this.exportOperationValidator = exportOperationValidator;
    this.jobRegistry = jobRegistry;
    this.requestTagFactory = requestTagFactory;
    this.exportResultRegistry = exportResultRegistry;
  }

  /**
   * Handles the $export operation.
   *
   * @param outputFormat The output format parameter (unused in current implementation).
   * @param since The since date parameter (unused in current implementation).
   * @param until The until date parameter (unused in current implementation).
   * @param type The type parameter (unused in current implementation).
   * @param elements The elements parameter (unused in current implementation).
   * @param requestDetails The request details.
   * @return The binary result, or null if the job was cancelled.
   */
  @Operation(name = "export", idempotent = true)
  @OperationAccess("export")
  @AsyncSupported
  @Nullable
  public Binary export(
      @Nullable @OperationParam(name = OUTPUT_FORMAT_PARAM_NAME) final String outputFormat,
      @Nullable @OperationParam(name = SINCE_PARAM_NAME) final InstantType since,
      @Nullable @OperationParam(name = UNTIL_PARAM_NAME) final InstantType until,
      @Nullable @OperationParam(name = TYPE_PARAM_NAME) final List<String> type,
      @Nullable @OperationParam(name = ELEMENTS_PARAM_NAME) final List<String> elements,
      @Nullable final ServletRequestDetails requestDetails
  ) {

    checkArgument(requestDetails != null, "requestDetails must not be null");
    final Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
    final RequestTag ownTag = requestTagFactory.createTag(requestDetails, authentication);
    final Job<ExportRequest> ownJob = jobRegistry.get(ownTag);
    if (ownJob == null) {
      throw new InvalidRequestException("Missing 'Prefer: respond-async' header value.");
    }
    // Check that the user requesting the result is the same user that started the job.
    final Optional<String> currentUserId = getCurrentUserId(authentication);
    if (currentUserId.isPresent() && !ownJob.getOwnerId().equals(currentUserId)) {
      throw new AccessDeniedError(
          "The requested result is not owned by the current user '%s'.".formatted(
              currentUserId.orElse("null")));
    }

    final ExportRequest exportRequest = ownJob.getPreAsyncValidationResult();
    if (ownJob.isCancelled()) {
      return null;
    }

    exportResultRegistry.put(ownJob.getId(), new ExportResult(ownJob.getOwnerId()));

    final ExportResponse exportResponse = exportExecutor.execute(exportRequest, ownJob.getId());

    // TODO - this is invoked everytime the $job endpoint is called, so the Expires header is "refreshed" everytime.
    // This is allowed (see 2.5.7) but what's missing here is the actual file updating.
    // Right now, this is just an arbitrary time not bound to anything.
    // If it's bound to the lifecycle of actual resources, then they should be updated accordingly 
    // when this consumer is invoked but that would introduce heavy side effects (is this ok??).
    ownJob.setResponseModification(httpServletResponse -> httpServletResponse.addHeader("Expires",
        ZonedDateTime.now(ZoneOffset.UTC).plusHours(24)
            .format(DateTimeFormatter.RFC_1123_DATE_TIME)));

    return exportResponse.toOutput();
  }

  @Override
  @SuppressWarnings("unchecked")
  @Nonnull
  public PreAsyncValidationResult<ExportRequest> preAsyncValidate(
      @Nonnull final ServletRequestDetails servletRequestDetails, @Nonnull final Object[] args) {
    return exportOperationValidator.validateRequest(
        servletRequestDetails,
        (String) args[0],
        (InstantType) args[1],
        (InstantType) args[2],
        (List<String>) args[3],
        (List<String>) args[4]);
  }
}
