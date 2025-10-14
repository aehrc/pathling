package au.csiro.pathling.export;

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
import org.springframework.context.annotation.Profile;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import static au.csiro.pathling.security.SecurityAspect.getCurrentUserId;

/**
 * @author Felix Naumann
 */
@Component
@Profile("server")
public class ExportProvider implements PreAsyncValidation<ExportRequest> {

  public static final String OUTPUT_FORMAT_PARAM_NAME = "_outputFormat";
  public static final String SINCE_PARAM_NAME = "_since";
  public static final String UNTIL_PARAM_NAME = "_until";
  public static final String TYPE_PARAM_NAME = "_type";
  public static final String ELEMENTS_PARAM_NAME = "_elements";

  private final ExportExecutor exportExecutor;
  private final ExportOperationValidator exportOperationValidator;
  private final JobRegistry jobRegistry;
  private final RequestTagFactory requestTagFactory;
  private final ExportResultRegistry exportResultRegistry;

  @Autowired
  public ExportProvider(ExportExecutor exportExecutor,
      ExportOperationValidator exportOperationValidator, JobRegistry jobRegistry,
      RequestTagFactory requestTagFactory, ExportResultRegistry exportResultRegistry) {
    this.exportExecutor = exportExecutor;
    this.exportOperationValidator = exportOperationValidator;
    this.jobRegistry = jobRegistry;
    this.requestTagFactory = requestTagFactory;
    this.exportResultRegistry = exportResultRegistry;
  }


  @Operation(name = "export", idempotent = true)
  @OperationAccess("export")
  @AsyncSupported
  public Binary export(
      @Nullable @OperationParam(name = OUTPUT_FORMAT_PARAM_NAME) String outputFormat,
      @Nullable @OperationParam(name = SINCE_PARAM_NAME) InstantType since,
      @Nullable @OperationParam(name = UNTIL_PARAM_NAME) InstantType until,
      @Nullable @OperationParam(name = TYPE_PARAM_NAME) List<String> type,
      @Nullable @OperationParam(name = ELEMENTS_PARAM_NAME) List<String> elements,
      ServletRequestDetails requestDetails
  ) {

    // TODO - is it ok to use auth=null to retrieve the job id first? Or should I keep track of this using a custom registry similar to how $extract uses a "resultRegistry"?
    final Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
    RequestTag ownTag = requestTagFactory.createTag(requestDetails, authentication);
    Job<ExportRequest> ownJob = jobRegistry.get(ownTag);
    if (ownJob == null) {
      throw new InvalidRequestException("Missing 'Prefer: respond-async' header value.");
    }
    // Check that the user requesting the result is the same user that started the job.
    final Optional<String> currentUserId = getCurrentUserId(authentication);
    if (currentUserId.isPresent() && !ownJob.getOwnerId().equals(currentUserId)) {
      throw new AccessDeniedError("The requested result is not owned by the current user '%s'.".formatted(currentUserId.orElse("null")));
    }
    
    ExportRequest exportRequest = ownJob.getPreAsyncValidationResult();
    if (ownJob.isCancelled()) {
      return null;
    }
    
    exportResultRegistry.put(ownJob.getId(), new ExportResult(ownJob.getOwnerId()));
    
    ExportResponse exportResponse = exportExecutor.execute(exportRequest, ownJob.getId());

    // TODO - this is invoked everytime the $job endpoint is called, so the Expires header is "refreshed" everytime
    // This is allowed (see 2.5.7) but what's missing here is the actual file updating
    // Right now, this is just an arbitrary time not bound to anything
    // If it's bound to the lifecycle of actual resources, then they should be updated accordingly when this consumer is invoked
    // but that would introduce heavy side effects (is this ok??)
    ownJob.setResponseModification(httpServletResponse -> httpServletResponse.addHeader("Expires",
        ZonedDateTime.now(ZoneOffset.UTC).plusHours(24)
            .format(DateTimeFormatter.RFC_1123_DATE_TIME)));

    return exportResponse.toOutput();
  }

  @Override
  public PreAsyncValidationResult<ExportRequest> preAsyncValidate(
      ServletRequestDetails servletRequestDetails, Object[] args) {
    return exportOperationValidator.validateRequest(
        servletRequestDetails,
        (String) args[0],
        (InstantType) args[1],
        (InstantType) args[2],
        (List<String>) args[3],
        (List<String>) args[4]);
  }
}
