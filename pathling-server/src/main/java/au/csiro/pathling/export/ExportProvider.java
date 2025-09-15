package au.csiro.pathling.export;

import au.csiro.pathling.FhirServer;
import au.csiro.pathling.async.*;
import au.csiro.pathling.library.PathlingContext;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import org.hl7.fhir.r4.model.Binary;
import org.hl7.fhir.r4.model.InstantType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * @author Felix Naumann
 */
@Component
@Profile("server")
public class ExportProvider implements PreAsyncValidation<ExportRequest> {

    private static final Logger log = LoggerFactory.getLogger(ExportProvider.class);

    public static final String OUTPUT_FORMAT_PARAM_NAME = "_outputFormat";
    public static final String SINCE_PARAM_NAME = "_since";
    public static final String UNTIL_PARAM_NAME = "_until";
    public static final String TYPE_PARAM_NAME = "_type";
    public static final String ELEMENTS_PARAM_NAME = "_elements";

    private final ExportExecutor exportExecutor;
    private final PathlingContext pathlingContext;
    private final FhirContext fhirContext;
    private final ExportOperationValidator exportOperationValidator;
    private final JobRegistry jobRegistry;
    private final RequestTagFactory requestTagFactory;

    @Value("${pathling.storage.warehouseUrl}")
    private String warehouseUrl;

    @Autowired
    public ExportProvider(ExportExecutor exportExecutor, PathlingContext pathlingContext, FhirContext fhirContext, ExportOperationValidator exportOperationValidator, JobRegistry jobRegistry, RequestTagFactory requestTagFactory) {
        this.exportExecutor = exportExecutor;
        this.pathlingContext = pathlingContext;
        this.fhirContext = fhirContext;
        this.exportOperationValidator = exportOperationValidator;
        this.jobRegistry = jobRegistry;
        this.requestTagFactory = requestTagFactory;
    }


    @Operation(name = "export", idempotent = true)
    @AsyncSupported
    public Binary export(
            @Nonnull @OperationParam(name = OUTPUT_FORMAT_PARAM_NAME) String outputFormat,
            @Nonnull @OperationParam(name = SINCE_PARAM_NAME) InstantType since,
            @Nullable @OperationParam(name = UNTIL_PARAM_NAME) InstantType until,
            @Nullable @OperationParam(name = TYPE_PARAM_NAME) List<String> type,
            @Nullable @OperationParam(name = ELEMENTS_PARAM_NAME) List<String> elements,

            ServletRequestDetails requestDetails
            ) {
        RequestTag ownTag = requestTagFactory.createTag(requestDetails);
        Job<ExportRequest> ownJob = jobRegistry.get(ownTag);
        if(ownJob == null) {
          throw new InvalidRequestException("Missing 'Prefer: respond-async' header value.");
        }
        ExportRequest exportRequest = ownJob.getPreAsyncValidationResult();
        if(ownJob.isCancelled()) {
          return null;
        }
        ExportResponse exportResponse = exportExecutor.execute(exportRequest, ownJob.getId());
        
        // TODO - this is invoked everytime the $job endpoint is called, so the Expires header is "refreshed" everytime
        // This is allowed (see 2.5.7) but what's missing here is the actual file updating
        // Right now, this is just an arbitrary time not bound to anything
        // If it's bound to the lifecycle of actual resources, then they should be updated accordingly when this consumer is invoked
        // but that would introduce heavy side effects (is this ok??)
        ownJob.setResponseModification(httpServletResponse -> httpServletResponse.addHeader("Expires", ZonedDateTime.now(ZoneOffset.UTC).plusHours(24).format(DateTimeFormatter.RFC_1123_DATE_TIME)));

        return exportResponse.toOutput();
    }

    @Override
    public PreAsyncValidationResult<ExportRequest> preAsyncValidate(ServletRequestDetails servletRequestDetails, Object[] args) {
        return exportOperationValidator.validateRequest(
                servletRequestDetails,
                (String) args[0],
                (InstantType) args[1],
                (InstantType) args[2],
                (List<String>) args[3],
                (List<String>) args[4]);
    }
}
