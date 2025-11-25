package au.csiro.pathling.operations.bulkexport;

import au.csiro.pathling.async.AsyncSupported;
import au.csiro.pathling.async.PreAsyncValidation;
import au.csiro.pathling.security.OperationAccess;
import ca.uhn.fhir.rest.annotation.Operation;
import ca.uhn.fhir.rest.annotation.OperationParam;
import ca.uhn.fhir.rest.server.servlet.ServletRequestDetails;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import org.hl7.fhir.r4.model.Binary;
import org.hl7.fhir.r4.model.InstantType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Provider for system-level bulk export operations.
 *
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
  private final ExportOperationValidator exportOperationValidator;

  @Nonnull
  private final ExportOperationHelper exportOperationHelper;

  /**
   * Constructs a new ExportProvider.
   *
   * @param exportOperationValidator the export operation validator
   * @param exportOperationHelper the export operation helper
   */
  @Autowired
  public ExportProvider(@Nonnull final ExportOperationValidator exportOperationValidator,
      @Nonnull final ExportOperationHelper exportOperationHelper) {
    this.exportOperationValidator = exportOperationValidator;
    this.exportOperationHelper = exportOperationHelper;
  }

  /**
   * Handles the $export operation at the system level (/$export).
   *
   * @param outputFormat the output format parameter (validated in pre-async validation)
   * @param since the since date parameter (validated in pre-async validation)
   * @param until the until date parameter (validated in pre-async validation)
   * @param type the type parameter (validated in pre-async validation)
   * @param elements the elements parameter (validated in pre-async validation)
   * @param requestDetails the request details
   * @return the binary result, or null if the job was cancelled
   */
  @Operation(name = "export", idempotent = true)
  @OperationAccess("export")
  @AsyncSupported
  @Nullable
  @SuppressWarnings("unused")
  public Binary export(
      @Nullable @OperationParam(name = OUTPUT_FORMAT_PARAM_NAME) final String outputFormat,
      @Nullable @OperationParam(name = SINCE_PARAM_NAME) final InstantType since,
      @Nullable @OperationParam(name = UNTIL_PARAM_NAME) final InstantType until,
      @Nullable @OperationParam(name = TYPE_PARAM_NAME) final List<String> type,
      @Nullable @OperationParam(name = ELEMENTS_PARAM_NAME) final List<String> elements,
      @Nonnull final ServletRequestDetails requestDetails
  ) {
    return exportOperationHelper.executeExport(requestDetails);
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
