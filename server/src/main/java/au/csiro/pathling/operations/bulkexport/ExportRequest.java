package au.csiro.pathling.operations.bulkexport;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Set;
import org.hl7.fhir.r4.model.InstantType;

/**
 * Parsed data of the incoming export request.
 *
 * @param originalRequest The original request URL.
 * @param serverBaseUrl The FHIR server base URL (without trailing slash), used for constructing
 * result URLs.
 * @param outputFormat The desired output format.
 * @param since Resources will be included in the response if their state has changed after the
 * supplied time.
 * @param until Resources will be included in the response if their state has changed before the
 * supplied time.
 * @param includeResourceTypeFilters When provided, resources will be included in the response if
 * their resource type is listed here. Uses String resource type codes to support both standard
 * FHIR resource types and custom types like ViewDefinition.
 * @param elements When provided, the listed FHIR resource elements will be the only ones returned
 * in the resources (alongside mandatory elements).
 * @param lenient Lenient handling enabled.
 * @param exportLevel The level at which the export is being performed.
 * @param patientIds The patient IDs to filter by for patient-level or group-level exports.
 * @author Felix Naumann
 * @author John Grimes
 */
public record ExportRequest(
    @Nonnull String originalRequest,
    @Nonnull String serverBaseUrl,
    @Nullable ExportOutputFormat outputFormat,
    @Nullable InstantType since,
    @Nullable InstantType until,
    @Nonnull List<String> includeResourceTypeFilters,
    @Nonnull List<FhirElement> elements,
    boolean lenient,
    @Nonnull ExportLevel exportLevel,
    @Nonnull Set<String> patientIds
) {

  /**
   * The level at which the export operation is being executed.
   */
  public enum ExportLevel {
    /**
     * System-level export: /$export.
     */
    SYSTEM,
    /**
     * Patient type-level export: /Patient/$export (all patients).
     */
    PATIENT_TYPE,
    /**
     * Patient instance-level export: /Patient/[id]/$export.
     */
    PATIENT_INSTANCE,
    /**
     * Group-level export: /Group/[id]/$export.
     */
    GROUP
  }

  /**
   * A small container for resource types and their top level elements.
   *
   * @param resourceTypeCode The resource type code for this element. Uses String to support both
   * standard FHIR resource types and custom types.
   * @param elementName The top level element name of the resource or the top level name across all
   * resources if "resourceTypeCode" is null.
   */
  public record FhirElement(
      @Nullable String resourceTypeCode,
      @Nonnull String elementName
  ) {

  }

}
