package au.csiro.pathling.operations.bulkexport;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import java.util.Set;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.InstantType;

/**
 * Parsed data of the incoming export request.
 *
 * @param originalRequest The original request URL.
 * @param outputFormat The desired output format.
 * @param since Resources will be included in the response if their state has changed after the
 * supplied time.
 * @param until Resources will be included in the response if their state has changed before the
 * supplied time.
 * @param includeResourceTypeFilters When provided, resources will be included in the response if
 * their resource type is listed here.
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
    @Nullable ExportOutputFormat outputFormat,
    @Nullable InstantType since,
    @Nullable InstantType until,
    @Nonnull List<ResourceType> includeResourceTypeFilters,
    @Nonnull List<FhirElement> elements,
    boolean lenient,
    @Nonnull ExportLevel exportLevel,
    @Nonnull Set<String> patientIds
) {

  /**
   * Backwards-compatible constructor for system-level exports.
   *
   * @param originalRequest the original request URL
   * @param outputFormat the desired output format
   * @param since resources changed after this time will be included
   * @param until resources changed before this time will be included
   * @param includeResourceTypeFilters resource types to include
   * @param elements FHIR elements to include in the response
   * @param lenient whether lenient handling is enabled
   */
  public ExportRequest(@Nonnull final String originalRequest,
      @Nullable final ExportOutputFormat outputFormat, @Nullable final InstantType since,
      @Nullable final InstantType until,
      @Nonnull final List<ResourceType> includeResourceTypeFilters,
      @Nonnull final List<FhirElement> elements,
      final boolean lenient) {
    this(originalRequest, outputFormat, since, until, includeResourceTypeFilters, elements, lenient,
        ExportLevel.SYSTEM, Set.of());
  }

  /**
   * Backwards-compatible constructor for system-level exports without lenient flag.
   *
   * @param originalRequest the original request URL
   * @param outputFormat the desired output format
   * @param since resources changed after this time will be included
   * @param until resources changed before this time will be included
   * @param includeResourceTypeFilters resource types to include
   * @param elements FHIR elements to include in the response
   */
  public ExportRequest(@Nonnull final String originalRequest,
      @Nullable final ExportOutputFormat outputFormat, @Nullable final InstantType since,
      @Nullable final InstantType until,
      @Nonnull final List<ResourceType> includeResourceTypeFilters,
      @Nonnull final List<FhirElement> elements) {
    this(originalRequest, outputFormat, since, until, includeResourceTypeFilters, elements, false,
        ExportLevel.SYSTEM, Set.of());
  }

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
   * @param resourceType The resource type for this element.
   * @param elementName The top level element name of the resource or the top level name across all
   * resources if "resourceType" is null.
   */
  public record FhirElement(
      @Nullable ResourceType resourceType,
      @Nonnull String elementName
  ) {

  }

}
