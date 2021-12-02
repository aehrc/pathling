/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.security.ga4gh;

import au.csiro.pathling.fhirpath.literal.StringLiteralPath;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import ca.uhn.fhir.context.RuntimeSearchParam;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * A component that knows how to take a visa manifest and build up a set of corresponding query
 * scopes.
 *
 * @author John Grimes
 */
@Component
@Profile("server & ga4gh")
public class ManifestConverter {

  private static final String PATIENT_IDENTIFIER_SYSTEM = "https://nagim.dev/patient";

  @Nonnull
  private final FhirContext fhirContext;

  /**
   * @param fhirContext a {@link FhirContext} that we use to look up the patient compartment
   */
  public ManifestConverter(@Nonnull final FhirContext fhirContext) {
    this.fhirContext = fhirContext;
  }

  void populateScope(@Nonnull final PassportScope passportScope,
      @Nonnull final VisaManifest manifest) {
    for (final String patientId : manifest.getPatientIds()) {
      // Add a filter for the Patient resource.
      final String patientIdFilter =
          "identifier.where(system = '" + StringLiteralPath.escapeFhirPathString(
              PATIENT_IDENTIFIER_SYSTEM)
              + "' and value = '" + StringLiteralPath.escapeFhirPathString(patientId)
              + "').empty().not()";
      final Set<String> patientFilters = new HashSet<>(List.of(patientIdFilter));
      passportScope.put(ResourceType.PATIENT, patientFilters);

      // Add a filters for each resource type covering off any resource references defined within
      // the patient compartment.
      // See: https://www.hl7.org/fhir/r4/compartmentdefinition-patient.html
      for (final ResourceType resourceType : ResourceType.values()) {
        if (resourceType.equals(ResourceType.DOMAINRESOURCE) || resourceType.equals(
            ResourceType.RESOURCE) || resourceType.equals(ResourceType.NULL)) {
          continue;
        }
        final RuntimeResourceDefinition definition = fhirContext.getResourceDefinition(
            resourceType.toCode());
        final List<RuntimeSearchParam> searchParams = definition.getSearchParamsForCompartmentName(
            "Patient");
        for (final RuntimeSearchParam searchParam : searchParams) {
          final String path = searchParam.getPath();

          // Remove the leading "[resource type]." from the path.
          final String pathTrimmed = path.replaceFirst("^" + resourceType.toCode() + "\\.", "");

          // Paths that end with this resolve pattern are polymorphic references, and will need
          // to be resolved using `ofType()` within our implementation.
          final String resolvePattern =
              ".where(resolve() is Patient)";
          final String filter;
          if (pathTrimmed.endsWith(resolvePattern)) {
            filter = pathTrimmed.replace(resolvePattern,
                ".resolve().ofType(Patient)." + patientIdFilter);
          } else if (searchParam.getTargets().size() > 1) {
            // If the search parameter is polymorphic, we also need to resolve it to Patient.
            filter = pathTrimmed + ".resolve().ofType(Patient)." + patientIdFilter;
          } else {
            // If the search parameter is monomorphic, we can resolve it without `ofType`.
            filter = pathTrimmed + ".resolve()." + patientIdFilter;
          }

          // Add the filter to the map.
          final Set<String> filters = passportScope.get(resourceType);
          if (filters == null) {
            passportScope.put(resourceType, new HashSet<>(List.of(filter)));
          } else {
            filters.add(filter);
          }
        }
      }
    }
  }

}
