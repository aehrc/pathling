/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.security.ga4gh;

import static au.csiro.pathling.fhirpath.literal.StringLiteral.escapeFhirPathString;

import au.csiro.pathling.config.ServerConfiguration;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import ca.uhn.fhir.context.RuntimeSearchParam;
import jakarta.annotation.Nonnull;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
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

  @Nonnull
  private final String patientIdSystem;

  @Nonnull
  private final FhirContext fhirContext;

  /**
   * @param configuration used to determine the patient identifier system
   * @param fhirContext a {@link FhirContext} that we use to look up the patient compartment
   */
  public ManifestConverter(@Nonnull final ServerConfiguration configuration,
      @Nonnull final FhirContext fhirContext) {
    patientIdSystem = configuration.getAuth().getGa4ghPassports().getPatientIdSystem();
    this.fhirContext = fhirContext;
  }

  void populateScope(@Nonnull final PassportScope passportScope,
      @Nonnull final VisaManifest manifest) {
    // Create a filter for the Patient resource.
    final String patientIdCollection = manifest.getPatientIds().stream()
        .map(id -> "'" + id + "'")
        .collect(Collectors.joining(" combine "));
    final String patientIdFilter =
        "identifier.where(system = '" + escapeFhirPathString(patientIdSystem)
            + "').where(value in (" + patientIdCollection + "))"
            + ".empty().not()";
    final Set<String> patientFilters = passportScope.get(ResourceType.PATIENT);
    if (patientFilters == null) {
      passportScope.put(ResourceType.PATIENT, new HashSet<>(List.of(patientIdFilter)));
    } else {
      patientFilters.add(patientIdFilter);
    }

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
        } else {
          final Set<String> targets = searchParam.getTargets();
          if (targets.size() > 0 && !targets.contains("Patient")) {
            // If the search parameter does not have a type of "Any" and does not include a type of
            // Patient, we need to skip it altogether.
            continue;
          } else if (targets.size() == 1) {
            // If the search parameter is monomorphic, we can resolve it without `ofType`.
            filter = pathTrimmed + ".resolve()." + patientIdFilter;
          } else {
            // If the search parameter is polymorphic, we also need to resolve it to Patient. Note
            // that polymorphic references with an "Any" type have zero targets.
            filter = pathTrimmed + ".resolve().ofType(Patient)." + patientIdFilter;
          }
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
