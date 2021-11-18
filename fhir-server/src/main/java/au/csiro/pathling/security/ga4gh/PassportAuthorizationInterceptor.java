/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.security.ga4gh;

import static au.csiro.pathling.fhirpath.literal.StringLiteralPath.escapeFhirPathString;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import ca.uhn.fhir.context.RuntimeSearchParam;
import ca.uhn.fhir.interceptor.api.Hook;
import ca.uhn.fhir.interceptor.api.Interceptor;
import ca.uhn.fhir.interceptor.api.Pointcut;
import ca.uhn.fhir.rest.server.exceptions.UnclassifiedServerFailureException;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.springframework.context.annotation.Profile;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.core.ClaimAccessor;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtClaimAccessor;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.stereotype.Component;

/**
 * @author John Grimes
 */
@Component
@Profile("server & ga4gh")
@Interceptor
@Slf4j
public class PassportAuthorizationInterceptor {

  private static final String PASSPORT_CLAIM_NAME = "ga4gh_passport_v1";
  private static final String VISA_CLAIM_NAME = "ga4gh_visa_v1";
  private static final String VISA_TYPE = "ControlledAccessGrants";
  private static final String PATIENT_IDENTIFIER_SYSTEM = "https://nagim.dev/patient";

  @Nonnull
  private final JwtDecoder jwtDecoder;

  @Nonnull
  private final FhirContext fhirContext;

  @Nonnull
  private final PassportScope passportScope;

  /**
   * @param jwtDecoder a {@link JwtDecoder} that we can use to decode embedded visas
   * @param fhirContext a {@link FhirContext} that we use to look up the patient compartment
   * @param passportScope a request-scoped {@link PassportScope} used to store the extracted
   * filters
   */
  @SuppressWarnings("WeakerAccess")
  public PassportAuthorizationInterceptor(@Nonnull final JwtDecoder jwtDecoder,
      @Nonnull final FhirContext fhirContext, @Nonnull final PassportScope passportScope) {
    this.jwtDecoder = jwtDecoder;
    this.fhirContext = fhirContext;
    this.passportScope = passportScope;
  }

  /**
   * Processes any GA4GH passport found in the request, adding a set of FHIRPath filters to a
   * request-scoped {@link PassportScope} bean.
   */
  @Hook(Pointcut.SERVER_INCOMING_REQUEST_PRE_HANDLED)
  @SuppressWarnings("unused")
  public void processPassport() {
    // Get the JWT that has already been processed and decoded by Spring Security.
    final ClaimAccessor credentials = (Jwt) SecurityContextHolder.getContext().getAuthentication()
        .getCredentials();

    // Get the list of encoded visas from the passport claim.
    final List<String> encodedVisas = credentials.getClaimAsStringList(PASSPORT_CLAIM_NAME);

    if (encodedVisas != null) {
      for (final String encodedVisa : encodedVisas) {
        // Decode each visa using the same decoder we use for the primary access token.
        final Jwt visa = jwtDecoder.decode(encodedVisa);

        // Get the main visa claim and make sure it has the expected visa type.
        if (!validateVisaType(visa)) {
          continue;
        }

        // Get the issuer and use it to retrieve the manifest.
        final VisaManifest manifest = getManifest(visa);

        // Translate each patient ID in the manifest into a set of filters, and add to the passport
        // scope.
        final Map<ResourceType, Set<String>> filters = translateManifestToFilters(manifest);
        filters.keySet().forEach(resourceType -> passportScope.get(resourceType)
            .addAll(filters.get(resourceType)));

      }

      log.debug("Resolved passport filters: {}", passportScope);
    }
  }

  @Nonnull
  private Map<ResourceType, Set<String>> translateManifestToFilters(
      @Nonnull final VisaManifest manifest) {
    final Map<ResourceType, Set<String>> result = new HashMap<>();
    for (final String patientId : manifest.getPatientIds()) {
      // Add a filter for the Patient resource.
      final String patientIdFilter =
          "identifier.where(system = '" + escapeFhirPathString(PATIENT_IDENTIFIER_SYSTEM)
              + "' and value = '" + escapeFhirPathString(patientId) + "')";
      final Set<String> patientFilters = new HashSet<>(List.of(patientIdFilter));
      result.put(ResourceType.PATIENT, patientFilters);

      // Add a filters for each resource type covering off any resource references defined within
      // the patient compartment.
      // See: https://www.hl7.org/fhir/r4/compartmentdefinition-patient.html
      for (final ResourceType resourceType : ResourceType.values()) {
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
              "\\.where\\(resolve\\(\\) is " + resourceType.toCode() + "\\)";
          final String filter;
          if (pathTrimmed.endsWith(resolvePattern)) {
            filter = pathTrimmed.replace(resolvePattern,
                ".resolve().ofType(Patient)." + patientIdFilter);
          } else {
            filter = pathTrimmed + "resolve()." + patientIdFilter;
          }

          // Add the filter to the map.
          final Set<String> filters = result.get(resourceType);
          if (filters == null) {
            result.put(resourceType, new HashSet<>(List.of(filter)));
          } else {
            filters.add(filter);
          }
        }
      }
    }
    return result;
  }

  private boolean validateVisaType(@Nonnull final ClaimAccessor visa) {
    final Map<String, Object> visaClaim = visa.getClaimAsMap(VISA_CLAIM_NAME);
    if (visaClaim == null || !visaClaim.get("type").equals(VISA_TYPE)) {
      log.debug("Visa is not of type {}, skipping", VISA_TYPE);
      return false;
    }
    return true;
  }

  @Nonnull
  private VisaManifest getManifest(@Nonnull final JwtClaimAccessor visa) {
    final URL issuer = visa.getIssuer();
    final CloseableHttpClient httpClient = HttpClients.createDefault();
    final VisaManifest manifest;
    try {
      log.debug("Retrieving manifest from {}", issuer);
      final HttpUriRequest get = new HttpGet(issuer.toURI());
      manifest = httpClient.execute(get, this::manifestResponseHandler);
    } catch (final URISyntaxException | IOException e) {
      throw new UnclassifiedServerFailureException(502, "Problem retrieving manifest for visa");
    }
    return manifest;
  }

  @Nonnull
  private VisaManifest manifestResponseHandler(@Nonnull final HttpResponse response)
      throws IOException {
    final int status = response.getStatusLine().getStatusCode();
    if (status != 200) {
      throw new ClientProtocolException(
          "VisaManifest retrieval - unexpected response status: " + status);
    }
    final HttpEntity entity = response.getEntity();
    if (entity == null) {
      throw new ClientProtocolException("VisaManifest retrieval - no content");
    }
    final Gson gson = new GsonBuilder().create();
    return gson.fromJson(EntityUtils.toString(entity), VisaManifest.class);
  }

}
