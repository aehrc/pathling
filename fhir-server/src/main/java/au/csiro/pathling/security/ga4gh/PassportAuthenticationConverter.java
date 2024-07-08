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

import static java.util.Objects.requireNonNull;

import au.csiro.pathling.config.ServerConfiguration;
import ca.uhn.fhir.rest.server.exceptions.UnclassifiedServerFailureException;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.shaded.com.nimbusds.jose.shaded.json.JSONObject;
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
import org.springframework.core.convert.converter.Converter;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.core.ClaimAccessor;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.stereotype.Component;

/**
 * Examines a JWT, works out the passport filters and adds the appropriate Pathling authorities to
 * the security context.
 *
 * @author John Grimes
 */
@Component
@Profile("server & ga4gh")
@Slf4j
public class PassportAuthenticationConverter extends JwtAuthenticationConverter {

  /**
   * @param visaDecoderFactory a factory for creating a visa {@link JwtDecoder}
   * @param configuration for use in creating the JWT decoder
   * @param manifestConverter a {@link ManifestConverter} that we use to convert the manifest into a
   * set of query scopes
   * @param passportScope a request-scoped {@link PassportScope} used to store the extracted
   * filters
   */
  public PassportAuthenticationConverter(@Nonnull final VisaDecoderFactory visaDecoderFactory,
      @Nonnull final ServerConfiguration configuration,
      @Nonnull final ManifestConverter manifestConverter,
      @Nonnull final PassportScope passportScope) {
    log.debug("Instantiating passport authentication converter");
    final JwtDecoder visaDecoder = visaDecoderFactory.createDecoder(configuration);
    final Converter<Jwt, Collection<GrantedAuthority>> authoritiesConverter = new PassportAuthoritiesConverter(
        visaDecoder, manifestConverter, passportScope);
    setJwtGrantedAuthoritiesConverter(authoritiesConverter);
  }

  private static class PassportAuthoritiesConverter implements
      Converter<Jwt, Collection<GrantedAuthority>> {

    private static final String PASSPORT_CLAIM_NAME = "ga4gh_passport_v1";
    private static final String VISAS_CLAIM_NAME = "ga4gh_visa_v1";
    private static final Collection<GrantedAuthority> IMPLIED_AUTHORITIES = Arrays.asList(
        new SimpleGrantedAuthority("pathling:aggregate"),
        new SimpleGrantedAuthority("pathling:search"),
        new SimpleGrantedAuthority("pathling:extract")
    );
    private static final String VISA_TYPE_CLAIM = "type";
    private static final String VISA_DATASET_ID_CLAIM = "value";

    @Nonnull
    private final JwtDecoder jwtDecoder;

    @Nonnull
    private final ManifestConverter manifestConverter;

    @Nonnull
    private final PassportScope passportScope;

    /**
     * @param jwtDecoder a {@link JwtDecoder} that we can use to decode embedded visas
     * @param manifestConverter a {@link ManifestConverter} that we use to convert the manifest into
     * a set of query scopes
     * @param passportScope a request-scoped {@link PassportScope} used to store the extracted
     * filters
     */
    private PassportAuthoritiesConverter(@Nonnull final JwtDecoder jwtDecoder,
        @Nonnull final ManifestConverter manifestConverter,
        @Nonnull final PassportScope passportScope) {
      this.jwtDecoder = jwtDecoder;
      this.manifestConverter = manifestConverter;
      this.passportScope = passportScope;
    }

    @Nonnull
    @Override
    public Collection<GrantedAuthority> convert(@Nullable final Jwt credentials) {
      requireNonNull(credentials);

      final List<String> encodedVisas = credentials.getClaimAsStringList(PASSPORT_CLAIM_NAME);
      checkToken(() -> requireNonNull(encodedVisas),
          "No " + PASSPORT_CLAIM_NAME + " claim");

      for (final String encodedVisa : encodedVisas) {
        // Decode each visa using the same decoder we use for the primary access token.
        final Jwt visa = jwtDecoder.decode(encodedVisa);

        // Get the main visa claim and make sure it has the expected visa type.
        final String datasetId = getControlledAccessDatasetId(visa);

        // Get the issuer and use it to retrieve the manifest.
        final VisaManifest manifest;
        try {
          manifest = getManifest(visa.getIssuer(), datasetId);
        } catch (final IOException e) {
          throw new RuntimeException(e);
        }
        log.debug("Manifest for dataset {}: {}", datasetId, manifest);

        // Translate each patient ID in the manifest into a set of filters, and add to the passport
        // scope.
        manifestConverter.populateScope(passportScope, manifest);
      }

      log.debug("Resolved passport filters: {}", passportScope);

      // We only add read authority for resources which have filters specified for them. This means
      // that the visa is essentially interpreted as a whitelist of criteria, and unfiltered access
      // is never granted to a resource type (unless a filter is explicitly specified to that effect).
      final Collection<GrantedAuthority> authorities = new ArrayList<>(IMPLIED_AUTHORITIES);
      for (final ResourceType resourceType : passportScope.keySet()) {
        if (!passportScope.get(resourceType).isEmpty()) {
          authorities.add(new SimpleGrantedAuthority("pathling:read:" + resourceType.toCode()));
        }
      }

      log.debug("Resolved passport authorities: {}", authorities);
      return authorities;
    }

    @Nonnull
    private String getControlledAccessDatasetId(@Nonnull final ClaimAccessor visa) {
      final JSONObject visasClaim = visa.getClaim(VISAS_CLAIM_NAME);
      if (visasClaim == null) {
        throw new UnclassifiedServerFailureException(502, "Visa is wrong type");
      }
      final String visaType = visasClaim.get(VISA_TYPE_CLAIM).toString();
      final String visaDatasetId = visasClaim.get(VISA_DATASET_ID_CLAIM).toString();
      if (visaType == null || visaDatasetId == null) {
        throw new UnclassifiedServerFailureException(502,
            "Visa is wrong type, or is missing dataset ID");
      }
      return visaDatasetId;
    }

    @Nonnull
    private VisaManifest getManifest(@Nonnull final URL issuer, @Nonnull final String datasetId)
        throws IOException {
      try (final CloseableHttpClient httpClient = HttpClients.createDefault()) {
        final VisaManifest manifest;
        final String manifestUrl = issuer + "/api/manifest/" + datasetId;
        try {
          log.debug("Retrieving manifest from {}", issuer);
          final HttpUriRequest get = new HttpGet(manifestUrl);
          manifest = httpClient.execute(get, this::manifestResponseHandler);
        } catch (final IOException e) {
          throw new UnclassifiedServerFailureException(502, "Problem retrieving manifest for visa");
        }
        return manifest;
      }
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

    @SuppressWarnings("SameParameterValue")
    private void checkToken(@Nonnull final Runnable check, @Nonnull final String message) {
      try {
        check.run();
      } catch (final Exception e) {
        final UnclassifiedServerFailureException error = new UnclassifiedServerFailureException(
            502, message);
        error.initCause(e);
        throw error;
      }
    }

  }
}
