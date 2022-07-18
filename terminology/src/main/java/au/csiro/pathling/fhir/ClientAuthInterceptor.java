/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhir;

import static au.csiro.pathling.utilities.Preconditions.checkNotNull;

import ca.uhn.fhir.interceptor.api.Hook;
import ca.uhn.fhir.interceptor.api.Interceptor;
import ca.uhn.fhir.interceptor.api.Pointcut;
import ca.uhn.fhir.rest.client.api.IHttpRequest;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.Header;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

@Interceptor
@Slf4j
public class ClientAuthInterceptor {

  @Nonnull
  private final String tokenEndpoint;

  @Nonnull
  private final String clientId;

  @Nonnull
  private final String clientSecret;

  @Nullable
  private final String scope;

  private final long tokenExpiryTolerance;

  @Nonnull
  private static final Map<AccessScope, AccessContext> accessContexts = new HashMap<>();

  public ClientAuthInterceptor(@Nonnull final String tokenEndpoint, @Nonnull final String clientId,
      @Nonnull final String clientSecret, @Nullable final String scope,
      final long tokenExpiryTolerance) {
    this.tokenEndpoint = tokenEndpoint;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.scope = scope;
    this.tokenExpiryTolerance = tokenExpiryTolerance;
  }

  @SuppressWarnings("unused")
  @Hook(Pointcut.CLIENT_REQUEST)
  public void handleClientRequest(@Nullable final IHttpRequest httpRequest) throws IOException {
    if (httpRequest != null) {
      final AccessContext accessContext = ensureAccessContext(clientId, clientSecret, tokenEndpoint,
          scope, tokenExpiryTolerance);
      // Now we should have a valid token, so we can add it to the request.
      final String accessToken = accessContext.getClientCredentialsResponse().getAccessToken();
      checkNotNull(accessToken);
      httpRequest.addHeader("Authorization", "Bearer " + accessToken);
    }
  }

  @Nonnull
  private static AccessContext ensureAccessContext(@Nonnull final String clientId,
      @Nonnull final String clientSecret, @Nonnull final String tokenEndpoint,
      @Nonnull final String scope, final long tokenExpiryTolerance)
      throws IOException {
    synchronized (accessContexts) {
      final AccessScope accessScope = new AccessScope(tokenEndpoint, clientId, scope);
      AccessContext accessContext = accessContexts.get(accessScope);
      if (accessContext == null || accessContext.getExpiryTime()
          .isBefore(Instant.now().plusSeconds(tokenExpiryTolerance))) {
        // We need to get a new token if:
        // (1) We don't have a token yet;
        // (2) The token is expired, or;
        // (3) The token is about to expire (within the tolerance).
        log.debug("Getting new token");
        accessContext = getNewAccessContext(clientId, clientSecret, tokenEndpoint, scope,
            tokenExpiryTolerance);
        accessContexts.put(accessScope, accessContext);
      }
      return accessContext;
    }
  }

  @Nonnull
  private static AccessContext getNewAccessContext(@Nonnull final String clientId,
      @Nonnull final String clientSecret, @Nonnull final String tokenEndpoint,
      @Nullable final String scope, final long tokenExpiryTolerance)
      throws IOException {
    final List<NameValuePair> authParams = new ArrayList<>();
    authParams.add(new BasicNameValuePair("client_id", clientId));
    authParams.add(new BasicNameValuePair("client_secret", clientSecret));
    return getAccessContext(authParams, tokenEndpoint, scope, tokenExpiryTolerance);
  }

  @Nonnull
  private static AccessContext getAccessContext(@Nonnull final List<NameValuePair> authParams,
      @Nonnull final String tokenEndpoint, @Nullable final String scope,
      final long tokenExpiryTolerance) throws IOException {
    final ClientCredentialsResponse response = clientCredentialsGrant(authParams, tokenEndpoint,
        scope, tokenExpiryTolerance);
    final Instant expires = getExpiryTime(response);
    log.debug("New token will expire at {}", expires);
    return new AccessContext(response, expires);
  }

  @Nonnull
  private static ClientCredentialsResponse clientCredentialsGrant(
      @Nonnull final List<NameValuePair> authParams, @Nonnull final String tokenEndpoint,
      @Nullable final String scope, final long tokenExpiryTolerance)
      throws IOException {
    log.debug("Performing client credentials grant using token endpoint: {}", tokenEndpoint);
    try (final CloseableHttpClient httpClient = HttpClients.createDefault()) {
      final HttpPost request = new HttpPost(tokenEndpoint);
      request.addHeader("Content-Type", "application/x-www-form-urlencoded");
      request.addHeader("Accept", "application/json");

      final List<NameValuePair> params = new ArrayList<>();
      params.add(new BasicNameValuePair("grant_type", "client_credentials"));
      if (scope != null) {
        authParams.add(new BasicNameValuePair("scope", scope));
      }
      params.addAll(authParams);
      request.setEntity(new UrlEncodedFormEntity(params));

      final CloseableHttpResponse response = httpClient.execute(request);
      @Nullable final Header contentTypeHeader = response.getFirstHeader("Content-Type");
      if (contentTypeHeader == null) {
        throw new ClientProtocolException(
            "Client credentials response contains no Content-Type header");
      }
      final boolean responseIsJson = contentTypeHeader.getValue()
          .startsWith("application/json");
      if (!responseIsJson) {
        throw new ClientProtocolException(
            "Invalid response from token endpoint: content type is not application/json");
      }
      final Gson gson = new GsonBuilder()
          .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
          .create();
      final String responseString = EntityUtils.toString(response.getEntity());
      final ClientCredentialsResponse grant = gson.fromJson(
          responseString,
          ClientCredentialsResponse.class);
      if (grant.getAccessToken() == null) {
        throw new ClientProtocolException("Client credentials grant does not contain access token");
      }
      if (grant.getExpiresIn() < tokenExpiryTolerance) {
        throw new ClientProtocolException(
            "Client credentials grant expiry is less than the tolerance: " + grant.getExpiresIn());
      }
      return grant;
    }
  }

  private static Instant getExpiryTime(@Nonnull final ClientCredentialsResponse response) {
    return Instant.now().plusSeconds(response.getExpiresIn());
  }

  public static void clearAccessContexts() {
    synchronized (accessContexts) {
      accessContexts.clear();
    }
  }

}
