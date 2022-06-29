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
import ca.uhn.fhir.rest.client.api.IHttpResponse;
import ca.uhn.fhir.rest.client.api.IRestfulClient;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
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

  @Nullable
  private static ClientCredentialsResponse token = null;

  @Nullable
  private static Instant expires = null;

  // TODO: Make the static variables into a map, that is keyed on token endpoint, client ID and scope.

  public ClientAuthInterceptor(@Nonnull final String tokenEndpoint, @Nonnull final String clientId,
      @Nonnull final String clientSecret, @Nullable final String scope) {
    this.tokenEndpoint = tokenEndpoint;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.scope = scope;
  }

  @Hook(Pointcut.CLIENT_REQUEST)
  public void handleClientRequest(@Nullable final IHttpRequest httpRequest) throws IOException {
    if (httpRequest != null) {
      final ClientCredentialsResponse currentToken = checkAndUpdateToken(clientId, clientSecret,
          tokenEndpoint, scope);
      // Now we should have a token, so we can add it to the request.
      checkNotNull(currentToken);
      checkNotNull(currentToken.getAccessToken());
      httpRequest.addHeader("Authorization", "Bearer " + currentToken.getAccessToken());
    }
  }

  @Hook(Pointcut.CLIENT_RESPONSE)
  public void handleClientResponse(@Nullable final IHttpRequest httpRequest,
      @Nullable final IHttpResponse httpResponse, @Nullable final IRestfulClient client)
      throws IOException {
    if (httpResponse != null && httpResponse.getStatus() == 401) {
      log.debug("Received 401 response from server, attempting to retrieve a new token");
      checkAndUpdateToken(clientId, clientSecret, tokenEndpoint, scope);
    }
    // The HttpClient retry mechanism will take care of retrying the request.
  }

  private static synchronized ClientCredentialsResponse checkAndUpdateToken(final String clientId,
      final String clientSecret, final String tokenEndpoint, final String scope)
      throws IOException {
    if (token == null) {
      // If we don't have a token, we need to get one.
      token = getToken(clientId, clientSecret, tokenEndpoint, scope);
      updateExpiry();
    } else if (expires != null && expires.isBefore(Instant.now())
        && token.getRefreshToken() != null) {
      // If we have a token, but it's expired, we need to refresh it.
      token = refreshToken(token.getRefreshToken(), tokenEndpoint, scope);
      updateExpiry();
    }
    return token;
  }

  @Nonnull
  private static ClientCredentialsResponse getToken(final String clientId,
      final String clientSecret, final String tokenEndpoint, final String scope)
      throws IOException {
    final List<NameValuePair> authParams = new ArrayList<>();
    authParams.add(new BasicNameValuePair("client_id", clientId));
    authParams.add(new BasicNameValuePair("client_secret", clientSecret));
    return clientCredentialsGrant(authParams, tokenEndpoint, scope);
  }

  @Nonnull
  private static ClientCredentialsResponse refreshToken(@Nonnull final String refreshToken,
      final String tokenEndpoint, final String scope)
      throws IOException {
    log.debug("Refreshing token, expired at: {}", expires);
    final List<NameValuePair> authParams = new ArrayList<>();
    authParams.add(new BasicNameValuePair("refresh_token", refreshToken));
    return clientCredentialsGrant(authParams, tokenEndpoint, scope);
  }

  @Nonnull
  private static ClientCredentialsResponse clientCredentialsGrant(
      @Nonnull final List<NameValuePair> authParams, final String tokenEndpoint, final String scope)
      throws IOException {
    log.debug("Performing client credentials grant using token endpoint: {}", tokenEndpoint);
    try (final CloseableHttpClient httpClient = HttpClients.createDefault()) {
      final HttpPost request = new HttpPost(tokenEndpoint);
      request.addHeader("Content-Type", "application/x-www-form-urlencoded");
      request.addHeader("Accept", "application/json");

      final List<NameValuePair> params = new ArrayList<>();
      params.add(new BasicNameValuePair("grant_type", "client_credentials"));
      params.addAll(authParams);
      if (scope != null) {
        authParams.add(new BasicNameValuePair("scope", scope));
      }
      request.setEntity(new UrlEncodedFormEntity(params));

      final CloseableHttpResponse response = httpClient.execute(request);
      final boolean responseIsJson = response.getFirstHeader("Content-Type").getValue()
          .startsWith("application/json");
      if (!responseIsJson) {
        throw new ClientProtocolException(
            "Invalid response from token endpoint: content type is not application/json");
      }
      final Gson gson = new GsonBuilder()
          .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
          .create();
      final ClientCredentialsResponse grant = gson.fromJson(
          EntityUtils.toString(response.getEntity()),
          ClientCredentialsResponse.class);
      if (grant.getAccessToken() == null) {
        throw new ClientProtocolException(
            "Client credentials grant does not contain access token");
      }
      return grant;
    }
  }

  private static void updateExpiry() {
    checkNotNull(token);
    expires = Instant.now().plusSeconds(token.getExpiresIn());
    log.debug("New token will expire at {}", expires);
  }

}
