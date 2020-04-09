/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhir;

import ca.uhn.fhir.interceptor.api.Hook;
import ca.uhn.fhir.interceptor.api.Interceptor;
import ca.uhn.fhir.interceptor.api.Pointcut;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * This class intercepts requests to `.well-known/smart-configuration` and returns a Well-Known
 * Uniform Resource Identifiers document with the configured authorization URIs.
 *
 * @author John Grimes
 */
@Interceptor
public class SmartConfigurationInterceptor {

  private final AnalyticsServerConfiguration configuration;

  public SmartConfigurationInterceptor(
      AnalyticsServerConfiguration configuration) {
    this.configuration = configuration;
  }

  @Hook(Pointcut.SERVER_INCOMING_REQUEST_PRE_PROCESSED)
  public boolean serveUris(HttpServletRequest servletRequest, HttpServletResponse servletResponse)
      throws IOException {
    boolean smartUrisConfigured =
        configuration.getAuthorizeUrl() != null || configuration.getTokenUrl() != null
            || configuration.getRevokeTokenUrl() != null;
    if (servletRequest.getPathInfo() != null
        && servletRequest.getPathInfo().equals("/.well-known/smart-configuration")
        && smartUrisConfigured) {
      SmartConfiguration smartConfiguration = new SmartConfiguration();
      smartConfiguration.setAuthorizationEndpoint(configuration.getAuthorizeUrl());
      smartConfiguration.setTokenEndpoint(configuration.getTokenUrl());
      smartConfiguration.setRevocationEndpoint(configuration.getRevokeTokenUrl());

      Gson gson = new GsonBuilder()
          .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
          .create();
      String json = gson.toJson(smartConfiguration);

      servletResponse.setStatus(200);
      servletResponse.setContentType("application/json");
      servletResponse.getWriter().append(json);
      return false;
    } else {
      return true;
    }
  }

  static class SmartConfiguration {

    private String authorizationEndpoint;
    private String tokenEndpoint;
    private String revocationEndpoint;
    private List<String> capabilities = Collections.singletonList("launch-standalone");

    public SmartConfiguration() {
    }

    public String getAuthorizationEndpoint() {
      return authorizationEndpoint;
    }

    public void setAuthorizationEndpoint(String authorizationEndpoint) {
      this.authorizationEndpoint = authorizationEndpoint;
    }

    public String getTokenEndpoint() {
      return tokenEndpoint;
    }

    public void setTokenEndpoint(String tokenEndpoint) {
      this.tokenEndpoint = tokenEndpoint;
    }

    public String getRevocationEndpoint() {
      return revocationEndpoint;
    }

    public void setRevocationEndpoint(String revocationEndpoint) {
      this.revocationEndpoint = revocationEndpoint;
    }

    public List<String> getCapabilities() {
      return capabilities;
    }

    public void setCapabilities(List<String> capabilities) {
      this.capabilities = capabilities;
    }
  }

}
