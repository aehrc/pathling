/*
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use is subject to
 * license terms and conditions.
 */

package au.csiro.clinsight.fhir;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.api.EncodingEnum;
import ca.uhn.fhir.rest.server.RestfulServer;
import ca.uhn.fhir.rest.server.interceptor.CorsInterceptor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.servlet.ServletException;
import org.hibernate.SessionFactory;
import org.springframework.web.cors.CorsConfiguration;

/**
 * @author John Grimes
 */
public class FhirServer extends RestfulServer {

  private FhirServerConfiguration configuration;
  private SessionFactory sessionFactory;
  private QueryExecutor queryExecutor;

  public FhirServer(FhirServerConfiguration configuration) {
    super(FhirContext.forDstu3());
    this.configuration = configuration;
  }

  @Override
  protected void initialize() throws ServletException {
    super.initialize();

    // Set default response encoding to JSON.
    setDefaultResponseEncoding(EncodingEnum.JSON);

    declareProviders();
    defineCorsConfiguration();
  }

  private void declareProviders() {
    List<Object> plainProviders = new ArrayList<>();
    plainProviders.add(new QueryOperationProvider(queryExecutor));
    setPlainProviders(plainProviders);
  }

  private void defineCorsConfiguration() {
    CorsConfiguration config = new CorsConfiguration();
    config.addAllowedHeader("x-fhir-starter");
    config.addAllowedHeader("Origin");
    config.addAllowedHeader("Accept");
    config.addAllowedHeader("X-Requested-With");
    config.addAllowedHeader("Content-Type");

    config.addAllowedOrigin("*");

    config.addExposedHeader("Location");
    config.addExposedHeader("Content-Location");
    config.setAllowedMethods(Arrays.asList("GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"));

    CorsInterceptor interceptor = new CorsInterceptor(config);
    registerInterceptor(interceptor);
  }

}
