/*
 * Copyright CSIRO Australian e-Health Research Centre (http://aehrc.com). All rights reserved. Use is subject to
 * license terms and conditions.
 */

package au.csiro.clinsight.fhir;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.api.EncodingEnum;
import ca.uhn.fhir.rest.server.IResourceProvider;
import ca.uhn.fhir.rest.server.RestfulServer;
import ca.uhn.fhir.rest.server.interceptor.CorsInterceptor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.servlet.ServletException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.cors.CorsConfiguration;

/**
 * @author John Grimes
 */
@Component
public class FhirServer extends RestfulServer {

  private DimensionAttributeResourceProvider dimensionAttributeResourceProvider;
  private MetricResourceProvider metricResourceProvider;
  private QueryResourceProvider queryResourceProvider;
  private QueryOperationProvider queryOperationProvider;
  private DimensionResourceProvider dimensionResourceProvider;

  @Autowired
  public FhirServer(FhirContext theCtx,
      DimensionAttributeResourceProvider dimensionAttributeResourceProvider,
      MetricResourceProvider metricResourceProvider,
      QueryResourceProvider queryResourceProvider,
      QueryOperationProvider queryOperationProvider,
      DimensionResourceProvider dimensionResourceProvider) {
    super(theCtx);
    this.dimensionAttributeResourceProvider = dimensionAttributeResourceProvider;
    this.metricResourceProvider = metricResourceProvider;
    this.queryResourceProvider = queryResourceProvider;
    this.queryOperationProvider = queryOperationProvider;
    this.dimensionResourceProvider = dimensionResourceProvider;
  }

  @Override
  protected void initialize() throws ServletException {
    super.initialize();

    // Set default response encoding to JSON.
    setDefaultResponseEncoding(EncodingEnum.JSON);

    declareResourceProviders();
    defineCorsConfiguration();
  }

  private void declareResourceProviders() {
    List<IResourceProvider> resourceProviders = new ArrayList<>();
    resourceProviders.add(dimensionResourceProvider);
    resourceProviders.add(dimensionAttributeResourceProvider);
    resourceProviders.add(metricResourceProvider);
    resourceProviders.add(queryResourceProvider);
    setResourceProviders(resourceProviders);
    List<Object> plainProviders = new ArrayList<>();
    plainProviders.add(queryOperationProvider);
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
