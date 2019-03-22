/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.clinsight.fhir.AnalyticsServer;
import au.csiro.clinsight.fhir.AnalyticsServerConfiguration;
import au.csiro.clinsight.resources.AggregateQuery;
import au.csiro.clinsight.resources.AggregateQueryResult;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import com.google.common.collect.Sets;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.spark.sql.Dataset;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.hl7.fhir.dstu3.model.Bundle;
import org.hl7.fhir.dstu3.model.IdType;
import org.hl7.fhir.dstu3.model.ResourceType;
import org.hl7.fhir.dstu3.model.StructureDefinition;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.mockito.stubbing.Answer;

/**
 * @author John Grimes
 */
abstract class TestConfiguration {

  private static final int FHIR_SERVER_PORT = 8075;
  static final String FHIR_SERVER_URL = "http://localhost:" + FHIR_SERVER_PORT + "/fhir";

  private static final FhirContext fhirContext = initialiseFhirContext();
  static final IParser jsonParser = fhirContext.newJsonParser();

  private static final InputStream resourceStream = Thread.currentThread().getContextClassLoader()
      .getResourceAsStream("profiles-resources.json");
  private static final InputStream typeStream = Thread.currentThread().getContextClassLoader()
      .getResourceAsStream("profiles-types.json");

  private static final Bundle resourceDefinitionBundle = (Bundle) jsonParser
      .parseResource(resourceStream);
  private static final Bundle typeDefinitionBundle = (Bundle) jsonParser.parseResource(typeStream);

  private static final Map<String, StructureDefinition> resources = resourceDefinitionBundle
      .getEntry()
      .stream()
      .filter(entry -> entry.getResource().getResourceType() == ResourceType.StructureDefinition)
      .map(entry -> (StructureDefinition) entry.getResource())
      .collect(Collectors.toMap(StructureDefinition::getUrl, sd -> sd));
  private static final Map<String, StructureDefinition> complexTypes = typeDefinitionBundle
      .getEntry()
      .stream()
      .filter(entry -> entry.getResource().getResourceType() == ResourceType.StructureDefinition)
      .map(entry -> (StructureDefinition) entry.getResource())
      .collect(Collectors.toMap(StructureDefinition::getUrl, sd -> sd));

  private static FhirContext initialiseFhirContext() {
    FhirContext fhirContext = FhirContext.forDstu3();
    fhirContext.registerCustomType(AggregateQuery.class);
    fhirContext.registerCustomType(AggregateQueryResult.class);
    return fhirContext;
  }

  static Server startFhirServer(AnalyticsServerConfiguration configuration) throws Exception {
    Server server = new Server(FHIR_SERVER_PORT);
    ServletHandler servletHandler = new ServletHandler();
    ServletHolder servletHolder = new ServletHolder(new AnalyticsServer(configuration));
    servletHandler.addServletWithMapping(servletHolder, "/fhir/*");
    server.setHandler(servletHandler);
    server.start();
    return server;
  }

  static void mockDefinitionRetrieval(TerminologyClient terminologyClient) {
    List<StructureDefinition> allDefinitions = new ArrayList<>(resources.values());
    allDefinitions.addAll(complexTypes.values());
    when(terminologyClient.getAllStructureDefinitions(Sets.newHashSet("url", "kind")))
        .thenReturn(allDefinitions);
    when(terminologyClient.getStructureDefinitionById(any(IdType.class))).thenAnswer(
        (Answer<StructureDefinition>) invocation -> {
          IdType theId = invocation.getArgument(0);
          return allDefinitions.stream()
              .filter(sd -> sd.getId().equals(theId.asStringValue()))
              .findFirst()
              .orElse(null);
        }
    );
  }

  static Dataset createMockDataset() {
    return mock(Dataset.class);
  }

  @Nonnull
  static HttpPost postFhirResource(IBaseResource resource, String url)
      throws UnsupportedEncodingException {
    String json = jsonParser.encodeResourceToString(resource);
    HttpPost httpPost = new HttpPost(url);
    httpPost.setEntity(new StringEntity(json));
    httpPost.addHeader("Content-Type", "application/fhir+json");
    httpPost.addHeader("Accept", "application/fhir+json");
    return httpPost;
  }
}
