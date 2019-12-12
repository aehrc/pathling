/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.pathling.fhir;

import au.csiro.pathling.query.AggregateExecutor;
import ca.uhn.fhir.rest.annotation.Metadata;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.server.IServerConformanceProvider;
import ca.uhn.fhir.rest.server.RestfulServer;
import ca.uhn.fhir.rest.server.exceptions.UnclassifiedServerFailureException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletRequest;
import org.hl7.fhir.r4.model.*;
import org.hl7.fhir.r4.model.CapabilityStatement.*;
import org.hl7.fhir.r4.model.Enumerations.FHIRVersion;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;

/**
 * This class provides a customised CapabilityStatement describing the functionality of the
 * analytics server.
 *
 * @author John Grimes
 */
public class AnalyticsServerCapabilities implements
    IServerConformanceProvider<CapabilityStatement> {

  private AnalyticsServerConfiguration configuration;
  private AggregateExecutor aggregateExecutor;

  public AnalyticsServerCapabilities(AnalyticsServerConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  @Metadata
  public CapabilityStatement getServerConformance(HttpServletRequest httpServletRequest,
      RequestDetails requestDetails) {
    checkServerHealth();
    CapabilityStatement capabilityStatement = new CapabilityStatement();
    Meta meta = new Meta();
    meta.addProfile(
        "https://server.pathling.app/fhir/StructureDefinition/fhir-server-capabilities-0");
    capabilityStatement.setMeta(meta);
    capabilityStatement
        .setUrl("https://server.pathling.app/fhir/CapabilityStatement/pathling-fhir-api-0");
    capabilityStatement.setVersion("0.0.0");
    capabilityStatement.setTitle("Pathling FHIR API");
    capabilityStatement.setName("pathling-fhir-api");
    capabilityStatement.setStatus(PublicationStatus.DRAFT);
    capabilityStatement.setExperimental(true);
    capabilityStatement.setPublisher("Australian e-Health Research Centre, CSIRO");
    capabilityStatement.setCopyright(
        "Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.");
    capabilityStatement.setKind(CapabilityStatementKind.CAPABILITY);
    CapabilityStatementSoftwareComponent software = new CapabilityStatementSoftwareComponent(
        new StringType("Pathling FHIR Server"));
    software.setVersion(configuration.getVersion());
    capabilityStatement.setSoftware(software);
    capabilityStatement.setFhirVersion(FHIRVersion._4_0_0);
    capabilityStatement.setFormat(Arrays.asList(new CodeType("json"), new CodeType("xml")));
    capabilityStatement.setRest(buildRestComponent());
    return capabilityStatement;
  }

  private void checkServerHealth() {
    if (aggregateExecutor == null || !aggregateExecutor.isReady()) {
      throw new UnclassifiedServerFailureException(503,
          "Server is not currently available for query, check with your server administrator");
    }
  }

  @Nonnull
  private List<CapabilityStatementRestComponent> buildRestComponent() {
    List<CapabilityStatementRestComponent> rest = new ArrayList<>();
    CapabilityStatementRestComponent server = new CapabilityStatementRestComponent();
    server.setMode(RestfulCapabilityMode.SERVER);
    server.setResource(buildResources());
    server.setOperation(buildOperations());
    rest.add(server);
    return rest;
  }

  private List<CapabilityStatementRestResourceComponent> buildResources() {
    List<CapabilityStatementRestResourceComponent> resources = new ArrayList<>();
    CapabilityStatementRestResourceComponent strucDefResource = new CapabilityStatementRestResourceComponent(
        new CodeType("StructureDefinition"));
    ResourceInteractionComponent readInteraction = new ResourceInteractionComponent();
    readInteraction.setCode(TypeRestfulInteraction.READ);
    strucDefResource.addInteraction(readInteraction);
    CapabilityStatementRestResourceComponent opDefResource = new CapabilityStatementRestResourceComponent(
        new CodeType("OperationDefinition"));
    opDefResource.addInteraction(readInteraction);
    resources.add(strucDefResource);
    resources.add(opDefResource);
    return resources;
  }

  private List<CapabilityStatementRestResourceOperationComponent> buildOperations() {
    List<CapabilityStatementRestResourceOperationComponent> operations = new ArrayList<>();

    CanonicalType aggregateOperationUri = new CanonicalType(
        "https://server.pathling.app/fhir/OperationDefinition/aggregate-0");
    CapabilityStatementRestResourceOperationComponent aggregateOperation = new CapabilityStatementRestResourceOperationComponent(
        new StringType("aggregate"), aggregateOperationUri);
    for (Enumerations.ResourceType resourceType : aggregateExecutor.getAvailableResourceTypes()) {
      Extension extension = new Extension();
      extension
          .setUrl("https://server.pathling.app/fhir/StructureDefinition/available-resource-type-0");
      extension.setValue(new CodeType(resourceType.toCode()));
      aggregateOperation.addExtension(extension);
    }

    CanonicalType importOperationUri = new CanonicalType(
        "https://server.pathling.app/fhir/OperationDefinition/import-0");
    CapabilityStatementRestResourceOperationComponent importOperation = new CapabilityStatementRestResourceOperationComponent(
        new StringType("import"), importOperationUri);

    operations.add(aggregateOperation);
    operations.add(importOperation);
    return operations;
  }

  @Override
  public void setRestfulServer(RestfulServer restfulServer) {
  }

  public void setAggregateExecutor(AggregateExecutor aggregateExecutor) {
    this.aggregateExecutor = aggregateExecutor;
  }

}
