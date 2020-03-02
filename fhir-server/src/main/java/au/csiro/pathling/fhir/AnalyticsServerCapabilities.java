/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhir;

import au.csiro.pathling.query.ResourceReader;
import ca.uhn.fhir.rest.annotation.Metadata;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.server.IServerConformanceProvider;
import ca.uhn.fhir.rest.server.RestfulServer;
import java.util.*;
import javax.annotation.Nonnull;
import javax.servlet.http.HttpServletRequest;
import org.hl7.fhir.r4.model.*;
import org.hl7.fhir.r4.model.CapabilityStatement.*;
import org.hl7.fhir.r4.model.Enumerations.FHIRVersion;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides a customised CapabilityStatement describing the functionality of the
 * analytics server.
 *
 * @author John Grimes
 */
public class AnalyticsServerCapabilities implements
    IServerConformanceProvider<CapabilityStatement> {

  private static final Logger logger = LoggerFactory.getLogger(AnalyticsServerCapabilities.class);
  private final AnalyticsServerConfiguration configuration;
  private final ResourceReader resourceReader;

  public AnalyticsServerCapabilities(AnalyticsServerConfiguration configuration,
      ResourceReader resourceReader) {
    this.configuration = configuration;
    this.resourceReader = resourceReader;
  }

  @Override
  @Metadata
  public CapabilityStatement getServerConformance(HttpServletRequest httpServletRequest,
      RequestDetails requestDetails) {
    logger.info("Received request for server capabilities");
    CapabilityStatement capabilityStatement = new CapabilityStatement();
    capabilityStatement
        .setUrl("https://server.pathling.app/fhir/CapabilityStatement/pathling-fhir-api-1");
    capabilityStatement.setVersion("1.0.0");
    capabilityStatement.setTitle("Pathling FHIR API");
    capabilityStatement.setName("pathling-fhir-api");
    capabilityStatement.setStatus(PublicationStatus.ACTIVE);
    capabilityStatement.setExperimental(true);
    capabilityStatement.setPublisher("Australian e-Health Research Centre, CSIRO");
    capabilityStatement.setCopyright(
        "Dedicated to the public domain via CC0: https://creativecommons.org/publicdomain/zero/1.0/");
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
    CapabilityStatementRestResourceComponent opDefResource = null;
    Set<Enumerations.ResourceType> availableResourceTypes = EnumSet
        .copyOf(resourceReader.getAvailableResourceTypes());
    availableResourceTypes.add(Enumerations.ResourceType.OPERATIONDEFINITION);

    for (Enumerations.ResourceType resourceType : availableResourceTypes) {
      // Add the `fhirPath` search parameter to all resources.
      CapabilityStatementRestResourceComponent resource = new CapabilityStatementRestResourceComponent(
          new CodeType(resourceType.toCode()));
      CapabilityStatementRestResourceOperationComponent searchOperation = new CapabilityStatementRestResourceOperationComponent();
      searchOperation.setName("fhirPath");
      searchOperation
          .setDefinition("https://server.pathling.app/fhir/OperationDefinition/search-1");
      resource.addOperation(searchOperation);
      resources.add(resource);

      // Save away the OperationDefinition resource, so that we can later add the read operation to
      // it.
      if (resourceType.toCode().equals("OperationDefinition")) {
        opDefResource = resource;
      }
    }

    // Add the read operation to the StructureDefinition and OperationDefinition resources.
    assert opDefResource != null;
    ResourceInteractionComponent readInteraction = new ResourceInteractionComponent();
    readInteraction.setCode(TypeRestfulInteraction.READ);
    opDefResource.addInteraction(readInteraction);

    return resources;
  }

  private List<CapabilityStatementRestResourceOperationComponent> buildOperations() {
    List<CapabilityStatementRestResourceOperationComponent> operations = new ArrayList<>();

    CanonicalType aggregateOperationUri = new CanonicalType(
        "https://server.pathling.app/fhir/OperationDefinition/aggregate-1");
    CapabilityStatementRestResourceOperationComponent aggregateOperation = new CapabilityStatementRestResourceOperationComponent(
        new StringType("aggregate"), aggregateOperationUri);

    CanonicalType importOperationUri = new CanonicalType(
        "https://server.pathling.app/fhir/OperationDefinition/import-1");
    CapabilityStatementRestResourceOperationComponent importOperation = new CapabilityStatementRestResourceOperationComponent(
        new StringType("import"), importOperationUri);

    operations.add(aggregateOperation);
    operations.add(importOperation);
    return operations;
  }

  @Override
  public void setRestfulServer(RestfulServer restfulServer) {
  }

}
