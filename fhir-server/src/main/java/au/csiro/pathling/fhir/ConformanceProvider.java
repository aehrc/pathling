/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhir;

import static au.csiro.pathling.utilities.Versioning.getMajorVersion;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.Configuration.Authorisation;
import au.csiro.pathling.io.ResourceReader;
import ca.uhn.fhir.rest.annotation.Metadata;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.server.IServerConformanceProvider;
import ca.uhn.fhir.rest.server.RestfulServer;
import java.util.*;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.*;
import org.hl7.fhir.r4.model.CapabilityStatement.*;
import org.hl7.fhir.r4.model.Enumerations.FHIRVersion;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * This class provides a customised CapabilityStatement describing the functionality of the
 * analytics server.
 *
 * @author John Grimes
 */
@Component
@Profile("server")
@Slf4j
public class ConformanceProvider implements IServerConformanceProvider<CapabilityStatement> {

  private static final String URI_BASE = "https://pathling.csiro.au/fhir";
  private static final String FHIR_RESOURCE_BASE = "http://hl7.org/fhir/StructureDefinition/";
  private static final String RESTFUL_SECURITY_URI = "http://terminology.hl7.org/CodeSystem/restful-security-service";
  private static final String RESTFUL_SECURITY_CODE = "SMART-on-FHIR";
  private static final String SMART_OAUTH_URI = "http://fhir-registry.smarthealthit.org/StructureDefinition/oauth-uris";

  @Nonnull
  private final au.csiro.pathling.Configuration configuration;

  @Nonnull
  private final ResourceReader resourceReader;

  @Nonnull
  private Optional<RestfulServer> restfulServer;

  /**
   * @param configuration A {@link Configuration} object controlling the behaviour of the capability
   * statement
   * @param resourceReader A {@link ResourceReader} to use in checking which resources are
   * available
   */
  public ConformanceProvider(@Nonnull final Configuration configuration,
      @Nonnull final ResourceReader resourceReader) {
    this.configuration = configuration;
    this.resourceReader = resourceReader;
    restfulServer = Optional.empty();
  }

  @Override
  @Metadata(cacheMillis = 0)
  public CapabilityStatement getServerConformance(
      @Nullable final HttpServletRequest httpServletRequest,
      @Nullable final RequestDetails requestDetails) {
    log.info("Received request for server capabilities");

    final CapabilityStatement capabilityStatement = new CapabilityStatement();
    capabilityStatement.setUrl(getCapabilityUri());
    capabilityStatement.setVersion(configuration.getVersion());
    capabilityStatement.setTitle("Pathling FHIR API");
    capabilityStatement.setName("pathling-fhir-api");
    capabilityStatement.setStatus(PublicationStatus.ACTIVE);
    capabilityStatement.setExperimental(true);
    capabilityStatement.setPublisher("Australian e-Health Research Centre, CSIRO");
    capabilityStatement.setCopyright(
        "Dedicated to the public domain via CC0: https://creativecommons.org/publicdomain/zero/1.0/");
    capabilityStatement.setKind(CapabilityStatementKind.INSTANCE);

    final CapabilityStatementSoftwareComponent software = new CapabilityStatementSoftwareComponent(
        new StringType("Pathling"));
    software.setVersion(configuration.getVersion());
    capabilityStatement.setSoftware(software);

    final CapabilityStatementImplementationComponent implementation =
        new CapabilityStatementImplementationComponent(
            new StringType(configuration.getImplementationDescription()));
    final Optional<String> serverBase = getServerBase(Optional.ofNullable(httpServletRequest));
    serverBase.ifPresent(implementation::setUrl);
    capabilityStatement.setImplementation(implementation);

    capabilityStatement.setFhirVersion(FHIRVersion._4_0_1);
    capabilityStatement.setFormat(
        Arrays.asList(new CodeType("application/fhir+json"), new CodeType("application/fhir+xml")));
    capabilityStatement.setRest(buildRestComponent());

    return capabilityStatement;
  }

  @Nonnull
  private List<CapabilityStatementRestComponent> buildRestComponent() {
    final List<CapabilityStatementRestComponent> rest = new ArrayList<>();
    final CapabilityStatementRestComponent server = new CapabilityStatementRestComponent();
    server.setMode(RestfulCapabilityMode.SERVER);
    server.setSecurity(buildSecurity());
    server.setResource(buildResources());
    server.setOperation(buildOperations());
    rest.add(server);
    return rest;
  }

  @Nonnull
  private CapabilityStatementRestSecurityComponent buildSecurity() {
    final CapabilityStatementRestSecurityComponent security = new CapabilityStatementRestSecurityComponent();
    security.setCors(true);
    if (configuration.getAuth().isEnabled()) {
      final Authorisation authorisationConfig = configuration.getAuth();

      final CodeableConcept smart = new CodeableConcept(
          new Coding(RESTFUL_SECURITY_URI, RESTFUL_SECURITY_CODE, RESTFUL_SECURITY_CODE));
      smart.setText("OAuth2 using SMART-on-FHIR profile (see http://docs.smarthealthit.org)");
      security.getService().add(smart);

      if (authorisationConfig.getAuthorizeUrl().isPresent()
          || authorisationConfig.getTokenUrl().isPresent()
          || authorisationConfig.getRevokeUrl().isPresent()) {
        final Extension oauthUris = new Extension(SMART_OAUTH_URI);
        authorisationConfig.getAuthorizeUrl()
            .ifPresent(url -> oauthUris.addExtension("authorize", new UriType(url)));
        authorisationConfig.getTokenUrl()
            .ifPresent(url -> oauthUris.addExtension("token", new UriType(url)));
        authorisationConfig.getRevokeUrl()
            .ifPresent(url -> oauthUris.addExtension("revoke", new UriType(url)));
        security.addExtension(oauthUris);
      }
    }
    return security;
  }

  @Nonnull
  private List<CapabilityStatementRestResourceComponent> buildResources() {
    final List<CapabilityStatementRestResourceComponent> resources = new ArrayList<>();
    @Nullable CapabilityStatementRestResourceComponent opDefResource = null;
    final Set<Enumerations.ResourceType> availableToRead = resourceReader
        .getAvailableResourceTypes();
    final Set<Enumerations.ResourceType> availableResourceTypes =
        availableToRead.isEmpty()
        ? EnumSet.noneOf(Enumerations.ResourceType.class)
        : EnumSet.copyOf(availableToRead);
    availableResourceTypes.add(Enumerations.ResourceType.OPERATIONDEFINITION);

    for (final Enumerations.ResourceType resourceType : availableResourceTypes) {
      final CapabilityStatementRestResourceComponent resource =
          new CapabilityStatementRestResourceComponent(new CodeType(resourceType.toCode()));
      resource.setProfile(FHIR_RESOURCE_BASE + resourceType.toCode());
      final ResourceInteractionComponent interaction = new ResourceInteractionComponent();
      interaction.setCode(TypeRestfulInteraction.SEARCHTYPE);
      resource.getInteraction().add(interaction);

      // Add the `aggregate` operation to all resources.
      final CanonicalType aggregateOperationUri = new CanonicalType(getAggregateUri());
      final CapabilityStatementRestResourceOperationComponent aggregateOperation =
          new CapabilityStatementRestResourceOperationComponent(new StringType("aggregate"),
              aggregateOperationUri);
      resource.addOperation(aggregateOperation);

      // Add the `fhirPath` search parameter to all resources.
      final CapabilityStatementRestResourceOperationComponent searchOperation = new CapabilityStatementRestResourceOperationComponent();
      searchOperation.setName("fhirPath");
      searchOperation.setDefinition(getSearchUri());
      resource.addOperation(searchOperation);

      resources.add(resource);

      // Save away the OperationDefinition resource, so that we can later add the read operation to
      // it.
      if (resourceType.toCode().equals("OperationDefinition")) {
        opDefResource = resource;
      }
    }

    if (opDefResource != null) {
      // Add the read operation to the StructureDefinition and OperationDefinition resources.
      final ResourceInteractionComponent readInteraction = new ResourceInteractionComponent();
      readInteraction.setCode(TypeRestfulInteraction.READ);
      opDefResource.addInteraction(readInteraction);
    }

    return resources;
  }

  @Nonnull
  private List<CapabilityStatementRestResourceOperationComponent> buildOperations() {
    final List<CapabilityStatementRestResourceOperationComponent> operations = new ArrayList<>();

    // Add the `import` operation at the system level.
    final CanonicalType importOperationUri = new CanonicalType(getImportUri());
    final CapabilityStatementRestResourceOperationComponent importOperation =
        new CapabilityStatementRestResourceOperationComponent(new StringType("import"),
            importOperationUri);

    operations.add(importOperation);
    return operations;
  }

  @Nonnull
  private String getCapabilityUri() {
    return URI_BASE + "/CapabilityStatement/pathling-fhir-api-" + getMajorVersion(
        configuration.getVersion());
  }

  @Nonnull
  private String getSearchUri() {
    return URI_BASE + "/OperationDefinition/search-" + getMajorVersion(configuration.getVersion());
  }

  @Nonnull
  private String getAggregateUri() {
    return URI_BASE + "/OperationDefinition/aggregate-" + getMajorVersion(
        configuration.getVersion());
  }

  @Nonnull
  private String getImportUri() {
    return URI_BASE + "/OperationDefinition/import-" + getMajorVersion(configuration.getVersion());
  }

  @Override
  public void setRestfulServer(@Nullable final RestfulServer restfulServer) {
    this.restfulServer = Optional.ofNullable(restfulServer);
  }

  @Nonnull
  private Optional<String> getServerBase(
      @Nonnull final Optional<HttpServletRequest> httpServletRequest) {
    if (httpServletRequest.isEmpty() || restfulServer.isEmpty()) {
      log.warn("Attempted to get server base URL, HTTP servlet request or RestfulServer missing");
      return Optional.empty();
    } else {
      final ServletContext servletContext =
          (ServletContext) httpServletRequest.get()
              .getAttribute(RestfulServer.SERVLET_CONTEXT_ATTRIBUTE);
      return Optional.ofNullable(restfulServer.get().getServerAddressStrategy()
          .determineServerBase(servletContext, httpServletRequest.get()));
    }
  }

}
