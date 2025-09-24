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

package au.csiro.pathling.fhir;

import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static java.util.Objects.requireNonNull;

import au.csiro.pathling.FhirServer;
import au.csiro.pathling.PathlingVersion;
import au.csiro.pathling.cache.Cacheable;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.errors.ResourceNotFoundError;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.annotation.IdParam;
import ca.uhn.fhir.rest.annotation.Metadata;
import ca.uhn.fhir.rest.annotation.Read;
import ca.uhn.fhir.rest.api.server.RequestDetails;
import ca.uhn.fhir.rest.server.IServerConformanceProvider;
import ca.uhn.fhir.rest.server.RestfulServer;
import com.google.common.collect.ImmutableMap;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.servlet.ServletContext;
import jakarta.servlet.http.HttpServletRequest;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.instance.model.api.IIdType;
import org.hl7.fhir.r4.model.CanonicalType;
import org.hl7.fhir.r4.model.CapabilityStatement;
import org.hl7.fhir.r4.model.CapabilityStatement.CapabilityStatementImplementationComponent;
import org.hl7.fhir.r4.model.CapabilityStatement.CapabilityStatementKind;
import org.hl7.fhir.r4.model.CapabilityStatement.CapabilityStatementRestComponent;
import org.hl7.fhir.r4.model.CapabilityStatement.CapabilityStatementRestResourceComponent;
import org.hl7.fhir.r4.model.CapabilityStatement.CapabilityStatementRestResourceOperationComponent;
import org.hl7.fhir.r4.model.CapabilityStatement.CapabilityStatementSoftwareComponent;
import org.hl7.fhir.r4.model.CapabilityStatement.ResourceInteractionComponent;
import org.hl7.fhir.r4.model.CapabilityStatement.RestfulCapabilityMode;
import org.hl7.fhir.r4.model.CapabilityStatement.SystemInteractionComponent;
import org.hl7.fhir.r4.model.CapabilityStatement.SystemRestfulInteraction;
import org.hl7.fhir.r4.model.CapabilityStatement.TypeRestfulInteraction;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Enumerations.FHIRVersion;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.OperationDefinition;
import org.hl7.fhir.r4.model.StringType;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * This class provides a customised CapabilityStatement describing the functionality of the
 * analytics server.
 *
 * @author John Grimes
 * @see <a href="https://hl7.org/fhir/R4/capabilitystatement.html">CapabilityStatement</a>
 */
@Component
@Profile("server")
@Slf4j
public class ConformanceProvider implements IServerConformanceProvider<CapabilityStatement>,
        Cacheable {

  /**
   * The base URI for canonical URIs.
   */
  public static final String URI_BASE = "https://pathling.csiro.au/fhir";
  /**
   * All system-level operations available within Pathling.
   */
  protected static final List<String> SYSTEM_LEVEL_OPERATIONS = Arrays.asList("job", "export");

  private static final String FHIR_RESOURCE_BASE = "http://hl7.org/fhir/StructureDefinition/";
  private static final String UNKNOWN_VERSION = "UNKNOWN";

  /**
   * All resource-level operations available within Pathling.
   */
  private static final List<String> RESOURCE_LEVEL_OPERATIONS = List.of();

  /**
   * All operations available within Pathling.
   */
  private static final List<String> OPERATIONS;

  static {
    OPERATIONS = new ArrayList<>();
    OPERATIONS.addAll(ConformanceProvider.SYSTEM_LEVEL_OPERATIONS);
    OPERATIONS.addAll(ConformanceProvider.RESOURCE_LEVEL_OPERATIONS);
  }

  @Nonnull
  private final ServerConfiguration configuration;

  @Nonnull
  private Optional<RestfulServer> restfulServer;

  @Nonnull
  private final PathlingVersion version;

  @Nonnull
  private final FhirContext fhirContext;

  @Nonnull
  private final IParser jsonParser;

  @Nonnull
  private final Map<String, OperationDefinition> resources;

  /**
   * @param configuration a {@link ServerConfiguration} object controlling the behaviour of the
   * capability statement
   * @param version a {@link PathlingVersion} object containing version information for the server
   * @param fhirContext a {@link FhirContext} for determining the supported FHIR version
   * @param jsonParser a {@link IParser} for parsing JSON OperationDefinitions
   */
  public ConformanceProvider(@Nonnull final ServerConfiguration configuration,
                             @Nonnull final PathlingVersion version, @Nonnull final FhirContext fhirContext,
                             @Nonnull final IParser jsonParser) {
    this.configuration = configuration;
    this.version = version;
    this.fhirContext = fhirContext;
    this.jsonParser = jsonParser;

    final ImmutableMap.Builder<String, OperationDefinition> mapBuilder = new ImmutableMap.Builder<>();
    for (final String operation : ConformanceProvider.OPERATIONS) {
      final String id =
          "OperationDefinition/" + operation + "-" +
              version.getMajorVersion().orElse(UNKNOWN_VERSION);
      final String path = "fhir/" + operation + ".OperationDefinition.json";
      mapBuilder.put(id, load(path));
    }
    resources = mapBuilder.build();

    restfulServer = Optional.empty();
  }

  @Override
  @Metadata(cacheMillis = 0)
  public CapabilityStatement getServerConformance(
      @Nullable final HttpServletRequest httpServletRequest,
      @Nullable final RequestDetails requestDetails) {
    log.debug("Received request for server capabilities");

    final CapabilityStatement capabilityStatement = new CapabilityStatement();
    capabilityStatement.setUrl(getCapabilityUri());
    capabilityStatement.setVersion(version.getBuildVersion().orElse(UNKNOWN_VERSION));
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
    software.setVersion(version.getDescriptiveVersion().orElse(UNKNOWN_VERSION));
    capabilityStatement.setSoftware(software);

    final CapabilityStatementImplementationComponent implementation =
        new CapabilityStatementImplementationComponent(
            new StringType(configuration.getImplementationDescription()));
    final Optional<String> serverBase = getServerBase(Optional.ofNullable(httpServletRequest));
    serverBase.ifPresent(implementation::setUrl);
    capabilityStatement.setImplementation(implementation);

    final FHIRVersion fhirVersion = FHIRVersion.fromCode(
        fhirContext.getVersion().getVersion().getFhirVersionString());
    capabilityStatement.setFhirVersion(fhirVersion);
    capabilityStatement.setFormat(Arrays.asList(new CodeType("json"), new CodeType("xml")));
    capabilityStatement.setRest(buildRestComponent());

    return capabilityStatement;
  }

  @Nonnull
  private List<CapabilityStatementRestComponent> buildRestComponent() {
    final List<CapabilityStatementRestComponent> rest = new ArrayList<>();
    final CapabilityStatementRestComponent server = new CapabilityStatementRestComponent();
    server.setMode(RestfulCapabilityMode.SERVER);
    server.setResource(buildResources());
    server.setOperation(buildOperations());
    server.setInteraction(buildSystemLevelInteractions());
    rest.add(server);
    return rest;
  }

  @Nonnull
  private List<CapabilityStatementRestResourceComponent> buildResources() {
    final List<CapabilityStatementRestResourceComponent> resources2 = new ArrayList<>();
    final Set<ResourceType> supported = FhirServer.supportedResourceTypes();
    final Set<ResourceType> supportedResourceTypes = supported.isEmpty()
                                                     ? EnumSet.noneOf(ResourceType.class)
                                                     : EnumSet.copyOf(supported);

    for (final ResourceType resourceType : supportedResourceTypes) {
      final CapabilityStatementRestResourceComponent resource =
          new CapabilityStatementRestResourceComponent(new CodeType(resourceType.toCode()));
      resource.setProfile(FHIR_RESOURCE_BASE + resourceType.toCode());

      // Add the search operation to all resources.
      final ResourceInteractionComponent search = new ResourceInteractionComponent();
      search.setCode(TypeRestfulInteraction.SEARCHTYPE);
      resource.getInteraction().add(search);

      // Add the create and update operations to all resources.
      final ResourceInteractionComponent create = new ResourceInteractionComponent();
      final ResourceInteractionComponent update = new ResourceInteractionComponent();
      create.setCode(TypeRestfulInteraction.CREATE);
      update.setCode(TypeRestfulInteraction.UPDATE);
      resource.getInteraction().add(create);
      resource.getInteraction().add(update);
      
      resources2.add(resource);
    }

    // Add the read operation to the OperationDefinition resource.
    final String opDefCode = ResourceType.OPERATIONDEFINITION.toCode();
    final CapabilityStatementRestResourceComponent opDefResource =
        new CapabilityStatementRestResourceComponent(new CodeType(opDefCode));
    opDefResource.setProfile(FHIR_RESOURCE_BASE + opDefCode);
    final ResourceInteractionComponent readInteraction = new ResourceInteractionComponent();
    readInteraction.setCode(TypeRestfulInteraction.READ);
    opDefResource.addInteraction(readInteraction);
    resources2.add(opDefResource);

    return resources2;
  }

  @Nonnull
  private List<CapabilityStatementRestResourceOperationComponent> buildOperations() {
    final List<CapabilityStatementRestResourceOperationComponent> operations = new ArrayList<>();

    for (final String name : SYSTEM_LEVEL_OPERATIONS) {
      final CanonicalType operationUri = new CanonicalType(getOperationUri(name));
      final CapabilityStatementRestResourceOperationComponent operation =
          new CapabilityStatementRestResourceOperationComponent(new StringType(name),
              operationUri);
      operations.add(operation);
    }

    return operations;
  }

  @Nonnull
  private List<SystemInteractionComponent> buildSystemLevelInteractions() {
    final List<SystemInteractionComponent> interactions = new ArrayList<>();
    final SystemInteractionComponent interaction = new SystemInteractionComponent();
    interaction.setCode(SystemRestfulInteraction.BATCH);
    interactions.add(interaction);
    return interactions;
  }

  @Nonnull
  private String getCapabilityUri() {
    return URI_BASE + "/CapabilityStatement/pathling-fhir-api-" + version.getMajorVersion()
        .orElse(UNKNOWN_VERSION);
  }

  @Nonnull
  private String getOperationUri(final String name) {
    return URI_BASE + "/OperationDefinition/" + name + "-" + version.getMajorVersion()
        .orElse(UNKNOWN_VERSION);
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

  @Override
  public Optional<String> getCacheKey() {
    return version.getDescriptiveVersion();
  }

  @Override
  public boolean cacheKeyMatches(final String otherKey) {
    return version.getDescriptiveVersion().map(key -> key.equals(otherKey)).orElse(false);
  }

  /**
   * Handles all read requests to the OperationDefinition resource.
   *
   * @param id the ID of the desired OperationDefinition
   * @return an {@link OperationDefinition} resource
   */
  @Read(typeName = "OperationDefinition")
  @SuppressWarnings("unused")
  public IBaseResource getOperationDefinitionById(@Nullable @IdParam final IIdType id) {
    checkUserInput(id != null, "Missing ID parameter");
    log.info("Reading OperationDefinition with ID {}", id.getValue());

    final String idString = id.getValue();
    final OperationDefinition resource = resources.get(idString);
    if (resource == null) {
      throw new ResourceNotFoundError("OperationDefinition not found: " + idString);
    }
    return resource;
  }

  @Nonnull
  private OperationDefinition load(@Nonnull final String resourcePath) {
    @Nullable final InputStream resourceStream = Thread.currentThread().getContextClassLoader()
        .getResourceAsStream(resourcePath);
    requireNonNull(resourceStream);

    final OperationDefinition operationDefinition = (OperationDefinition) jsonParser
        .parseResource(resourceStream);
    final String id = String
        .format("%1$s%2$s", operationDefinition.getName(),
            version.getMajorVersion().map(v -> String.format("-%1$s", v)).orElse(""));
    operationDefinition.setId(id);
    final String url = String
        .format("%1$s/OperationDefinition/%2$s", ConformanceProvider.URI_BASE, id);
    operationDefinition.setUrl(url);
    operationDefinition.setVersion(version.getBuildVersion().orElse(
        ConformanceProvider.UNKNOWN_VERSION));
    return operationDefinition;
  }

}
