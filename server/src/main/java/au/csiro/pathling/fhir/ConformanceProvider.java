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

import static au.csiro.pathling.security.OidcConfiguration.ConfigItem.AUTH_URL;
import static au.csiro.pathling.security.OidcConfiguration.ConfigItem.REVOKE_URL;
import static au.csiro.pathling.security.OidcConfiguration.ConfigItem.TOKEN_URL;
import static au.csiro.pathling.utilities.Preconditions.checkPresent;
import static au.csiro.pathling.utilities.Preconditions.checkUserInput;
import static java.util.Objects.requireNonNull;

import au.csiro.pathling.FhirServer;
import au.csiro.pathling.PathlingServerVersion;
import au.csiro.pathling.cache.Cacheable;
import au.csiro.pathling.config.ServerConfiguration;
import au.csiro.pathling.errors.ResourceNotFoundError;
import au.csiro.pathling.security.OidcConfiguration;
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
import org.hl7.fhir.r4.model.CapabilityStatement.CapabilityStatementRestResourceSearchParamComponent;
import org.hl7.fhir.r4.model.CapabilityStatement.CapabilityStatementRestSecurityComponent;
import org.hl7.fhir.r4.model.CapabilityStatement.CapabilityStatementSoftwareComponent;
import org.hl7.fhir.r4.model.CapabilityStatement.ResourceInteractionComponent;
import org.hl7.fhir.r4.model.CapabilityStatement.RestfulCapabilityMode;
import org.hl7.fhir.r4.model.CapabilityStatement.SystemInteractionComponent;
import org.hl7.fhir.r4.model.CapabilityStatement.SystemRestfulInteraction;
import org.hl7.fhir.r4.model.CapabilityStatement.TypeRestfulInteraction;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.FHIRVersion;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Enumerations.SearchParamType;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.OperationDefinition;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.UriType;
import org.springframework.stereotype.Component;

/**
 * This class provides a customised CapabilityStatement describing the functionality of the
 * analytics server.
 *
 * @author John Grimes
 * @see <a href="https://hl7.org/fhir/R4/capabilitystatement.html">CapabilityStatement</a>
 */
@Component
@Slf4j
public class ConformanceProvider implements IServerConformanceProvider<CapabilityStatement>,
    Cacheable {

  /**
   * The base URI for canonical URIs.
   */
  public static final String URI_BASE = "https://pathling.csiro.au/fhir";

  private static final String EXPORT_OPERATION = "export";
  private static final String RUN_OPERATION = "run";

  /**
   * Base system-level operations available within Pathling.
   */
  private static final List<String> BASE_SYSTEM_OPERATIONS = Arrays.asList("job", "result",
      EXPORT_OPERATION, "import", "import-pnp", "viewdefinition-run");

  /**
   * Bulk submit operations, added when bulk submit is configured.
   */
  private static final List<String> BULK_SUBMIT_OPERATIONS = Arrays.asList("bulk-submit",
      "bulk-submit-status");

  private static final String RESTFUL_SECURITY_URI = "http://terminology.hl7.org/CodeSystem/restful-security-service";
  private static final String RESTFUL_SECURITY_CODE = "SMART-on-FHIR";
  private static final String SMART_OAUTH_URI = "http://fhir-registry.smarthealthit.org/StructureDefinition/oauth-uris";

  private static final String FHIR_RESOURCE_BASE = "http://hl7.org/fhir/StructureDefinition/";
  private static final String UNKNOWN_VERSION = "UNKNOWN";

  /**
   * All resource-level operations available within Pathling.
   */
  private static final List<String> RESOURCE_LEVEL_OPERATIONS = List.of(EXPORT_OPERATION,
      RUN_OPERATION);

  /**
   * Resource types that have the export operation available.
   */
  private static final Set<ResourceType> EXPORT_RESOURCE_TYPES = Set.of(
      ResourceType.PATIENT, ResourceType.GROUP);

  @Nonnull
  private final ServerConfiguration configuration;

  @Nonnull
  private final Optional<OidcConfiguration> oidcConfiguration;

  @Nonnull
  private Optional<RestfulServer> restfulServer;

  @Nonnull
  private final PathlingServerVersion version;

  @Nonnull
  private final FhirContext fhirContext;

  @Nonnull
  private final IParser jsonParser;

  @Nonnull
  private final Map<String, OperationDefinition> resources;

  @Nonnull
  private final List<String> systemLevelOperations;

  /**
   * @param configuration a {@link ServerConfiguration} object controlling the behaviour of the
   * capability statement
   * @param oidcConfiguration the OIDC configuration for security
   * @param version a {@link PathlingServerVersion} object containing version information for the
   * server
   * @param fhirContext a {@link FhirContext} for determining the supported FHIR version
   * @param jsonParser a {@link IParser} for parsing JSON OperationDefinitions
   */
  public ConformanceProvider(@Nonnull final ServerConfiguration configuration,
      @Nonnull final Optional<OidcConfiguration> oidcConfiguration,
      @Nonnull final PathlingServerVersion version, @Nonnull final FhirContext fhirContext,
      @Nonnull final IParser jsonParser) {
    this.configuration = configuration;
    this.oidcConfiguration = oidcConfiguration;
    this.version = version;
    this.fhirContext = fhirContext;
    this.jsonParser = jsonParser;

    // Compute system-level operations based on configuration.
    final List<String> systemOps = new ArrayList<>(BASE_SYSTEM_OPERATIONS);
    if (configuration.getBulkSubmit() != null) {
      systemOps.addAll(BULK_SUBMIT_OPERATIONS);
    }
    this.systemLevelOperations = List.copyOf(systemOps);

    // Compute all operations (deduplicated).
    final Set<String> operationSet = new java.util.LinkedHashSet<>();
    operationSet.addAll(this.systemLevelOperations);
    operationSet.addAll(RESOURCE_LEVEL_OPERATIONS);
    final List<String> allOperations = new ArrayList<>(operationSet);

    final ImmutableMap.Builder<String, OperationDefinition> mapBuilder = new ImmutableMap.Builder<>();
    for (final String operation : allOperations) {
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
        new StringType("Pathling Server"));
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
    server.setSecurity(buildSecurity());
    server.setResource(buildResources());
    server.setOperation(buildOperations());
    server.setInteraction(buildSystemInteractions());
    rest.add(server);
    return rest;
  }

  @Nonnull
  private List<SystemInteractionComponent> buildSystemInteractions() {
    final List<SystemInteractionComponent> interactions = new ArrayList<>();

    // Add batch interaction at system level.
    final SystemInteractionComponent batchInteraction = new SystemInteractionComponent();
    batchInteraction.setCode(SystemRestfulInteraction.BATCH);
    interactions.add(batchInteraction);

    return interactions;
  }

  @Nonnull
  private CapabilityStatementRestSecurityComponent buildSecurity() {
    final CapabilityStatementRestSecurityComponent security = new CapabilityStatementRestSecurityComponent();
    security.setCors(true);
    if (configuration.getAuth().isEnabled()) {
      final OidcConfiguration checkedConfig = checkPresent(oidcConfiguration);

      final CodeableConcept smart = new CodeableConcept(
          new Coding(RESTFUL_SECURITY_URI, RESTFUL_SECURITY_CODE, RESTFUL_SECURITY_CODE));
      smart.setText("OAuth2 using SMART-on-FHIR profile (see http://docs.smarthealthit.org)");
      security.getService().add(smart);

      final Optional<String> authUrl = checkedConfig.get(AUTH_URL);
      final Optional<String> tokenUrl = checkedConfig.get(TOKEN_URL);
      final Optional<String> revokeUrl = checkedConfig.get(REVOKE_URL);
      if (authUrl.isPresent() || tokenUrl.isPresent() || revokeUrl.isPresent()) {
        final Extension oauthUris = new Extension(SMART_OAUTH_URI);
        authUrl.ifPresent(url -> oauthUris.addExtension("authorize", new UriType(url)));
        tokenUrl.ifPresent(url -> oauthUris.addExtension("token", new UriType(url)));
        revokeUrl.ifPresent(url -> oauthUris.addExtension("revoke", new UriType(url)));
        security.addExtension(oauthUris);
      }
    }
    return security;
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

      // Add search-type interaction to all resource types.
      final ResourceInteractionComponent searchInteraction = new ResourceInteractionComponent();
      searchInteraction.setCode(TypeRestfulInteraction.SEARCHTYPE);
      resource.addInteraction(searchInteraction);

      // Add update interaction to all resource types.
      final ResourceInteractionComponent updateInteraction = new ResourceInteractionComponent();
      updateInteraction.setCode(TypeRestfulInteraction.UPDATE);
      resource.addInteraction(updateInteraction);

      // Add create interaction to all resource types.
      final ResourceInteractionComponent createInteraction = new ResourceInteractionComponent();
      createInteraction.setCode(TypeRestfulInteraction.CREATE);
      resource.addInteraction(createInteraction);

      // Add read interaction to all resource types.
      final ResourceInteractionComponent readInteraction = new ResourceInteractionComponent();
      readInteraction.setCode(TypeRestfulInteraction.READ);
      resource.addInteraction(readInteraction);

      // Add the fhirPath named query with filter parameter for FHIRPath-based search.
      final CapabilityStatementRestResourceSearchParamComponent filterParam =
          new CapabilityStatementRestResourceSearchParamComponent();
      filterParam.setName("filter");
      filterParam.setType(SearchParamType.STRING);
      filterParam.setDocumentation(
          "FHIRPath expression to filter resources (use with _query=fhirPath)");
      resource.addSearchParam(filterParam);

      // Add export operation to Patient and Group resources.
      if (EXPORT_RESOURCE_TYPES.contains(resourceType)) {
        final CanonicalType exportUri = new CanonicalType(getOperationUri(EXPORT_OPERATION));
        final CapabilityStatementRestResourceOperationComponent exportOp =
            new CapabilityStatementRestResourceOperationComponent(new StringType(EXPORT_OPERATION),
                exportUri);
        resource.addOperation(exportOp);
      }

      resources2.add(resource);
    }

    // Add the read operation to the OperationDefinition resource.
    final String opDefCode = ResourceType.OPERATIONDEFINITION.toCode();
    final CapabilityStatementRestResourceComponent opDefResource =
        new CapabilityStatementRestResourceComponent(new CodeType(opDefCode));
    opDefResource.setProfile(FHIR_RESOURCE_BASE + opDefCode);
    final ResourceInteractionComponent opDefReadInteraction = new ResourceInteractionComponent();
    opDefReadInteraction.setCode(TypeRestfulInteraction.READ);
    opDefResource.addInteraction(opDefReadInteraction);
    resources2.add(opDefResource);

    // Add ViewDefinition as a custom resource type with read, search, and update interactions.
    final String viewDefCode = "ViewDefinition";
    final CapabilityStatementRestResourceComponent viewDefResource =
        new CapabilityStatementRestResourceComponent(new CodeType(viewDefCode));
    viewDefResource.setProfile("http://hl7.org/fhir/uv/sql-on-fhir/StructureDefinition/ViewDefinition");

    final ResourceInteractionComponent viewDefSearchInteraction = new ResourceInteractionComponent();
    viewDefSearchInteraction.setCode(TypeRestfulInteraction.SEARCHTYPE);
    viewDefResource.addInteraction(viewDefSearchInteraction);

    final ResourceInteractionComponent viewDefUpdateInteraction = new ResourceInteractionComponent();
    viewDefUpdateInteraction.setCode(TypeRestfulInteraction.UPDATE);
    viewDefResource.addInteraction(viewDefUpdateInteraction);

    final ResourceInteractionComponent viewDefReadInteraction = new ResourceInteractionComponent();
    viewDefReadInteraction.setCode(TypeRestfulInteraction.READ);
    viewDefResource.addInteraction(viewDefReadInteraction);

    final ResourceInteractionComponent viewDefCreateInteraction = new ResourceInteractionComponent();
    viewDefCreateInteraction.setCode(TypeRestfulInteraction.CREATE);
    viewDefResource.addInteraction(viewDefCreateInteraction);

    // Add the fhirPath named query with filter parameter for ViewDefinition.
    final CapabilityStatementRestResourceSearchParamComponent viewDefFilterParam =
        new CapabilityStatementRestResourceSearchParamComponent();
    viewDefFilterParam.setName("filter");
    viewDefFilterParam.setType(SearchParamType.STRING);
    viewDefFilterParam.setDocumentation(
        "FHIRPath expression to filter resources (use with _query=fhirPath)");
    viewDefResource.addSearchParam(viewDefFilterParam);

    // Add $run operation to ViewDefinition resource.
    final CanonicalType runUri = new CanonicalType(getOperationUri(RUN_OPERATION));
    final CapabilityStatementRestResourceOperationComponent runOp =
        new CapabilityStatementRestResourceOperationComponent(new StringType(RUN_OPERATION),
            runUri);
    viewDefResource.addOperation(runOp);

    resources2.add(viewDefResource);

    return resources2;
  }

  @Nonnull
  private List<CapabilityStatementRestResourceOperationComponent> buildOperations() {
    final List<CapabilityStatementRestResourceOperationComponent> operations = new ArrayList<>();

    for (final String name : systemLevelOperations) {
      final CanonicalType operationUri = new CanonicalType(getOperationUri(name));
      final CapabilityStatementRestResourceOperationComponent operation =
          new CapabilityStatementRestResourceOperationComponent(new StringType(name),
              operationUri);
      operations.add(operation);
    }

    return operations;
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
