/*
 * Copyright 2025 Commonwealth Scientific and Industrial Research
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

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.FhirServer;
import au.csiro.pathling.PathlingServerVersion;
import au.csiro.pathling.config.AuthorizationConfiguration;
import au.csiro.pathling.config.OperationConfiguration;
import au.csiro.pathling.config.ServerConfiguration;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.hl7.fhir.r4.model.CapabilityStatement;
import org.hl7.fhir.r4.model.CapabilityStatement.CapabilityStatementRestResourceComponent;
import org.hl7.fhir.r4.model.CapabilityStatement.CapabilityStatementRestResourceOperationComponent;
import org.hl7.fhir.r4.model.CapabilityStatement.ResourceInteractionComponent;
import org.hl7.fhir.r4.model.CapabilityStatement.TypeRestfulInteraction;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Tests for {@link ConformanceProvider}.
 *
 * @author John Grimes
 */
class ConformanceProviderTest {

  private ConformanceProvider conformanceProvider;

  @BeforeEach
  void setUp() {
    final ServerConfiguration configuration = Mockito.mock(ServerConfiguration.class);
    final AuthorizationConfiguration authConfiguration =
        Mockito.mock(AuthorizationConfiguration.class);
    final OperationConfiguration opsConfiguration = new OperationConfiguration();
    Mockito.when(configuration.getAuth()).thenReturn(authConfiguration);
    Mockito.when(authConfiguration.isEnabled()).thenReturn(false);
    Mockito.when(configuration.getImplementationDescription()).thenReturn("Test Implementation");
    Mockito.when(configuration.getOperations()).thenReturn(opsConfiguration);

    final PathlingServerVersion version = Mockito.mock(PathlingServerVersion.class);
    Mockito.when(version.getMajorVersion()).thenReturn(Optional.of("1"));
    Mockito.when(version.getBuildVersion()).thenReturn(Optional.of("1.0.0"));
    Mockito.when(version.getDescriptiveVersion()).thenReturn(Optional.of("1.0.0"));

    final FhirContext fhirContext = FhirContext.forR4();
    final IParser jsonParser = fhirContext.newJsonParser();

    conformanceProvider =
        new ConformanceProvider(configuration, Optional.empty(), version, fhirContext, jsonParser);
  }

  @Test
  void capabilityStatementIncludesCreateInteractionForAllResourceTypes() {
    // When: Getting the capability statement.
    final CapabilityStatement capabilityStatement =
        conformanceProvider.getServerConformance(null, null);

    // Then: All supported resource types (except read-only ones) should have CREATE interaction.
    final Set<ResourceType> supportedResourceTypes = FhirServer.supportedResourceTypes();
    final List<CapabilityStatementRestResourceComponent> resources =
        capabilityStatement.getRest().get(0).getResource();

    for (final ResourceType resourceType : supportedResourceTypes) {
      // OperationDefinition is intentionally read-only.
      if (resourceType == ResourceType.OPERATIONDEFINITION) {
        continue;
      }
      final Optional<CapabilityStatementRestResourceComponent> resourceComponent =
          resources.stream().filter(r -> r.getType().equals(resourceType.toCode())).findFirst();

      assertThat(resourceComponent).isPresent();

      final Set<TypeRestfulInteraction> interactions =
          resourceComponent.get().getInteraction().stream()
              .map(ResourceInteractionComponent::getCode)
              .collect(Collectors.toSet());

      assertThat(interactions)
          .as("Resource type " + resourceType.toCode() + " should have CREATE interaction")
          .contains(TypeRestfulInteraction.CREATE);
    }
  }

  @Test
  void capabilityStatementIncludesCreateInteractionForViewDefinition() {
    // When: Getting the capability statement.
    final CapabilityStatement capabilityStatement =
        conformanceProvider.getServerConformance(null, null);

    // Then: ViewDefinition should have CREATE interaction.
    final List<CapabilityStatementRestResourceComponent> resources =
        capabilityStatement.getRest().get(0).getResource();

    final Optional<CapabilityStatementRestResourceComponent> viewDefResource =
        resources.stream().filter(r -> r.getType().equals("ViewDefinition")).findFirst();

    assertThat(viewDefResource).isPresent();

    final Set<TypeRestfulInteraction> interactions =
        viewDefResource.get().getInteraction().stream()
            .map(ResourceInteractionComponent::getCode)
            .collect(Collectors.toSet());

    assertThat(interactions).contains(TypeRestfulInteraction.CREATE);
  }

  @Test
  void capabilityStatementIncludesAllCrudInteractionsForResourceTypes() {
    // When: Getting the capability statement.
    final CapabilityStatement capabilityStatement =
        conformanceProvider.getServerConformance(null, null);

    // Then: All supported resource types (except read-only ones) should have CRUD interactions.
    final Set<ResourceType> supportedResourceTypes = FhirServer.supportedResourceTypes();
    final List<CapabilityStatementRestResourceComponent> resources =
        capabilityStatement.getRest().get(0).getResource();

    for (final ResourceType resourceType : supportedResourceTypes) {
      // OperationDefinition is intentionally read-only.
      if (resourceType == ResourceType.OPERATIONDEFINITION) {
        continue;
      }
      final Optional<CapabilityStatementRestResourceComponent> resourceComponent =
          resources.stream().filter(r -> r.getType().equals(resourceType.toCode())).findFirst();

      assertThat(resourceComponent).isPresent();

      final Set<TypeRestfulInteraction> interactions =
          resourceComponent.get().getInteraction().stream()
              .map(ResourceInteractionComponent::getCode)
              .collect(Collectors.toSet());

      assertThat(interactions)
          .as("Resource type " + resourceType.toCode() + " should have all CRUD interactions")
          .contains(
              TypeRestfulInteraction.READ,
              TypeRestfulInteraction.SEARCHTYPE,
              TypeRestfulInteraction.UPDATE,
              TypeRestfulInteraction.CREATE,
              TypeRestfulInteraction.DELETE);
    }
  }

  @Test
  void capabilityStatementIncludesDeleteInteractionForAllResourceTypes() {
    // When: Getting the capability statement.
    final CapabilityStatement capabilityStatement =
        conformanceProvider.getServerConformance(null, null);

    // Then: All supported resource types (except read-only ones) should have DELETE interaction.
    final Set<ResourceType> supportedResourceTypes = FhirServer.supportedResourceTypes();
    final List<CapabilityStatementRestResourceComponent> resources =
        capabilityStatement.getRest().get(0).getResource();

    for (final ResourceType resourceType : supportedResourceTypes) {
      // OperationDefinition is intentionally read-only.
      if (resourceType == ResourceType.OPERATIONDEFINITION) {
        continue;
      }
      final Optional<CapabilityStatementRestResourceComponent> resourceComponent =
          resources.stream().filter(r -> r.getType().equals(resourceType.toCode())).findFirst();

      assertThat(resourceComponent).isPresent();

      final Set<TypeRestfulInteraction> interactions =
          resourceComponent.get().getInteraction().stream()
              .map(ResourceInteractionComponent::getCode)
              .collect(Collectors.toSet());

      assertThat(interactions)
          .as("Resource type " + resourceType.toCode() + " should have DELETE interaction")
          .contains(TypeRestfulInteraction.DELETE);
    }
  }

  @Test
  void capabilityStatementIncludesDeleteInteractionForViewDefinition() {
    // When: Getting the capability statement.
    final CapabilityStatement capabilityStatement =
        conformanceProvider.getServerConformance(null, null);

    // Then: ViewDefinition should have DELETE interaction.
    final List<CapabilityStatementRestResourceComponent> resources =
        capabilityStatement.getRest().get(0).getResource();

    final Optional<CapabilityStatementRestResourceComponent> viewDefResource =
        resources.stream().filter(r -> r.getType().equals("ViewDefinition")).findFirst();

    assertThat(viewDefResource).isPresent();

    final Set<TypeRestfulInteraction> interactions =
        viewDefResource.get().getInteraction().stream()
            .map(ResourceInteractionComponent::getCode)
            .collect(Collectors.toSet());

    assertThat(interactions).contains(TypeRestfulInteraction.DELETE);
  }

  @Test
  void capabilityStatementIncludesViewDefinitionExportOperation() {
    // When: Getting the capability statement.
    final CapabilityStatement capabilityStatement =
        conformanceProvider.getServerConformance(null, null);

    // Then: The system-level operations should include viewdefinition-export.
    final List<CapabilityStatementRestResourceOperationComponent> operations =
        capabilityStatement.getRest().get(0).getOperation();

    final Set<String> operationNames =
        operations.stream()
            .map(CapabilityStatementRestResourceOperationComponent::getName)
            .collect(Collectors.toSet());

    assertThat(operationNames)
        .as("System-level operations should include viewdefinition-export")
        .contains("viewdefinition-export");
  }

  @Test
  void capabilityStatementHasNoDuplicateResourceTypes() {
    // When: Getting the capability statement.
    final CapabilityStatement capabilityStatement =
        conformanceProvider.getServerConformance(null, null);

    // Then: There should be no duplicate resource types in the capability statement.
    final List<CapabilityStatementRestResourceComponent> resources =
        capabilityStatement.getRest().get(0).getResource();

    final List<String> resourceTypes =
        resources.stream().map(CapabilityStatementRestResourceComponent::getType).toList();

    final Set<String> uniqueResourceTypes = Set.copyOf(resourceTypes);

    assertThat(resourceTypes)
        .as("CapabilityStatement should not contain duplicate resource types")
        .hasSameSizeAs(uniqueResourceTypes);
  }

  @Test
  void capabilityStatementExcludesCreateWhenDisabled() {
    // Given: A configuration with create disabled.
    final ConformanceProvider provider =
        createProviderWithDisabledOperations(
            ops -> {
              ops.setCreateEnabled(false);
            });

    // When: Getting the capability statement.
    final CapabilityStatement capabilityStatement = provider.getServerConformance(null, null);

    // Then: No resource should have CREATE interaction.
    final List<CapabilityStatementRestResourceComponent> resources =
        capabilityStatement.getRest().get(0).getResource();

    for (final CapabilityStatementRestResourceComponent resource : resources) {
      // Skip OperationDefinition which is read-only.
      if (resource.getType().equals("OperationDefinition")) {
        continue;
      }
      final Set<TypeRestfulInteraction> interactions =
          resource.getInteraction().stream()
              .map(ResourceInteractionComponent::getCode)
              .collect(Collectors.toSet());

      assertThat(interactions)
          .as("Resource type " + resource.getType() + " should not have CREATE interaction")
          .doesNotContain(TypeRestfulInteraction.CREATE);
    }
  }

  @Test
  void capabilityStatementExcludesReadWhenDisabled() {
    // Given: A configuration with read disabled.
    final ConformanceProvider provider =
        createProviderWithDisabledOperations(
            ops -> {
              ops.setReadEnabled(false);
            });

    // When: Getting the capability statement.
    final CapabilityStatement capabilityStatement = provider.getServerConformance(null, null);

    // Then: No resource (except OperationDefinition) should have READ interaction.
    final List<CapabilityStatementRestResourceComponent> resources =
        capabilityStatement.getRest().get(0).getResource();

    for (final CapabilityStatementRestResourceComponent resource : resources) {
      // Skip OperationDefinition which always has READ.
      if (resource.getType().equals("OperationDefinition")) {
        continue;
      }
      final Set<TypeRestfulInteraction> interactions =
          resource.getInteraction().stream()
              .map(ResourceInteractionComponent::getCode)
              .collect(Collectors.toSet());

      assertThat(interactions)
          .as("Resource type " + resource.getType() + " should not have READ interaction")
          .doesNotContain(TypeRestfulInteraction.READ);
    }
  }

  @Test
  void capabilityStatementExcludesSearchWhenDisabled() {
    // Given: A configuration with search disabled.
    final ConformanceProvider provider =
        createProviderWithDisabledOperations(
            ops -> {
              ops.setSearchEnabled(false);
            });

    // When: Getting the capability statement.
    final CapabilityStatement capabilityStatement = provider.getServerConformance(null, null);

    // Then: No resource should have SEARCHTYPE interaction.
    final List<CapabilityStatementRestResourceComponent> resources =
        capabilityStatement.getRest().get(0).getResource();

    for (final CapabilityStatementRestResourceComponent resource : resources) {
      final Set<TypeRestfulInteraction> interactions =
          resource.getInteraction().stream()
              .map(ResourceInteractionComponent::getCode)
              .collect(Collectors.toSet());

      assertThat(interactions)
          .as("Resource type " + resource.getType() + " should not have SEARCHTYPE interaction")
          .doesNotContain(TypeRestfulInteraction.SEARCHTYPE);
    }
  }

  @Test
  void capabilityStatementExcludesDeleteWhenDisabled() {
    // Given: A configuration with delete disabled.
    final ConformanceProvider provider =
        createProviderWithDisabledOperations(
            ops -> {
              ops.setDeleteEnabled(false);
            });

    // When: Getting the capability statement.
    final CapabilityStatement capabilityStatement = provider.getServerConformance(null, null);

    // Then: No resource should have DELETE interaction.
    final List<CapabilityStatementRestResourceComponent> resources =
        capabilityStatement.getRest().get(0).getResource();

    for (final CapabilityStatementRestResourceComponent resource : resources) {
      // Skip OperationDefinition which is read-only.
      if (resource.getType().equals("OperationDefinition")) {
        continue;
      }
      final Set<TypeRestfulInteraction> interactions =
          resource.getInteraction().stream()
              .map(ResourceInteractionComponent::getCode)
              .collect(Collectors.toSet());

      assertThat(interactions)
          .as("Resource type " + resource.getType() + " should not have DELETE interaction")
          .doesNotContain(TypeRestfulInteraction.DELETE);
    }
  }

  @Test
  void capabilityStatementExcludesBatchWhenDisabled() {
    // Given: A configuration with batch disabled.
    final ConformanceProvider provider =
        createProviderWithDisabledOperations(
            ops -> {
              ops.setBatchEnabled(false);
            });

    // When: Getting the capability statement.
    final CapabilityStatement capabilityStatement = provider.getServerConformance(null, null);

    // Then: System interactions should not include BATCH.
    final List<CapabilityStatement.SystemInteractionComponent> interactions =
        capabilityStatement.getRest().get(0).getInteraction();

    final Set<CapabilityStatement.SystemRestfulInteraction> systemInteractions =
        interactions.stream()
            .map(CapabilityStatement.SystemInteractionComponent::getCode)
            .collect(Collectors.toSet());

    assertThat(systemInteractions)
        .as("System interactions should not include BATCH when disabled")
        .doesNotContain(CapabilityStatement.SystemRestfulInteraction.BATCH);
  }

  @Test
  void capabilityStatementIncludesBatchWhenEnabled() {
    // When: Getting the capability statement with default configuration (batch enabled).
    final CapabilityStatement capabilityStatement =
        conformanceProvider.getServerConformance(null, null);

    // Then: System interactions should include BATCH.
    final List<CapabilityStatement.SystemInteractionComponent> interactions =
        capabilityStatement.getRest().get(0).getInteraction();

    final Set<CapabilityStatement.SystemRestfulInteraction> systemInteractions =
        interactions.stream()
            .map(CapabilityStatement.SystemInteractionComponent::getCode)
            .collect(Collectors.toSet());

    assertThat(systemInteractions)
        .as("System interactions should include BATCH when enabled")
        .contains(CapabilityStatement.SystemRestfulInteraction.BATCH);
  }

  @Test
  void capabilityStatementExcludesExportWhenDisabled() {
    // Given: A configuration with system export disabled.
    final ConformanceProvider provider =
        createProviderWithDisabledOperations(
            ops -> {
              ops.setExportEnabled(false);
            });

    // When: Getting the capability statement.
    final CapabilityStatement capabilityStatement = provider.getServerConformance(null, null);

    // Then: System-level operations should not include export.
    final Set<String> operationNames =
        capabilityStatement.getRest().get(0).getOperation().stream()
            .map(CapabilityStatementRestResourceOperationComponent::getName)
            .collect(Collectors.toSet());

    assertThat(operationNames)
        .as("System-level operations should not include export when disabled")
        .doesNotContain("export");
  }

  @Test
  void capabilityStatementExcludesImportWhenDisabled() {
    // Given: A configuration with import disabled.
    final ConformanceProvider provider =
        createProviderWithDisabledOperations(
            ops -> {
              ops.setImportEnabled(false);
            });

    // When: Getting the capability statement.
    final CapabilityStatement capabilityStatement = provider.getServerConformance(null, null);

    // Then: System-level operations should not include import.
    final Set<String> operationNames =
        capabilityStatement.getRest().get(0).getOperation().stream()
            .map(CapabilityStatementRestResourceOperationComponent::getName)
            .collect(Collectors.toSet());

    assertThat(operationNames)
        .as("System-level operations should not include import when disabled")
        .doesNotContain("import");
  }

  @Test
  void capabilityStatementExcludesViewDefinitionRunWhenDisabled() {
    // Given: A configuration with viewdefinition-run disabled.
    final ConformanceProvider provider =
        createProviderWithDisabledOperations(
            ops -> {
              ops.setViewDefinitionRunEnabled(false);
            });

    // When: Getting the capability statement.
    final CapabilityStatement capabilityStatement = provider.getServerConformance(null, null);

    // Then: System-level operations should not include viewdefinition-run.
    final Set<String> operationNames =
        capabilityStatement.getRest().get(0).getOperation().stream()
            .map(CapabilityStatementRestResourceOperationComponent::getName)
            .collect(Collectors.toSet());

    assertThat(operationNames)
        .as("System-level operations should not include viewdefinition-run when disabled")
        .doesNotContain("viewdefinition-run");
  }

  @Test
  void capabilityStatementExcludesViewDefinitionInstanceRunWhenDisabled() {
    // Given: A configuration with instance-level run disabled.
    final ConformanceProvider provider =
        createProviderWithDisabledOperations(
            ops -> {
              ops.setViewDefinitionInstanceRunEnabled(false);
            });

    // When: Getting the capability statement.
    final CapabilityStatement capabilityStatement = provider.getServerConformance(null, null);

    // Then: ViewDefinition resource should not have the $run operation.
    final List<CapabilityStatementRestResourceComponent> resources =
        capabilityStatement.getRest().get(0).getResource();

    final Optional<CapabilityStatementRestResourceComponent> viewDefResource =
        resources.stream().filter(r -> r.getType().equals("ViewDefinition")).findFirst();

    assertThat(viewDefResource).isPresent();

    final Set<String> operations =
        viewDefResource.get().getOperation().stream()
            .map(CapabilityStatementRestResourceOperationComponent::getName)
            .collect(Collectors.toSet());

    assertThat(operations)
        .as("ViewDefinition should not have $run operation when disabled")
        .doesNotContain("run");
  }

  @Test
  void capabilityStatementExcludesPatientExportWhenDisabled() {
    // Given: A configuration with patient export disabled.
    final ConformanceProvider provider =
        createProviderWithDisabledOperations(
            ops -> {
              ops.setPatientExportEnabled(false);
            });

    // When: Getting the capability statement.
    final CapabilityStatement capabilityStatement = provider.getServerConformance(null, null);

    // Then: Patient resource should not have export operation.
    final List<CapabilityStatementRestResourceComponent> resources =
        capabilityStatement.getRest().get(0).getResource();

    final Optional<CapabilityStatementRestResourceComponent> patientResource =
        resources.stream().filter(r -> r.getType().equals("Patient")).findFirst();

    assertThat(patientResource).isPresent();

    final Set<String> operations =
        patientResource.get().getOperation().stream()
            .map(CapabilityStatementRestResourceOperationComponent::getName)
            .collect(Collectors.toSet());

    assertThat(operations)
        .as("Patient resource should not have export operation when patient export disabled")
        .doesNotContain("export");
  }

  @Test
  void capabilityStatementExcludesGroupExportWhenDisabled() {
    // Given: A configuration with group export disabled.
    final ConformanceProvider provider =
        createProviderWithDisabledOperations(
            ops -> {
              ops.setGroupExportEnabled(false);
            });

    // When: Getting the capability statement.
    final CapabilityStatement capabilityStatement = provider.getServerConformance(null, null);

    // Then: Group resource should not have export operation.
    final List<CapabilityStatementRestResourceComponent> resources =
        capabilityStatement.getRest().get(0).getResource();

    final Optional<CapabilityStatementRestResourceComponent> groupResource =
        resources.stream().filter(r -> r.getType().equals("Group")).findFirst();

    assertThat(groupResource).isPresent();

    final Set<String> operations =
        groupResource.get().getOperation().stream()
            .map(CapabilityStatementRestResourceOperationComponent::getName)
            .collect(Collectors.toSet());

    assertThat(operations)
        .as("Group resource should not have export operation when group export disabled")
        .doesNotContain("export");
  }

  /**
   * Helper method to create a ConformanceProvider with custom operation configuration.
   *
   * @param configurer a consumer to configure the OperationConfiguration
   * @return a ConformanceProvider with the configured operations
   */
  private ConformanceProvider createProviderWithDisabledOperations(
      final java.util.function.Consumer<OperationConfiguration> configurer) {
    final ServerConfiguration config = Mockito.mock(ServerConfiguration.class);
    final AuthorizationConfiguration authConfig = Mockito.mock(AuthorizationConfiguration.class);
    final OperationConfiguration opsConfig = new OperationConfiguration();
    configurer.accept(opsConfig);

    Mockito.when(config.getAuth()).thenReturn(authConfig);
    Mockito.when(authConfig.isEnabled()).thenReturn(false);
    Mockito.when(config.getImplementationDescription()).thenReturn("Test Implementation");
    Mockito.when(config.getOperations()).thenReturn(opsConfig);

    final PathlingServerVersion version = Mockito.mock(PathlingServerVersion.class);
    Mockito.when(version.getMajorVersion()).thenReturn(Optional.of("1"));
    Mockito.when(version.getBuildVersion()).thenReturn(Optional.of("1.0.0"));
    Mockito.when(version.getDescriptiveVersion()).thenReturn(Optional.of("1.0.0"));

    final FhirContext fhirContext = FhirContext.forR4();
    final IParser jsonParser = fhirContext.newJsonParser();

    return new ConformanceProvider(config, Optional.empty(), version, fhirContext, jsonParser);
  }
}
