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
import au.csiro.pathling.config.ServerConfiguration;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.hl7.fhir.r4.model.CapabilityStatement;
import org.hl7.fhir.r4.model.CapabilityStatement.CapabilityStatementRestResourceComponent;
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
    final AuthorizationConfiguration authConfiguration = Mockito.mock(
        AuthorizationConfiguration.class);
    Mockito.when(configuration.getAuth()).thenReturn(authConfiguration);
    Mockito.when(authConfiguration.isEnabled()).thenReturn(false);
    Mockito.when(configuration.getImplementationDescription()).thenReturn("Test Implementation");

    final PathlingServerVersion version = Mockito.mock(PathlingServerVersion.class);
    Mockito.when(version.getMajorVersion()).thenReturn(Optional.of("1"));
    Mockito.when(version.getBuildVersion()).thenReturn(Optional.of("1.0.0"));
    Mockito.when(version.getDescriptiveVersion()).thenReturn(Optional.of("1.0.0"));

    final FhirContext fhirContext = FhirContext.forR4();
    final IParser jsonParser = fhirContext.newJsonParser();

    conformanceProvider = new ConformanceProvider(configuration, Optional.empty(), version,
        fhirContext, jsonParser);
  }

  @Test
  void capabilityStatementIncludesCreateInteractionForAllResourceTypes() {
    // When: Getting the capability statement.
    final CapabilityStatement capabilityStatement = conformanceProvider.getServerConformance(null,
        null);

    // Then: All supported resource types should have CREATE interaction.
    final Set<ResourceType> supportedResourceTypes = FhirServer.supportedResourceTypes();
    final List<CapabilityStatementRestResourceComponent> resources = capabilityStatement.getRest()
        .get(0).getResource();

    for (final ResourceType resourceType : supportedResourceTypes) {
      final Optional<CapabilityStatementRestResourceComponent> resourceComponent = resources
          .stream()
          .filter(r -> r.getType().equals(resourceType.toCode()))
          .findFirst();

      assertThat(resourceComponent).isPresent();

      final Set<TypeRestfulInteraction> interactions = resourceComponent.get().getInteraction()
          .stream()
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
    final CapabilityStatement capabilityStatement = conformanceProvider.getServerConformance(null,
        null);

    // Then: ViewDefinition should have CREATE interaction.
    final List<CapabilityStatementRestResourceComponent> resources = capabilityStatement.getRest()
        .get(0).getResource();

    final Optional<CapabilityStatementRestResourceComponent> viewDefResource = resources.stream()
        .filter(r -> r.getType().equals("ViewDefinition"))
        .findFirst();

    assertThat(viewDefResource).isPresent();

    final Set<TypeRestfulInteraction> interactions = viewDefResource.get().getInteraction()
        .stream()
        .map(ResourceInteractionComponent::getCode)
        .collect(Collectors.toSet());

    assertThat(interactions).contains(TypeRestfulInteraction.CREATE);
  }

  @Test
  void capabilityStatementIncludesAllCrudInteractionsForResourceTypes() {
    // When: Getting the capability statement.
    final CapabilityStatement capabilityStatement = conformanceProvider.getServerConformance(null,
        null);

    // Then: All supported resource types should have READ, SEARCH, UPDATE, and CREATE.
    final Set<ResourceType> supportedResourceTypes = FhirServer.supportedResourceTypes();
    final List<CapabilityStatementRestResourceComponent> resources = capabilityStatement.getRest()
        .get(0).getResource();

    for (final ResourceType resourceType : supportedResourceTypes) {
      final Optional<CapabilityStatementRestResourceComponent> resourceComponent = resources
          .stream()
          .filter(r -> r.getType().equals(resourceType.toCode()))
          .findFirst();

      assertThat(resourceComponent).isPresent();

      final Set<TypeRestfulInteraction> interactions = resourceComponent.get().getInteraction()
          .stream()
          .map(ResourceInteractionComponent::getCode)
          .collect(Collectors.toSet());

      assertThat(interactions)
          .as("Resource type " + resourceType.toCode() + " should have all CRUD interactions")
          .contains(
              TypeRestfulInteraction.READ,
              TypeRestfulInteraction.SEARCHTYPE,
              TypeRestfulInteraction.UPDATE,
              TypeRestfulInteraction.CREATE
          );
    }
  }

}
