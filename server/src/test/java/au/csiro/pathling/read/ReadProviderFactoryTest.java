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

package au.csiro.pathling.read;

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.ViewDefinitionResource;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.util.CustomObjectDataSource;
import au.csiro.pathling.util.FhirServerTestConfiguration;
import ca.uhn.fhir.context.FhirContext;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

/**
 * Tests for {@link ReadProviderFactory}.
 *
 * @author John Grimes
 */
@Import(FhirServerTestConfiguration.class)
@SpringBootUnitTest
class ReadProviderFactoryTest {

  @Autowired private FhirContext fhirContext;

  @Autowired private SparkSession sparkSession;

  @Autowired private PathlingContext pathlingContext;

  @Autowired private FhirEncoders fhirEncoders;

  private ReadProviderFactory readProviderFactory;

  @BeforeEach
  void setUp() {
    // Create a minimal data source for testing factory creation.
    final List<IBaseResource> resources = new ArrayList<>();
    final CustomObjectDataSource dataSource =
        new CustomObjectDataSource(sparkSession, pathlingContext, fhirEncoders, resources);

    readProviderFactory = new ReadProviderFactory(fhirContext, dataSource, fhirEncoders);
  }

  // -------------------------------------------------------------------------
  // Test creating providers for standard resource types
  // -------------------------------------------------------------------------

  @Test
  void createReadProviderForPatient() {
    // When: creating a ReadProvider for Patient.
    final ReadProvider provider = readProviderFactory.createReadProvider(ResourceType.PATIENT);

    // Then: the provider should be configured for Patient resources.
    assertThat(provider).isNotNull();
    assertThat(provider.getResourceType()).isEqualTo(Patient.class);
  }

  @Test
  void createReadProviderForObservation() {
    // When: creating a ReadProvider for Observation.
    final ReadProvider provider = readProviderFactory.createReadProvider(ResourceType.OBSERVATION);

    // Then: the provider should be configured for Observation resources.
    assertThat(provider).isNotNull();
    assertThat(provider.getResourceType()).isEqualTo(Observation.class);
  }

  @Test
  void createReadProvidersForDifferentTypesAreDistinct() {
    // When: creating ReadProviders for different resource types.
    final ReadProvider patientProvider =
        readProviderFactory.createReadProvider(ResourceType.PATIENT);
    final ReadProvider observationProvider =
        readProviderFactory.createReadProvider(ResourceType.OBSERVATION);

    // Then: the providers should be distinct instances with different resource types.
    assertThat(patientProvider).isNotSameAs(observationProvider);
    assertThat(patientProvider.getResourceType())
        .isNotEqualTo(observationProvider.getResourceType());
  }

  // -------------------------------------------------------------------------
  // Test creating providers for custom resource types
  // -------------------------------------------------------------------------

  @Test
  void createReadProviderForViewDefinition() {
    // When: creating a ReadProvider for ViewDefinition (custom resource type).
    final ReadProvider provider = readProviderFactory.createReadProvider("ViewDefinition");

    // Then: the provider should be configured for ViewDefinition resources.
    assertThat(provider).isNotNull();
    assertThat(provider.getResourceType()).isEqualTo(ViewDefinitionResource.class);
  }

  @Test
  void createReadProviderForViewDefinitionViaStringCode() {
    // When: creating a ReadProvider using string resource code.
    final ReadProvider provider = readProviderFactory.createReadProvider("ViewDefinition");

    // Then: the provider should handle ViewDefinition resources.
    assertThat(provider).isNotNull();
    assertThat(provider.getResourceType()).isEqualTo(ViewDefinitionResource.class);
  }

  // -------------------------------------------------------------------------
  // Test that created providers are properly configured
  // -------------------------------------------------------------------------

  @Test
  void createdProvidersAreProperlyConfigured() {
    // When: creating a ReadProvider via the factory.
    final ReadProvider provider = readProviderFactory.createReadProvider(ResourceType.PATIENT);

    // Then: the provider should be properly configured with its dependencies.
    assertThat(provider).isNotNull();
    assertThat(provider.getResourceType()).isNotNull();
  }
}
