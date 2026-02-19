/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.operations.delete;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ca.uhn.fhir.context.FhirContext;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Patient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationContext;

/**
 * Tests for {@link DeleteProviderFactory} verifying that providers are created via the Spring
 * application context for the correct resource types.
 *
 * @author John Grimes
 */
@Tag("UnitTest")
@ExtendWith(MockitoExtension.class)
class DeleteProviderFactoryTest {

  @Mock private ApplicationContext applicationContext;
  @Mock private DeleteExecutor deleteExecutor;

  private DeleteProviderFactory factory;
  private FhirContext fhirContext;

  @BeforeEach
  void setUp() {
    fhirContext = FhirContext.forR4();
    factory = new DeleteProviderFactory(applicationContext, fhirContext, deleteExecutor);
  }

  @Test
  void createsProviderForResourceType() {
    // The factory should request a DeleteProvider bean for the resolved resource class.
    final DeleteProvider mockProvider = mock(DeleteProvider.class);
    when(applicationContext.getBean(eq(DeleteProvider.class), any(), any(), any()))
        .thenReturn(mockProvider);

    final DeleteProvider result = factory.createDeleteProvider(ResourceType.PATIENT);

    assertNotNull(result);
    verify(applicationContext)
        .getBean(DeleteProvider.class, deleteExecutor, fhirContext, Patient.class);
  }

  @Test
  void createsProviderForResourceTypeCode() {
    // The factory should also support creation by string resource type code.
    final DeleteProvider mockProvider = mock(DeleteProvider.class);
    when(applicationContext.getBean(eq(DeleteProvider.class), any(), any(), any()))
        .thenReturn(mockProvider);

    final DeleteProvider result = factory.createDeleteProvider("Patient");

    assertNotNull(result);
    verify(applicationContext)
        .getBean(DeleteProvider.class, deleteExecutor, fhirContext, Patient.class);
  }
}
