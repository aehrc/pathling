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

package au.csiro.pathling.operations.viewexport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import au.csiro.pathling.views.FhirView;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ViewInput}.
 *
 * @author John Grimes
 */
class ViewInputTest {

  // -------------------------------------------------------------------------
  // getEffectiveName tests
  // -------------------------------------------------------------------------

  @Test
  void effectiveNameReturnsProvidedName() {
    // When an explicit name is provided, it should be returned.
    final FhirView mockView = mock(FhirView.class);
    when(mockView.getResource()).thenReturn("Patient");
    final ViewInput input = new ViewInput("my_custom_name", mockView);

    assertThat(input.getEffectiveName(0)).isEqualTo("my_custom_name");
  }

  @Test
  void effectiveNameFallsBackToResourceTypeWithIndex() {
    // When name is null, it should fall back to the view's resource type with index suffix.
    final FhirView mockView = mock(FhirView.class);
    when(mockView.getResource()).thenReturn("Patient");
    final ViewInput input = new ViewInput(null, mockView);

    assertThat(input.getEffectiveName(0)).isEqualTo("patient_0");
  }

  @Test
  void effectiveNameUsesResourceTypeLowercased() {
    // Resource type should be lowercased.
    final FhirView mockView = mock(FhirView.class);
    when(mockView.getResource()).thenReturn("Observation");
    final ViewInput input = new ViewInput(null, mockView);

    assertThat(input.getEffectiveName(2)).isEqualTo("observation_2");
  }

  @Test
  void effectiveNamePrefersNameOverResourceType() {
    // Even when resource type is available, explicit name takes priority.
    final FhirView mockView = mock(FhirView.class);
    when(mockView.getResource()).thenReturn("Observation");
    final ViewInput input = new ViewInput("custom_observations", mockView);

    assertThat(input.getEffectiveName(3)).isEqualTo("custom_observations");
  }

  // -------------------------------------------------------------------------
  // Record accessor tests
  // -------------------------------------------------------------------------

  @Test
  void viewAccessorReturnsView() {
    final FhirView view = new FhirView();
    view.setResource("Patient");
    final ViewInput input = new ViewInput("name", view);

    assertThat(input.view()).isSameAs(view);
  }

  @Test
  void nameAccessorReturnsName() {
    final FhirView view = new FhirView();
    view.setResource("Patient");
    final ViewInput input = new ViewInput("test_name", view);

    assertThat(input.name()).isEqualTo("test_name");
  }

  @Test
  void nameAccessorReturnsNullWhenNotProvided() {
    final FhirView view = new FhirView();
    view.setResource("Patient");
    final ViewInput input = new ViewInput(null, view);

    assertThat(input.name()).isNull();
  }

}
