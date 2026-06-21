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

package au.csiro.pathling.operations.view;

import static org.assertj.core.api.Assertions.assertThat;

import org.hl7.fhir.r4.model.Reference;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ReferenceParameters}, the logical-id extractor shared by the run and export
 * operations' reference parameters.
 *
 * @author John Grimes
 */
class ReferenceParametersTest {

  @Test
  void extractsIdFromTypedRelativeReference() {
    // A literal relative reference of the form ResourceType/id yields its id part.
    assertThat(ReferenceParameters.extractId(new Reference("Patient/123"))).isEqualTo("123");
  }

  @Test
  void extractsIdFromViewDefinitionReference() {
    assertThat(ReferenceParameters.extractId(new Reference("ViewDefinition/abc"))).isEqualTo("abc");
  }

  @Test
  void extractsIdFromBareIdReference() {
    // A bare id (no resource type prefix) is itself the logical id.
    assertThat(ReferenceParameters.extractId(new Reference("abc"))).isEqualTo("abc");
  }

  @Test
  void returnsNullForNullReference() {
    assertThat(ReferenceParameters.extractId(null)).isNull();
  }

  @Test
  void returnsNullForEmptyReference() {
    // A reference carrying no literal reference string is not resolvable.
    assertThat(ReferenceParameters.extractId(new Reference())).isNull();
  }

  @Test
  void returnsNullForReferenceWithOnlyDisplay() {
    final Reference reference = new Reference();
    reference.setDisplay("A view with no literal reference");
    assertThat(ReferenceParameters.extractId(reference)).isNull();
  }

  @Test
  void returnsNullForAbsoluteUrlReference() {
    // Absolute URLs are out of scope for resolution and are treated as unresolvable.
    assertThat(
            ReferenceParameters.extractId(
                new Reference("http://example.com/fhir/ViewDefinition/abc")))
        .isNull();
  }
}
