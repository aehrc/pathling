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

package au.csiro.pathling.definition.fhir;

import static au.csiro.pathling.utilities.Functions.maybeCast;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import au.csiro.pathling.definition.ChildDefinition;
import au.csiro.pathling.definition.DefinitionContext;
import au.csiro.pathling.definition.ElementDefinition;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import org.junit.jupiter.api.Test;

/**
 * Tests that a reference resolves to a {@link FhirReferenceDefinition}, whichever route reaches it:
 * directly, through a choice, or through the open choice of an extension.
 */
class NodeDefinitionTest {

  @Nonnull
  private static final DefinitionContext DEFINITIONS =
      FhirDefinitionContext.of(FhirContext.forR4());

  @Test
  void resolvesAPlainReferenceElement() {
    final ChildDefinition referenceDefinition =
        DEFINITIONS.findResourceDefinition("Condition").getChildElement("subject").orElseThrow();
    assertInstanceOf(FhirReferenceDefinition.class, referenceDefinition);
  }

  @Test
  void resolvesAReferenceVariantOfAChoice() {
    final ChildDefinition medicationValue =
        DEFINITIONS
            .findResourceDefinition("MedicationDispense")
            .getChildElement("medication")
            .orElseThrow();
    assertInstanceOf(FhirChoiceDefinition.class, medicationValue);
    final ElementDefinition referenceDefinition =
        ((FhirChoiceDefinition) medicationValue).getChildByType("Reference").orElseThrow();
    assertInstanceOf(FhirReferenceDefinition.class, referenceDefinition);
    assertEquals("medicationReference", referenceDefinition.getElementName());
  }

  @Test
  void resolvesAReferenceVariantOfAnExtensionsOpenChoice() {
    final ElementDefinition referenceDefinition =
        DEFINITIONS
            .findResourceDefinition("Patient")
            .getChildElement("extension")
            .flatMap(extension -> extension.getChildElement("value"))
            .flatMap(maybeCast(FhirChoiceDefinition.class))
            .flatMap(value -> value.getChildByType("Reference"))
            .orElseThrow();
    assertInstanceOf(FhirReferenceDefinition.class, referenceDefinition);
    assertEquals("valueReference", referenceDefinition.getElementName());
  }
}
