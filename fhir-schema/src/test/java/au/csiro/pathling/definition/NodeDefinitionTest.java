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
package au.csiro.pathling.definition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests the enumeration of the children of a definition, against the FHIR R4 definitions of a
 * resource, a backbone element and a complex type.
 */
class NodeDefinitionTest {

  @Nonnull
  private static final DefinitionContext DEFINITIONS =
      FhirDefinitionContext.of(FhirContext.forR4());

  @Nonnull
  private static List<String> childNames(@Nonnull final NodeDefinition definition) {
    return definition.getChildren().stream().map(ChildDefinition::getName).toList();
  }

  @Nonnull
  private static NodeDefinition childOf(
      @Nonnull final NodeDefinition parent, @Nonnull final String name) {
    return parent.getChildElement(name).orElseThrow();
  }

  @Nonnull
  private static List<String> variantNames(
      @Nonnull final NodeDefinition parent, @Nonnull final String name) {
    final ChoiceDefinition choice = (ChoiceDefinition) childOf(parent, name);
    return choice.getAllChildTypes().stream().map(ElementDefinition::getElementName).toList();
  }

  @Test
  void enumeratesTheChildrenOfAResourceInDeclarationOrder() {
    // These are the elements of Patient in R4, in the order the specification declares them, with
    // the choice elements named without their type suffix.
    final List<String> expected =
        List.of(
            "id",
            "meta",
            "implicitRules",
            "language",
            "text",
            "contained",
            "extension",
            "modifierExtension",
            "identifier",
            "active",
            "name",
            "telecom",
            "gender",
            "birthDate",
            "deceased",
            "address",
            "maritalStatus",
            "multipleBirth",
            "photo",
            "contact",
            "communication",
            "generalPractitioner",
            "managingOrganization",
            "link");
    assertEquals(expected, childNames(DEFINITIONS.findResourceDefinition("Patient")));
  }

  @Test
  void enumeratesTheChildrenOfABackboneElementInDeclarationOrder() {
    final NodeDefinition contact =
        childOf(DEFINITIONS.findResourceDefinition("Patient"), "contact");
    final List<String> expected =
        List.of(
            "id",
            "extension",
            "modifierExtension",
            "relationship",
            "name",
            "telecom",
            "address",
            "gender",
            "organization",
            "period");
    assertEquals(expected, childNames(contact));
  }

  @Test
  void enumeratesTheChildrenOfAComplexTypeInDeclarationOrder() {
    final NodeDefinition humanName = childOf(DEFINITIONS.findResourceDefinition("Patient"), "name");
    final List<String> expected =
        List.of("id", "extension", "use", "text", "family", "given", "prefix", "suffix", "period");
    assertEquals(expected, childNames(humanName));
  }

  @Test
  void reportsAChoiceOnceAsAChoiceRatherThanPreExpanded() {
    final List<ChildDefinition> children =
        DEFINITIONS.findResourceDefinition("Patient").getChildren();

    final List<ChildDefinition> deceased =
        children.stream().filter(child -> "deceased".equals(child.getName())).toList();
    assertEquals(1, deceased.size());
    assertInstanceOf(ChoiceDefinition.class, deceased.get(0));

    // The expanded variants are reachable through the choice, but must not appear beside it.
    final List<String> names = childNames(DEFINITIONS.findResourceDefinition("Patient"));
    assertFalse(names.contains("deceasedBoolean"));
    assertFalse(names.contains("deceasedDateTime"));

    final ChoiceDefinition choice = (ChoiceDefinition) deceased.get(0);
    assertEquals(
        List.of("deceasedBoolean", "deceasedDateTime"),
        choice.getAllChildTypes().stream().map(ElementDefinition::getElementName).toList());
  }

  @Test
  void reportsNoChildrenForAPrimitiveElement() {
    final NodeDefinition birthDate =
        childOf(DEFINITIONS.findResourceDefinition("Patient"), "birthDate");
    assertTrue(birthDate.getChildren().isEmpty());
  }

  @Test
  void expandsAChoiceInTheOrderTheDefinitionsDeclareIt() {
    // The order of the expanded variants is the order the specification declares the types in, so
    // that the field order of a stored structure is reproducible by another implementation of the
    // layout and stable across an upgrade of the definition library.
    assertEquals(
        List.of(
            "valueQuantity",
            "valueCodeableConcept",
            "valueString",
            "valueBoolean",
            "valueInteger",
            "valueRange",
            "valueRatio",
            "valueSampledData",
            "valueTime",
            "valueDateTime",
            "valuePeriod"),
        variantNames(DEFINITIONS.findResourceDefinition("Observation"), "value"));
    assertEquals(
        List.of("multipleBirthBoolean", "multipleBirthInteger"),
        variantNames(DEFINITIONS.findResourceDefinition("Patient"), "multipleBirth"));
  }

  @Test
  void expandsAReferenceBearingChoiceWithEachDeclaredTypeInItsDeclaredPosition() {
    // A choice that admits a reference declares the resources it targets rather than the reference
    // itself, and the definition library maps those to no name. The name is recovered so that a
    // declared target keeps its declared position; only the names that are declared nowhere — the
    // plain reference and the untyped resource — follow in the stated order that ends the list.
    assertEquals(
        List.of(
            "productMedication",
            "productSubstance",
            "productCodeableConcept",
            "productReference",
            "productResource"),
        variantNames(DEFINITIONS.findResourceDefinition("ActivityDefinition"), "product"));

    final NodeDefinition trigger =
        childOf(childOf(DEFINITIONS.findResourceDefinition("PlanDefinition"), "action"), "trigger");
    assertEquals(
        List.of(
            "timingTiming",
            "timingSchedule",
            "timingDate",
            "timingDateTime",
            "timingReference",
            "timingResource"),
        variantNames(trigger, "timing"));
  }
}
