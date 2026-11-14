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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.definition.ElementDefinition;
import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementCompositeDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeChildAny;
import ca.uhn.fhir.context.RuntimeChildChoiceDefinition;
import ca.uhn.fhir.context.RuntimeChildExtension;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import ca.uhn.fhir.model.api.annotation.Child;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Extension;
import org.junit.jupiter.api.Test;

/**
 * Pins the expansion of every choice in every R4 resource against the names the definition library
 * says are valid for it, less the names that no FHIR instance can populate.
 *
 * <p>The order of an expansion is taken from the declared type list rather than from the set of
 * valid names, because the set is hash ordered. This sweep is what says the two describe the same
 * variants: a declared type that maps to no name, or to a name outside the valid set, would
 * silently drop a variant from every stored structure that carries the choice.
 *
 * <p>The valid names are not the expansion, because the definition library also accepts an alias
 * per resource a reference can target, and an alias for an untyped resource. FHIR declares one
 * reference, so the oracle subtracts those aliases from the valid names. It is computed from the
 * definition library alone, never from the expansion under test, so it bites in both directions: an
 * over-eager collapse makes the expansion too small, and an under-eager one too large.
 *
 * <p>The order is pinned too, because it is part of the type of every structure that carries the
 * choice (FR-057). For a declared choice the oracle is the type list of the child annotation. An
 * open choice declares no types, and the definition library fills it from a hash map keyed by type
 * class, so its order is that of identity hashes; the oracle is then the specification's own list,
 * {@link #R4_OPEN_TYPES}, followed by the types the library accepts beyond it, ordered by name.
 */
class ChoiceExpansionSweepTest {

  @Nonnull private static final FhirContext CONTEXT = FhirContext.forR4();

  /**
   * The types of {@code Extension.value[x]} in the order its R4 definition lists them, copied from
   * {@code StructureDefinition-Extension.json} in the {@code hl7.fhir.r4.core} 4.0.1 package. Every
   * open choice in R4 lists the same types in the same order.
   */
  @Nonnull
  private static final List<String> R4_OPEN_TYPES =
      List.of(
          "base64Binary",
          "boolean",
          "canonical",
          "code",
          "date",
          "dateTime",
          "decimal",
          "id",
          "instant",
          "integer",
          "markdown",
          "oid",
          "positiveInt",
          "string",
          "time",
          "unsignedInt",
          "uri",
          "url",
          "uuid",
          "Address",
          "Age",
          "Annotation",
          "Attachment",
          "CodeableConcept",
          "Coding",
          "ContactPoint",
          "Count",
          "Distance",
          "Duration",
          "HumanName",
          "Identifier",
          "Money",
          "Period",
          "Quantity",
          "Range",
          "Ratio",
          "Reference",
          "SampledData",
          "Signature",
          "Timing",
          "ContactDetail",
          "Contributor",
          "DataRequirement",
          "Expression",
          "ParameterDefinition",
          "RelatedArtifact",
          "TriggerDefinition",
          "UsageContext",
          "Dosage",
          "Meta");

  /**
   * The types the definition library accepts in an open choice beyond the specification's list. No
   * conformant instance can carry them, but they are kept (decision 45), after the listed types.
   */
  @Nonnull
  private static final Set<String> R4_OPEN_TYPE_EXTRAS =
      Set.of(
          "ElementDefinition",
          "Extension",
          "MarketingStatus",
          "Narrative",
          "Population",
          "ProdCharacteristic",
          "ProductShelfLife",
          "SubstanceAmount",
          "Xhtml");

  @Test
  void expandsEveryChoiceToExactlyTheVariantsTheDefinitionsValidate() {
    final Set<BaseRuntimeElementDefinition<?>> visited = new HashSet<>();
    final List<String> checked = new ArrayList<>();
    for (final ResourceType type : ResourceType.values()) {
      resourceDefinition(type).ifPresent(definition -> visit(definition, visited, checked));
    }
    // A sweep that visited nothing would pass every assertion within it.
    assertTrue(
        checked.size() > 100,
        "Expected the sweep to reach a substantial number of choices, but it reached "
            + checked.size());
  }

  @Nonnull
  private static Optional<RuntimeResourceDefinition> resourceDefinition(
      @Nonnull final ResourceType type) {
    if (ResourceType.NULL.equals(type)) {
      return Optional.empty();
    }
    try {
      return Optional.ofNullable(CONTEXT.getResourceDefinition(type.toCode()));
    } catch (final RuntimeException e) {
      // A type the definition library does not carry a structure for.
      return Optional.empty();
    }
  }

  private static void visit(
      @jakarta.annotation.Nullable final BaseRuntimeElementDefinition<?> definition,
      @Nonnull final Set<BaseRuntimeElementDefinition<?>> visited,
      @Nonnull final List<String> checked) {
    if (!(definition instanceof final BaseRuntimeElementCompositeDefinition<?> composite)
        || !visited.add(definition)) {
      return;
    }
    for (final BaseRuntimeChildDefinition child : composite.getChildren()) {
      if (child instanceof final RuntimeChildChoiceDefinition choice
          && !(child instanceof RuntimeChildExtension)) {
        check(choice, checked);
      }
      if (child instanceof RuntimeChildExtension) {
        // An extension child cannot be resolved by name, because HAPI resolves every extension
        // child against the name of the plain extension element. The type is taken directly, so
        // that the open choice within an extension is still reached.
        visit(child.getChildElementDefinitionByDatatype(Extension.class), visited, checked);
        continue;
      }
      for (final String name : child.getValidChildNames()) {
        final BaseRuntimeElementDefinition<?> childElement = child.getChildByName(name);
        if (childElement != null) {
          visit(childElement, visited, checked);
        }
      }
    }
  }

  private static void check(
      @Nonnull final RuntimeChildChoiceDefinition choice, @Nonnull final List<String> checked) {
    final String path = choice.getElementName();
    final List<String> expanded =
        new FhirChoiceDefinition(choice)
            .getAllChildTypes().stream().map(ElementDefinition::getElementName).toList();

    assertEquals(
        expanded.size(),
        Set.copyOf(expanded).size(),
        "Duplicate variant in the expansion of " + path);
    assertEquals(
        expectedVariants(choice),
        Set.copyOf(expanded),
        "The expansion of " + path + " does not describe the variants the definitions validate");
    assertEquals(expectedOrder(choice), expanded, "The expansion of " + path + " is out of order");
    checked.add(path);
  }

  /**
   * Returns the order the variants of a choice are expected in: the declared type list for a
   * declared choice, and the specification's list of open types for an open one, with each
   * reference target collapsed into the one reference variant at the position of the first.
   */
  @Nonnull
  private static List<String> expectedOrder(@Nonnull final RuntimeChildChoiceDefinition choice) {
    final String elementName = choice.getElementName();
    if (choice instanceof RuntimeChildAny) {
      final List<String> listed =
          R4_OPEN_TYPES.stream().map(type -> elementName + StringUtils.capitalize(type)).toList();
      final List<String> extras =
          R4_OPEN_TYPE_EXTRAS.stream().map(type -> elementName + type).sorted().toList();
      return Stream.concat(listed.stream(), extras.stream()).toList();
    }
    final Child annotation = choice.getField().getAnnotation(Child.class);
    return Stream.of(annotation.type())
        .map(
            type ->
                choice.getResourceTypes().contains(type)
                    ? elementName + "Reference"
                    : choice.getChildNameByDatatype(type))
        .distinct()
        .toList();
  }

  /**
   * Returns the names the definitions validate that a FHIR instance can actually carry: every valid
   * name, less the alias the definition library accepts for each resource a reference can target,
   * and less the alias it accepts for an untyped resource.
   */
  @Nonnull
  private static Set<String> expectedVariants(@Nonnull final RuntimeChildChoiceDefinition choice) {
    final String elementName = choice.getElementName();
    final Set<String> aliases = new HashSet<>();
    choice.getResourceTypes().forEach(target -> aliases.add(elementName + target.getSimpleName()));
    aliases.add(elementName + "Resource");
    final Set<String> expected = new HashSet<>(choice.getValidChildNames());
    expected.removeAll(aliases);
    return expected;
  }
}
