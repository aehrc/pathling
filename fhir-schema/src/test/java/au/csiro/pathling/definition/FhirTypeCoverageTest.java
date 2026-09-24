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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Tests that every element the R4 definitions describe reports its type.
 *
 * <p>The definitions report a type as a value of the R4 enumeration, which fails for a code the
 * enumeration does not contain. The transform consults the type of every element a source carries,
 * and a dense schema will visit every element the definitions describe, so such a code would fail a
 * whole transform rather than a single query.
 */
class FhirTypeCoverageTest {

  @Test
  void reportsTheTypeOfEveryElementTheDefinitionsDescribe() {
    final FhirContext fhirContext = FhirContext.forR4();
    final DefinitionContext definitions = FhirDefinitionContext.of(fhirContext);
    final Set<Object> visited = new HashSet<>();
    final Set<String> typed = new HashSet<>();
    for (final String resourceType : fhirContext.getResourceTypes()) {
      visit(definitions.findResourceDefinition(resourceType).getChildren(), visited, typed);
    }
    // A walk that reached nothing would pass every assertion within it.
    assertTrue(
        typed.size() > 50,
        "Expected the walk to reach a substantial number of types, but it reached " + typed);
  }

  private static void visit(
      @Nonnull final List<ChildDefinition> children,
      @Nonnull final Set<Object> visited,
      @Nonnull final Set<String> typed) {
    for (final ChildDefinition child : children) {
      if (child instanceof final ElementDefinition element) {
        assertDoesNotThrow(element::getFhirType, element.getElementName())
            .ifPresent(type -> typed.add(type.toCode()));
      }
      // A type reached by several paths is walked once, which is also what stops the recursion
      // through extensions and recursive backbone elements.
      if (visited.add(child.getTypeIdentity())) {
        visit(child.getChildren(), visited, typed);
      }
    }
  }
}
