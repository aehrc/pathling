/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2018-2025 Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230.
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
 *
 */

package au.csiro.pathling.encoders.utils;

import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/** Helper application, which find recursive type definitions in FHIR R4. */
public class FindRecursiveTypesApp {

  private final FhirContext fhirContext = FhirContext.forR4();
  private final Map<String, String> path = new HashMap<>();
  private final Collection<String> recursive = new HashSet<>();

  private void traverseDefinition(
      final BaseRuntimeElementDefinition<?> definition, final String name, final int level) {
    final String thisType = definition.getName();
    if (path.containsKey(thisType)) {
      final String parentPath = path.get(thisType);
      final boolean isIndirect = name.substring(parentPath.length() + 1).indexOf('.') > 0;
      recursive.add(
          definition.getName()
              + ": "
              + parentPath
              + "-> "
              + name
              + (isIndirect ? " (indirect)" : ""));
    } else {
      if (!("Reference".equals(definition.getName()) || "Extension".equals(definition.getName()))) {
        path.put(thisType, name);
        final List<BaseRuntimeChildDefinition> children = definition.getChildren();
        // for each child
        for (final BaseRuntimeChildDefinition child : children) {
          for (final String validChildName : child.getValidChildNames()) {
            if (!validChildName.equals("modifierExtension")) {
              traverseDefinition(
                  child.getChildByName(validChildName),
                  name.isBlank() ? validChildName : name + "." + validChildName,
                  level + 1);
            }
          }
        }
        path.remove(thisType);
      }
    }
  }

  private void findRecursiveTypes() {
    System.out.println(">>> Listing recursive types:");
    for (final String resourceType : fhirContext.getResourceTypes()) {
      final RuntimeResourceDefinition rd = fhirContext.getResourceDefinition(resourceType);

      recursive.clear();
      path.clear();
      traverseDefinition(rd, "", 0);
      if (!recursive.isEmpty()) {
        System.out.println(resourceType);
        recursive.forEach(s -> System.out.println("  " + s));
      }
    }
  }

  public static void main(final String[] args) {
    new FindRecursiveTypesApp().findRecursiveTypes();
  }
}
