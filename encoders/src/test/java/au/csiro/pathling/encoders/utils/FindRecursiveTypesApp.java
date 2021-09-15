package au.csiro.pathling.encoders.utils;

import ca.uhn.fhir.context.BaseRuntimeChildDefinition;
import ca.uhn.fhir.context.BaseRuntimeElementDefinition;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import java.util.*;


/**
 * Helper application, which find recursive type definitions in FHIR R4.
 */
public class FindRecursiveTypesApp {

  private final FhirContext fhirContext = FhirContext.forR4();
  private final Map<String, String> path = new HashMap<>();
  private final Set<String> recursive = new HashSet<>();

  void traverseDefinition(BaseRuntimeElementDefinition<?> definition, String name, int level) {
    final String thisType = definition.getName();
    if (path.containsKey(thisType)) {
      String parentPath = path.get(thisType);
      boolean isIndirect = name.substring(parentPath.length() + 1).indexOf('.') > 0;
      recursive.add(definition.getName() + ": " + parentPath + "-> " + name + (isIndirect
                                                                               ? " (indirect)"
                                                                               : ""));
    } else {
      if (!("Reference".equals(definition.getName()) || "Extension".equals(definition.getName()))) {
        path.put(thisType, name);
        List<BaseRuntimeChildDefinition> children = definition.getChildren();
        // for each child
        for (BaseRuntimeChildDefinition child : children) {
          for (String validChildName : child.getValidChildNames()) {
            if (!validChildName.equals("modifierExtension")) {
              traverseDefinition(child.getChildByName(validChildName), name.isBlank()
                                                                       ? validChildName
                                                                       : name + "."
                                                                           + validChildName,
                  level + 1);
            }
          }
        }
        path.remove(thisType);
      }
    }
  }

  void findRecursiveTypes() {
    System.out.println(">>> Listing recursive types:");
    for (String resourceType : fhirContext.getResourceTypes()) {
      RuntimeResourceDefinition rd = fhirContext
          .getResourceDefinition(resourceType);

      recursive.clear();
      path.clear();
      traverseDefinition(rd, "", 0);
      if (!recursive.isEmpty()) {
        System.out.println(resourceType);
        recursive.forEach(s -> System.out.println("  " + s));
      }
    }
  }

  public static void main(String[] args) {
    new FindRecursiveTypesApp().findRecursiveTypes();
  }
}
