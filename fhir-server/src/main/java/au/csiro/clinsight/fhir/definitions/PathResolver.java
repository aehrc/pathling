/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir.definitions;

import static au.csiro.clinsight.fhir.definitions.PathTraversal.ResolvedElementType.*;
import static au.csiro.clinsight.fhir.definitions.ResourceDefinitions.checkInitialised;
import static au.csiro.clinsight.fhir.definitions.ResourceDefinitions.getElementsForType;
import static au.csiro.clinsight.fhir.definitions.ResourceDefinitions.supportedPrimitiveTypes;
import static au.csiro.clinsight.utilities.Strings.tokenizePath;
import static au.csiro.clinsight.utilities.Strings.untokenizePath;

import au.csiro.clinsight.fhir.definitions.exceptions.ElementNotKnownException;
import au.csiro.clinsight.fhir.definitions.exceptions.ResourceNotKnownException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

public class PathResolver {

  /**
   * Returns a PathTraversal matching a given path. Only works for base resources currently.
   */
  @Nonnull
  public static PathTraversal resolvePath(@Nonnull String path)
      throws ResourceNotKnownException, ElementNotKnownException {
    checkInitialised();

    LinkedList<String> tail = tokenizePath(path);
    LinkedList<String> head = new LinkedList<>();
    head.push(tail.pop());
    String resourceOrDataType = head.peek();

    // Get the StructureDefinition that describes the parent Resource or DataType.
    assert resourceOrDataType != null;
    Map<String, ElementDefinition> elements = getElementsForType(resourceOrDataType);
    if (elements == null) {
      throw new ResourceNotKnownException("Resource or data type not known: " + resourceOrDataType);
    }

    // Initialise a new object to hold the result.
    PathTraversal result = new PathTraversal();
    result.setPath(path);

    // Go through the full path, starting with just the first component and adding one component
    // with each iteration of the loop. This allows us to pick up multiple cardinalities and
    // transitions into complex types.
    while (true) {
      String currentPath = untokenizePath(head);
      ElementDefinition elementDefinition = elements.get(currentPath);

      // If the path cannot be found in the set of valid elements for this definition, loop back
      // around, add another component and try again. Also skip elements without a type or
      // cardinality, i.e. root elements.
      if (elementDefinition == null || elementDefinition.getMaxCardinality() == null) {
        if (tail.size() == 0) {
          if (elementDefinition == null) {
            throw new ElementNotKnownException("Element not known: " + path);
          }
          result.setType(RESOURCE);
          result.setElementDefinition(elementDefinition);
          return result;
        }
        head.add(tail.pop());
        continue;
      }

      // Save information about a multi-value traversal, if this is one.
      harvestMultiValueTraversal(result, elementDefinition);

      // If this is a `BackboneElement`, we either keep going along the path, or return it if it is
      // the last element in the path.
      if (elementDefinition.getTypeCode().equals("BackboneElement")) {
        if (tail.size() == 0) {
          result.setType(BACKBONE);
          result.setElementDefinition(elementDefinition);
          return result;
        }
        head.add(tail.pop());
      } else {
        return examineLeafElement(tail, elementDefinition, result, currentPath);
      }
    }
  }

  /**
   * Checks if a PathTraversal has a cardinality greater than one, and adds a MultiValueTraversal
   * object to the PathTraversal if necessary. The information within this object will be used
   * within queries to traverse these types of elements within the underlying data structures.
   */
  private static void harvestMultiValueTraversal(@Nonnull PathTraversal result,
      @Nonnull ElementDefinition element) {
    assert element.getMaxCardinality() != null;
    if (!element.getMaxCardinality().equals("1")) {
      result.getMultiValueTraversals().add(element);
    }
  }

  /**
   * Examines a leaf element within the tree and takes the appropriate action based upon the type,
   * such as returning a PathTraversal or further recursing into the structure in order to retrieve
   * elements that lie beyond resource references.
   */
  private static PathTraversal examineLeafElement(@Nonnull LinkedList<String> tail,
      @Nonnull ElementDefinition elementDefinition, @Nonnull PathTraversal result,
      @Nonnull String currentPath) {
    String typeCode = elementDefinition.getTypeCode();
    if (supportedPrimitiveTypes.contains(typeCode)) {
      // If the element is a primitive, stop here and return the result.
      result.setType(PRIMITIVE);
      result.setElementDefinition(elementDefinition);
      return result;
    } else if (ResourceDefinitions.supportedComplexTypes.contains(typeCode)) {
      if (tail.isEmpty()) {
        // If the tail is empty, it means that a complex type is the last component of the path. If
        // it is a reference, we tag it as such.
        if (typeCode.equals("Reference")) {
          result.setType(REFERENCE);
        } else {
          result.setType(COMPLEX);
        }
        result.setElementDefinition(elementDefinition);
        return result;
      } else {
        // If the element is complex and a path within it has been requested, recurse into
        // scanning the definition of that type.
        String newPath = String.join(".", typeCode, String.join(".", tail));
        PathTraversal nestedResult;
        try {
          nestedResult = resolvePath(newPath);
        } catch (ElementNotKnownException e) {
          throw new ElementNotKnownException("Element not known: " + result.getPath());
        }

        // Translate the keys within the multi-value traversals of the nested results. This will,
        // for example, translate `CodeableConcept.coding` into
        // `Patient.communication.language.coding` for inclusion in the final result.
        List<ElementDefinition> translatedTraversals = nestedResult.getMultiValueTraversals();
        for (ElementDefinition traversal : translatedTraversals) {
          traversal.setPath(traversal.getPath().replace(typeCode, currentPath));
        }

        // Merge the nested result into the final result and return.
        result.getMultiValueTraversals().addAll(translatedTraversals);
        result.setType(nestedResult.getType());
        result.setElementDefinition(nestedResult.getElementDefinition());
        return result;
      }
    } else {
      throw new AssertionError("Encountered unknown type: " + typeCode);
    }
  }

}