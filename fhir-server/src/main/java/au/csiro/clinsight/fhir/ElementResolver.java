package au.csiro.clinsight.fhir;

import static au.csiro.clinsight.fhir.ResourceDefinitions.checkInitialised;
import static au.csiro.clinsight.fhir.ResourceDefinitions.getElementsForType;
import static au.csiro.clinsight.fhir.ResourceDefinitions.isComplex;
import static au.csiro.clinsight.fhir.ResourceDefinitions.supportedPrimitiveTypes;
import static au.csiro.clinsight.utilities.Strings.tokenizePath;
import static au.csiro.clinsight.utilities.Strings.untokenizePath;

import au.csiro.clinsight.fhir.ResolvedElement.ResolvedElementType;
import au.csiro.clinsight.fhir.ResourceScanner.SummarisedElement;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

public class ElementResolver {

  /**
   * Returns a ResolvedElement matching a given path. Returns null if the path is not found. Only
   * works for base resources currently.
   */
  public static ResolvedElement resolveElement(@Nonnull String path)
      throws ResourceNotKnownException, ElementNotKnownException {
    checkInitialised();

    LinkedList<String> tail = tokenizePath(path);
    LinkedList<String> head = new LinkedList<>();
    head.push(tail.pop());
    String resourceOrDataType = head.peek();

    // Get the StructureDefinition that describes the parent Resource or DataType.
    assert resourceOrDataType != null;
    Map<String, SummarisedElement> elements = getElementsForType(resourceOrDataType);
    if (elements == null) {
      throw new ResourceNotKnownException("Resource or data type not known: " + resourceOrDataType);
    }

    // Initialise a new object to hold the result.
    ResolvedElement result = new ResolvedElement(path);

    // Go through the full path, starting with just the first component and adding one component
    // with each iteration of the loop. This allows us to pick up multiple cardinalities and
    // transitions into complex types.
    while (true) {
      String currentPath = untokenizePath(head);
      SummarisedElement element = elements.get(currentPath);

      // If the path cannot be found in the set of valid elements for this definition, loop back
      // around, add another component and try again. Also skip elements without a type or
      // cardinality, i.e. root elements.
      if (element == null || element.getTypeCode() == null || element.getMaxCardinality() == null) {
        if (tail.size() == 0) {
          if (element == null) {
            throw new ElementNotKnownException("Element not known: " + path);
          }
          result.setTypeCode(element.getTypeCode());
          result.setType(ResolvedElementType.RESOURCE);
          return result;
        }
        head.add(tail.pop());
        continue;
      }

      // Save information about a multi-value traversal, if this is one.
      harvestMultiValueTraversal(result, currentPath, element);

      // Examine the type of the element.
      String typeCode = element.getTypeCode();
      result.setTypeCode(typeCode);

      // If this is a `BackboneElement`, we either keep going along the path, or return it if it is
      // the last element in the path.
      if (typeCode.equals("BackboneElement")) {
        if (tail.size() == 0) {
          result.setType(ResolvedElementType.BACKBONE);
          return result;
        }
        head.add(tail.pop());
        continue;
      }
      return examineLeafElement(tail, result, currentPath, typeCode);
    }
  }

  private static void harvestMultiValueTraversal(
      @Nonnull ResolvedElement result,
      @Nonnull String currentPath,
      @Nonnull SummarisedElement element) {
    // If this element has a max cardinality of greater than one, record some information about
    // this traversal. This will be packaged up with the final result, and used to do things
    // like constructing joins when it comes time to build a query.
    assert element.getTypeCode() != null;
    assert element.getMaxCardinality() != null;
    if (!element.getMaxCardinality().equals("1")) {
      // If the current element is complex, we need to look up the children for it from the
      // element map for that type.
      if (isComplex(element.getTypeCode())) {
        SummarisedElement complexElement = getElementsForType(element.getTypeCode())
            .get(element.getTypeCode());
        MultiValueTraversal traversal = new MultiValueTraversal(currentPath,
            complexElement.getChildElements(), complexElement.getTypeCode());
        result.getMultiValueTraversals().add(traversal);
      } else {
        MultiValueTraversal traversal = new MultiValueTraversal(currentPath,
            element.getChildElements(), element.getTypeCode());
        result.getMultiValueTraversals().add(traversal);
      }
    }
  }

  private static ResolvedElement examineLeafElement(LinkedList<String> tail,
      ResolvedElement result,
      String currentPath, String typeCode) {
    if (supportedPrimitiveTypes.contains(typeCode)) {
      // If the element is a primitive, stop here and return the result.
      result.setType(ResolvedElementType.PRIMITIVE);
      return result;
    } else if (ResourceDefinitions.supportedComplexTypes.contains(typeCode)) {
      if (tail.isEmpty()) {
        // If the tail is empty, it means that a complex type is the last component of the path.
        result.setType(ResolvedElementType.COMPLEX);
        return result;
      } else {
        // If the element is complex and a path within it has been requested, recurse into
        // scanning the definition of that type.
        String newPath = String.join(".", typeCode, String.join(".", tail));
        ResolvedElement nestedResult;
        try {
          nestedResult = resolveElement(newPath);
        } catch (ElementNotKnownException e) {
          throw new ElementNotKnownException("Element not known: " + result.getPath());
        }

        // Translate the keys within the multi-value traversals of the nested results. This will,
        // for example, translate `CodeableConcept.coding` into
        // `Patient.communication.language.coding` for inclusion in the final result.
        List<MultiValueTraversal> translatedTraversals = nestedResult.getMultiValueTraversals();
        for (MultiValueTraversal traversal : translatedTraversals) {
          traversal.setPath(traversal.getPath().replace(typeCode, currentPath));
        }

        // Merge the nested result into the final result and return.
        result.getMultiValueTraversals().addAll(translatedTraversals);
        result.setTypeCode(nestedResult.getTypeCode());
        result.setType(nestedResult.getType());
        return result;
      }
    } else {
      throw new AssertionError("Encountered unknown type: " + typeCode);
    }
  }

}