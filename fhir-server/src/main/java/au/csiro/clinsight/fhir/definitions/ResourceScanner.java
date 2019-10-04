/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir.definitions;

import static au.csiro.clinsight.fhir.definitions.ResourceDefinitions.BASE_RESOURCE_URL_PREFIX;
import static au.csiro.clinsight.utilities.Strings.tokenizePath;

import au.csiro.clinsight.utilities.Strings;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.ElementDefinition.TypeRefComponent;
import org.hl7.fhir.r4.model.Enumerations.FHIRDefinedType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.StructureDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class knows how to retrieve, summarise and index a set of StructureDefinitions from a
 * terminology server.
 *
 * @author John Grimes
 */
class ResourceScanner {

  private static final Logger logger = LoggerFactory.getLogger(ResourceScanner.class);

  /**
   * Check that all StructureDefinitions within a collection have a snapshot component. Accepting
   * StructureDefinitions without a snapshot is problematic, as we would have to traverse the
   * inheritance tree to understand the definition of all elements.
   */
  static void validateDefinitions(@Nonnull Collection<StructureDefinition> definitions) {
    definitions.forEach(definition -> {
      if (definition.getSnapshot() == null || definition.getSnapshot().isEmpty()) {
        throw new AssertionError(
            "Encountered StructureDefinition with empty snapshot: " + definition.getUrl());
      }
    });
  }

  @Nonnull
  static Map<ResourceType, Map<String, ElementDefinition>> summariseResourceDefinitions(
      @Nonnull Collection<StructureDefinition> definitions) {
    Map<ResourceType, Map<String, ElementDefinition>> result = new EnumMap<>(ResourceType.class);
    for (StructureDefinition definition : definitions) {
      Map<String, ElementDefinition> summarisedElements = summariseElements(
          definition.getSnapshot().getElement());
      result.put(ResourceType.fromCode(definition.getType()), summarisedElements);
    }
    return result;
  }

  @Nonnull
  static Map<FHIRDefinedType, Map<String, ElementDefinition>> summariseComplexTypeDefinitions(
      @Nonnull Collection<StructureDefinition> definitions) {
    Map<FHIRDefinedType, Map<String, ElementDefinition>> result = new EnumMap<>(
        FHIRDefinedType.class);
    for (StructureDefinition definition : definitions) {
      Map<String, ElementDefinition> summarisedElements = summariseElements(
          definition.getSnapshot().getElement());
      result.put(FHIRDefinedType.fromCode(definition.getType()), summarisedElements);
    }
    return result;
  }

  /**
   * Converts a list of ElementDefinitions into a map between element path and a summary of its
   * definition.
   */
  @Nonnull
  private static Map<String, ElementDefinition> summariseElements(
      @Nonnull List<org.hl7.fhir.r4.model.ElementDefinition> elements) {
    Map<String, ElementDefinition> result = new HashMap<>();

    for (org.hl7.fhir.r4.model.ElementDefinition element : elements) {
      List<TypeRefComponent> typeRefComponents = element.getType();
      String elementPath = element.getPath();
      assert typeRefComponents != null : "Encountered element with no type";
      assert elementPath != null : "Encountered element with no path";

      Set<String> elementChildren = findElementChildren(elementPath, elements);

      String maxCardinality = !typeRefComponents.isEmpty()
          ? element.getMax()
          : null;

      List<String> types = typeRefComponents.stream().map(TypeRefComponent::getCode)
          .collect(Collectors.toList());

      // Check for elements that have multiple types - these will generate multiple entries in the
      // resulting map.
      if (elementPath.matches(".*\\[x]$")) {
        for (String type : types) {
          assert type != null : "Encountered element type with no code";
          String transformedPath = elementPath
              .replaceAll("\\[x]", Strings.capitalize(type));
          ElementDefinition elementDefinition = new ElementDefinition();
          elementDefinition.setPath(transformedPath);
          elementDefinition.setFhirType(FHIRDefinedType.fromCode(type));
          elementDefinition.getChildElements().addAll(elementChildren);
          elementDefinition.setMaxCardinality(maxCardinality);
          result.put(transformedPath, elementDefinition);
        }
      } else {
        String type;
        if (typeRefComponents.isEmpty()) {
          // If the element is at the root and does not have a type within the StructureDefinition,
          // give it a type equal to its own name.
          LinkedList<String> pathTokens = tokenizePath(elementPath);
          type = pathTokens.size() == 1
              ? pathTokens.getFirst()
              : null;
        } else {
          type = typeRefComponents.get(0).getCode();
        }

        if (type == null) {
          // TODO: Handle element definitions with `contentReference`s. At the moment we just drop
          //       them.
          continue;
        }
        ElementDefinition elementDefinition = new ElementDefinition();
        elementDefinition.setPath(elementPath);
        elementDefinition.setFhirType(FHIRDefinedType.fromCode(type));
        elementDefinition.getChildElements().addAll(elementChildren);
        elementDefinition.setMaxCardinality(maxCardinality);

        if (type.equals("Reference")) {
          // If the element is a Reference, add a list of all the valid types that can be
          // referenced. This will be used later when resolving references.
          for (TypeRefComponent typeRefComponent : typeRefComponents) {
            List<ResourceType> referenceTypes = typeRefComponent.getTargetProfile().stream()
                .map(canonicalType -> {
                  assert canonicalType.getValue().startsWith(
                      BASE_RESOURCE_URL_PREFIX) : "Encountered reference target which is not base resource";
                  String resourceType = canonicalType.getValue()
                      .replaceFirst(BASE_RESOURCE_URL_PREFIX, "");
                  return ResourceType.fromCode(resourceType);
                }).collect(Collectors.toList());
            elementDefinition.getReferenceTypes().addAll(referenceTypes);
          }
        }
        result.put(elementPath, elementDefinition);
      }
    }

    return result;
  }

  /**
   * This method knows how to find the children of a given element within a list of
   * ElementDefinitions. This is needed because StructureDefinitions are not hierarchical in their
   * structure, they rely on paths to convey the structure.
   */
  private static Set<String> findElementChildren(@Nonnull String elementPath,
      @Nonnull List<org.hl7.fhir.r4.model.ElementDefinition> elements) {
    Set<String> children = elements.stream()
        .filter(e -> e.getPath().matches(elementPath + "\\.[a-zA-Z\\[\\]]+$"))
        .map(e -> {
          LinkedList<String> pathComponents = tokenizePath(e.getPath());
          if (pathComponents.size() == 0) {
            throw new AssertionError("Encountered child path with no components");
          }
          return pathComponents.get(pathComponents.size() - 1);
        })
        .collect(Collectors.toSet());
    // `extension` and `modifierExtension` are not relevant to the implementation (currently).
    children.remove("extension");
    children.remove("modifierExtension");
    return children;
  }

}