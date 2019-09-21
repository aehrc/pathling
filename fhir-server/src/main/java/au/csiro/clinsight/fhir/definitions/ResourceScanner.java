/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.fhir.definitions;

import static au.csiro.clinsight.fhir.definitions.ResourceDefinitions.BASE_RESOURCE_URL_PREFIX;
import static au.csiro.clinsight.fhir.definitions.ResourceDefinitions.supportedComplexTypes;
import static au.csiro.clinsight.utilities.Strings.tokenizePath;

import au.csiro.clinsight.utilities.Strings;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.hl7.fhir.r4.model.ElementDefinition.TypeRefComponent;
import org.hl7.fhir.r4.model.StructureDefinition;
import org.hl7.fhir.r4.model.StructureDefinition.StructureDefinitionKind;
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
   * Retrieve a map of URLs to full StructureDefinitions, given a list of summarised
   * StructureDefinitions (for example, from a search result).
   *
   * Requires a lambda which describes how to retrieve a StructureDefinition, based on its ID.
   */
  @Nonnull
  static Map<String, StructureDefinition> retrieveResourceDefinitions(
      @Nonnull List<StructureDefinition> structureDefinitions,
      @Nonnull Function<StructureDefinition, StructureDefinition> fetchResourceWithId) {
    return structureDefinitions.stream()
        .filter(sd -> sd.getKind() == StructureDefinition.StructureDefinitionKind.RESOURCE)
        .peek(sd -> logger.info("Retrieving resource StructureDefinition: " + sd.getUrl()))
        .collect(Collectors.toMap(StructureDefinition::getUrl, fetchResourceWithId));
  }

  /**
   * Retrieve a map of URLs to full StructureDefinitions, given a list of summarised
   * StructureDefinitions (for example, from a search result).
   *
   * Requires a lambda which describes how to retrieve a StructureDefinition, based on its ID.
   */
  @Nonnull
  static Map<String, StructureDefinition> retrieveComplexTypeDefinitions(
      @Nonnull List<StructureDefinition> structureDefinitions,
      @Nonnull Function<StructureDefinition, StructureDefinition> fetchResourceWithId) {
    // Create a filter that matches only base complex types.
    Supplier<Predicate<StructureDefinition>> complexTypeFilter = () -> sd -> {
      // Check that the StructureDefinition is a complex type, and that the URL matches the base
      // FHIR prefix.
      if (sd.getKind() != StructureDefinitionKind.COMPLEXTYPE ||
          !sd.getUrl().matches("^http://hl7.org/fhir/StructureDefinition/.+")) {
        return false;
      }
      // Check that the last component of the URL matches one of our supported complex types.
      String[] components = sd.getUrl().split("/");
      if (components.length == 0) {
        return false;
      } else {
        return supportedComplexTypes.contains(components[components.length - 1]);
      }
    };

    // Retrieve the StructureDefinitions that match the complex type filter.
    Map<String, StructureDefinition> definitions = structureDefinitions.stream()
        .filter(complexTypeFilter.get())
        .peek(sd -> logger.info("Retrieving complex type StructureDefinition: " + sd.getUrl()))
        .collect(Collectors.toMap(StructureDefinition::getUrl, fetchResourceWithId));

    // Do a sanity check that we were able to retrieve all of the supported types.
    if (definitions.size() != supportedComplexTypes.size()) {
      Set<String> difference = supportedComplexTypes.stream()
          .map(t -> BASE_RESOURCE_URL_PREFIX + t).collect(Collectors.toSet());
      difference.removeAll(definitions.keySet());
      logger.warn("Number of complex types retrieved does not equal number of "
          + "supported complex types, missing: " + String.join(", ", difference));
    }

    return definitions;
  }

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

  /**
   * Returns a map of URLs to maps of element names to summaries of those elements. This provides an
   * object which is easier and more efficient to interrogate about element information than the
   * HAPI StructureDefinition class.
   */
  @Nonnull
  static Map<String, Map<String, ElementDefinition>> summariseDefinitions(
      @Nonnull Collection<StructureDefinition> definitions) {
    Map<String, Map<String, ElementDefinition>> result = new HashMap<>();
    for (StructureDefinition definition : definitions) {
      Map<String, ElementDefinition> summarisedElements = summariseElements(
          definition.getSnapshot().getElement());
      result.put(definition.getUrl(), summarisedElements);
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
      assert elementPath != null : "Encountered element with no path";

      List<String> elementChildren = findElementChildren(elementPath, elements);
      assert elementChildren != null;

      String maxCardinality = (typeRefComponents != null && !typeRefComponents.isEmpty())
          ? element.getMax()
          : null;

      List<String> types = typeRefComponents == null
          ? new ArrayList<>()
          : typeRefComponents.stream().map(TypeRefComponent::getCode)
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
          elementDefinition.setTypeCode(type);
          elementDefinition.getChildElements().addAll(elementChildren);
          elementDefinition.setMaxCardinality(maxCardinality);
          result.put(transformedPath, elementDefinition);
        }
      } else {
        String typeCode;
        if (typeRefComponents == null || typeRefComponents.isEmpty()) {
          // If the element is at the root and does not have a type within the StructureDefinition,
          // give it a type equal to its own name.
          LinkedList<String> pathTokens = tokenizePath(elementPath);
          typeCode = pathTokens.size() == 1
              ? pathTokens.getFirst()
              : null;
        } else {
          typeCode = typeRefComponents.get(0).getCode();
        }

        if (typeCode == null) {
          // TODO: Handle element definitions with `contentReference`s. At the moment we just drop
          //       them.
          continue;
        }
        ElementDefinition elementDefinition = new ElementDefinition();
        elementDefinition.setPath(elementPath);
        elementDefinition.setTypeCode(typeCode);
        elementDefinition.getChildElements().addAll(elementChildren);
        elementDefinition.setMaxCardinality(maxCardinality);

        if (typeCode.equals("Reference")) {
          // If the element is a Reference, add a list of all the valid types that can be
          // referenced. This will be used later when resolving references.
          @SuppressWarnings("ConstantConditions") List<String> referenceTypes = typeRefComponents
              .stream()
              .map(typeRefComponent -> typeRefComponent.getTargetProfile().toString())
              .collect(Collectors.toList());
          elementDefinition.getReferenceTypes().addAll(referenceTypes);
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
  private static List<String> findElementChildren(@Nonnull String elementPath,
      @Nonnull List<org.hl7.fhir.r4.model.ElementDefinition> elements) {
    List<String> children = elements.stream()
        .filter(e -> e.getPath().matches(elementPath + "\\.[a-zA-Z\\[\\]]+$"))
        .map(e -> {
          LinkedList<String> pathComponents = tokenizePath(e.getPath());
          if (pathComponents.size() == 0) {
            throw new AssertionError("Encountered child path with no components");
          }
          return pathComponents.get(pathComponents.size() - 1);
        })
        .collect(Collectors.toList());
    // All instances of BackboneElement have an `id` element.
    if (!children.contains("id")) {
      children.add("id");
    }
    // `extension` and `modifierExtension` are not relevant to the implementation (currently).
    children.remove("extension");
    children.remove("modifierExtension");
    return children;
  }

}