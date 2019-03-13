package au.csiro.clinsight.fhir;

import static au.csiro.clinsight.fhir.ResourceDefinitions.BASE_RESOURCE_URL_PREFIX;
import static au.csiro.clinsight.fhir.ResourceDefinitions.supportedComplexTypes;
import static au.csiro.clinsight.utilities.Strings.tokenizePath;

import au.csiro.clinsight.utilities.Strings;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.hl7.fhir.dstu3.model.ElementDefinition;
import org.hl7.fhir.dstu3.model.ElementDefinition.TypeRefComponent;
import org.hl7.fhir.dstu3.model.StructureDefinition;
import org.hl7.fhir.dstu3.model.StructureDefinition.StructureDefinitionKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ResourceScanner {

  private static final Logger logger = LoggerFactory.getLogger(ResourceScanner.class);

  static Map<String, StructureDefinition> retrieveResourceDefinitions(
      @Nonnull List<StructureDefinition> structureDefinitions,
      @Nonnull Function<StructureDefinition, StructureDefinition> fetchResourceWithId) {
    return structureDefinitions.stream()
        .filter(sd -> sd.getKind() == StructureDefinitionKind.RESOURCE)
        .peek(sd -> logger.info("Retrieving resource StructureDefinition: " + sd.getUrl()))
        .collect(Collectors.toMap(StructureDefinition::getUrl, fetchResourceWithId));
  }

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
      logger.warn("Number of complex type complexTypes retrieved does not equal number of "
          + "supported complex types, missing: " + String.join(", ", difference));
    }

    return definitions;
  }

  static void validateDefinitions(@Nonnull Collection<StructureDefinition> definitions) {
    definitions.forEach(definition -> {
      if (definition.getSnapshot() == null || definition.getSnapshot().isEmpty()) {
        throw new AssertionError(
            "Encountered StructureDefinition with empty snapshot: " + definition.getUrl());
      }
    });
  }

  static Map<String, Map<String, SummarisedElement>> summariseDefinitions(
      @Nonnull Collection<StructureDefinition> definitions) {
    Map<String, Map<String, SummarisedElement>> result = new HashMap<>();
    for (StructureDefinition definition : definitions) {
      Map<String, SummarisedElement> summarisedElements = summariseElements(
          definition.getSnapshot().getElement());
      result.put(definition.getUrl(), summarisedElements);
    }
    return result;
  }

  private static Map<String, SummarisedElement> summariseElements(
      @Nonnull List<ElementDefinition> elements) {
    Map<String, SummarisedElement> result = new HashMap<>();

    for (ElementDefinition element : elements) {
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
          SummarisedElement summarisedElement = new SummarisedElement(
              transformedPath, type);
          summarisedElement.getChildElements().addAll(elementChildren);
          summarisedElement.setMaxCardinality(maxCardinality);
          result.put(transformedPath, summarisedElement);
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
        SummarisedElement summarisedElement = new SummarisedElement(elementPath, typeCode);
        summarisedElement.getChildElements().addAll(elementChildren);
        summarisedElement.setMaxCardinality(maxCardinality);

        if (typeCode.equals("Reference")) {
          // If the element is a Reference, add a list of all the valid types that can be
          // referenced. This will be used later when resolving references.
          @SuppressWarnings("ConstantConditions") List<String> referenceTypes = typeRefComponents
              .stream()
              .map(TypeRefComponent::getTargetProfile)
              .collect(Collectors.toList());
          summarisedElement.getReferenceTypes().addAll(referenceTypes);
        }
        result.put(elementPath, summarisedElement);
      }
    }

    return result;
  }

  private static List<String> findElementChildren(@Nonnull String elementPath,
      @Nonnull List<ElementDefinition> elements) {
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