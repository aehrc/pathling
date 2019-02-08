package au.csiro.clinsight.fhir;

import static au.csiro.clinsight.fhir.ResourceDefinitions.BASE_RESOURCE_URL_PREFIX;
import static au.csiro.clinsight.fhir.ResourceDefinitions.supportedComplexTypes;

import au.csiro.clinsight.utilities.Strings;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
      Set<String> elementChildren = findElementChildren(elementPath, elements);
      assert elementChildren != null;
      String maxCardinality = (typeRefComponents != null && !typeRefComponents.isEmpty())
          ? element.getMax()
          : null;

      // Check for elements that have multiple types - these will generate multiple entries in the
      // resulting map.
      if (elementPath.matches(".*\\[x]$")) {
        List<String> types = typeRefComponents == null
            ? new ArrayList<>()
            : typeRefComponents.stream().map(TypeRefComponent::getCode)
                .collect(Collectors.toList());
        for (String type : types) {
          assert type != null : "Encountered element type with no code";
          String transformedPath = elementPath
              .replaceAll("\\[x]", Strings.capitalize(type));
          SummarisedElement resolvedElement = new SummarisedElement(
              transformedPath);
          resolvedElement.setTypeCode(type);
          resolvedElement.setChildElements(elementChildren);
          resolvedElement.setMaxCardinality(maxCardinality);
          result.put(transformedPath, resolvedElement);
        }
      } else {
        String typeCode = typeRefComponents == null || typeRefComponents.isEmpty()
            ? null
            : typeRefComponents.get(0).getCode();
        SummarisedElement resolvedElement = new SummarisedElement(
            elementPath);
        resolvedElement.setTypeCode(typeCode);
        resolvedElement.setChildElements(elementChildren);
        resolvedElement.setMaxCardinality(maxCardinality);
        result.put(elementPath, resolvedElement);
      }
    }

    return result;
  }

  private static Set<String> findElementChildren(@Nonnull String elementPath,
      @Nonnull List<ElementDefinition> elements) {
    Set<String> children = elements.stream()
        .filter(e -> e.getPath().matches(elementPath + "\\.[a-zA-Z\\[\\]]+$"))
        .map(e -> {
          String[] pathComponents = e.getPath().split("\\.");
          if (pathComponents.length == 0) {
            throw new AssertionError("Encountered child path with no components");
          }
          return pathComponents[pathComponents.length - 1];
        })
        .collect(Collectors.toSet());
    // All instances of BackboneElement have an `id` element.
    children.add("id");
    // `extension` and `modifierExtension` are not relevant to the implementation (currently).
    children.remove("extension");
    children.remove("modifierExtension");
    return children;
  }

  static class SummarisedElement {

    private String path;
    private Set<String> childElements;
    @Nullable
    private String typeCode;
    @Nullable
    private String maxCardinality;

    SummarisedElement(String path) {
      this.path = path;
    }

    String getPath() {
      return path;
    }

    void setPath(String path) {
      this.path = path;
    }

    @Nullable
    String getTypeCode() {
      return typeCode;
    }

    void setTypeCode(@Nullable String typeCode) {
      this.typeCode = typeCode;
    }

    @Nullable
    String getMaxCardinality() {
      return maxCardinality;
    }

    void setMaxCardinality(@Nullable String maxCardinality) {
      this.maxCardinality = maxCardinality;
    }

    Set<String> getChildElements() {
      return childElements;
    }

    void setChildElements(Set<String> childElements) {
      this.childElements = childElements;
    }

  }
}