/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright © Australian e-Health Research Centre,
 * CSIRO. All rights reserved.
 */

package au.csiro.pathling.bunsen.codes.broadcast;

import au.csiro.pathling.bunsen.codes.Mapping;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

/**
 * A broadcastable concept map available in Spark operations or user-defined functions. This maps to
 * an instance of a single FHIR concept map, but contains the fully exploded codes and is optimized
 * to be broadcast across a Spark cluster.
 *
 * <p>
 * Most users will create this via the {@link BroadcastableMappings} class.
 * </p>
 */
public class BroadcastableConceptMap implements Serializable {

  private String conceptMapUri;

  /**
   * Nested map of system and code values for the map.
   */
  private Map<String, Map<String, List<CodeValue>>> targetValueMap = new HashMap<>();

  private final List<BroadcastableConceptMap> delegates;

  /**
   * Constructs the broadcastable concept map.
   *
   * @param conceptMapUri the URI of the map
   * @param mappings the mappings to include in the broadcast
   * @param delegates The maps to which this concept map should delegate if it cannot resolve its
   * code values.
   */
  public BroadcastableConceptMap(String conceptMapUri,
      List<Mapping> mappings,
      List<BroadcastableConceptMap> delegates) {

    this.conceptMapUri = conceptMapUri;

    Map<String, List<Mapping>> groupedBySystem =
        mappings.stream()
            .collect(Collectors.groupingBy(Mapping::getSourceSystem));

    groupedBySystem.entrySet().forEach(e ->
        targetValueMap.put(e.getKey(),
            sourceToTargetValueMap(e.getValue())));

    this.delegates = delegates;
  }

  /**
   * Constructs the broadcastable concept map.
   *
   * @param conceptMapUri the URI of the map
   * @param mappings the mappings to include in the broadcast
   */
  BroadcastableConceptMap(String conceptMapUri, List<Mapping> mappings) {

    this(conceptMapUri, mappings, Collections.emptyList());
  }

  private static Map<String, List<CodeValue>> sourceToTargetValueMap(List<Mapping> mappings) {

    return mappings.stream()
        .collect(Collectors.groupingBy(Mapping::getSourceValue))
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Entry::getKey,
            entry -> entry.getValue()
                .stream()
                .map(mapping -> new CodeValue(mapping.getTargetSystem(), mapping.getTargetValue()))
                .collect(Collectors.toList())));
  }

  /**
   * Returns the URI for the concept map.
   *
   * @return the URI for the concept map.
   */
  public String getConceptMapUri() {

    return conceptMapUri;
  }

  /**
   * Returns the target list of code values, or an empty list.
   *
   * @param sourceSystem the source code system
   * @param sourceValue the source code value
   * @return the list of target code values, or an empty list.
   */
  public List<CodeValue> getTarget(String sourceSystem, String sourceValue) {

    List<CodeValue> results = targetValueMap
        .getOrDefault(sourceSystem, Collections.emptyMap())
        .getOrDefault(sourceValue, Collections.emptyList());

    if (!results.isEmpty()) {

      // We have a match, so use it.
      return results;

    } else {

      for (BroadcastableConceptMap delegate : delegates) {

        List<CodeValue> delegateResults = delegate.getTarget(sourceSystem, sourceValue);

        if (!delegateResults.isEmpty()) {

          return delegateResults;
        }
      }

      // No match was found directly or indirectly, so return an empty list.
      return Collections.emptyList();
    }
  }

  /**
   * A specific code instance, identified by system and value, that has been broadcast.
   */
  public static class CodeValue implements Serializable {

    private final String system;

    private final String value;

    CodeValue(String system, String value) {
      this.system = system;
      this.value = value;
    }

    /**
     * Returns the code system.
     *
     * @return the code system.
     */
    public String getSystem() {
      return system;
    }

    /**
     * Returns the code value.
     *
     * @return the code value.
     */
    public String getValue() {
      return value;
    }
  }
}