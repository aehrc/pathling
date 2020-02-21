/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright © 2018-2020, Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230. Licensed
 * under the CSIRO Open Source Software Licence Agreement.
 */

package au.csiro.pathling.encoders.r4.codes;

import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.codes.Mapping;
import au.csiro.pathling.encoders.codes.UrlAndVersion;
import au.csiro.pathling.encoders.codes.base.AbstractConceptMaps;
import au.csiro.pathling.encoders.codes.broadcast.BroadcastableConceptMap;
import au.csiro.pathling.encoders.codes.broadcast.BroadcastableMappings;
import ca.uhn.fhir.context.FhirVersionEnum;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.ConceptMap.ConceptMapGroupComponent;
import org.hl7.fhir.r4.model.ConceptMap.ConceptMapGroupUnmappedMode;
import org.hl7.fhir.r4.model.ConceptMap.SourceElementComponent;
import org.hl7.fhir.r4.model.ConceptMap.TargetElementComponent;
import org.hl7.fhir.r4.model.UriType;


/**
 * An immutable collection of FHIR ConceptMaps. This class is used to import concept map content,
 * explore it, and persist it to a database.
 */
public class ConceptMaps extends AbstractConceptMaps<ConceptMap, ConceptMaps> {

  private static final Encoder<ConceptMap> CONCEPT_MAP_ENCODER = FhirEncoders.forR4()
      .getOrCreate()
      .of(ConceptMap.class);

  /**
   * Returns the encoder for mappings.
   *
   * @return an encoder for mappings.
   */
  public static Encoder<Mapping> getMappingEncoder() {

    return MAPPING_ENCODER;
  }

  /**
   * Returns the encoder for concept maps.
   *
   * @return an encoder for concept maps.
   */
  public static Encoder<ConceptMap> getConceptMapEncoder() {

    return CONCEPT_MAP_ENCODER;
  }


  protected ConceptMaps(SparkSession spark,
      Dataset<UrlAndVersion> members,
      Dataset<ConceptMap> conceptMaps,
      Dataset<Mapping> mappings) {

    super(spark, FhirVersionEnum.R4, members, conceptMaps, mappings, CONCEPT_MAP_ENCODER);
  }

  protected ConceptMaps newInstance(SparkSession spark,
      Dataset<UrlAndVersion> members,
      Dataset<ConceptMap> conceptMaps,
      Dataset<Mapping> mappings) {

    return new ConceptMaps(spark, members, conceptMaps, mappings);
  }

  /**
   * Returns the collection of concept maps from the default database and tables.
   *
   * @param spark the spark session
   * @return a ConceptMaps instance.
   */
  public static ConceptMaps getDefault(SparkSession spark) {

    return getFromDatabase(spark, MAPPING_DATABASE);
  }

  /**
   * Returns the collection of concept maps from the tables in the given database.
   *
   * @param spark the spark session
   * @param databaseName name of the database containing the conceptmaps and mappings tables.
   * @return a ConceptMaps instance.
   */
  public static ConceptMaps getFromDatabase(SparkSession spark, String databaseName) {

    Dataset<Mapping> mappings = spark.sql(
        "SELECT * FROM " + databaseName + "." + MAPPING_TABLE).as(MAPPING_ENCODER);

    Dataset<ConceptMap> conceptMaps = spark
        .sql("SELECT * FROM " + databaseName + "." + CONCEPT_MAP_TABLE)
        .as(CONCEPT_MAP_ENCODER);

    return new ConceptMaps(spark,
        spark.emptyDataset(URL_AND_VERSION_ENCODER),
        conceptMaps,
        mappings);
  }

  /**
   * Returns an empty ConceptMaps instance.
   *
   * @param spark the spark session
   * @return an empty ConceptMaps instance.
   */
  public static ConceptMaps getEmpty(SparkSession spark) {

    Dataset<ConceptMap> emptyConceptMaps = spark.emptyDataset(CONCEPT_MAP_ENCODER)
        .withColumn("timestamp", lit(null).cast("timestamp"))
        .as(CONCEPT_MAP_ENCODER);

    return new ConceptMaps(spark,
        spark.emptyDataset(URL_AND_VERSION_ENCODER),
        emptyConceptMaps,
        spark.emptyDataset(MAPPING_ENCODER));
  }

  @Override
  protected void addToConceptMap(ConceptMap map, Dataset<Mapping> mappings) {

    // Sort the items so they are grouped together optimally, and so
    // we consistently produce the same ordering, therefore making
    // inspection and comparison of the concept maps easier.
    List<Mapping> sortedMappings = mappings.sort("sourceSystem",
        "targetSystem",
        "sourceValue",
        "targetValue")
        .collectAsList();

    ConceptMapGroupComponent currentGroup = null;
    SourceElementComponent element = null;

    // Workaround for the decoder producing an immutable array by
    // replacing it with a mutable one.
    map.setGroup(new ArrayList<>(map.getGroup()));
    for (Mapping mapping : sortedMappings) {

      // Add a new group if we don't match the previous one.
      if (currentGroup == null
          || !mapping.getSourceSystem().equals(currentGroup.getSource())
          || !mapping.getTargetSystem().equals(currentGroup.getTarget())) {

        currentGroup = null;

        // Find a matching group.
        for (ConceptMapGroupComponent candidate : map.getGroup()) {

          if (mapping.getSourceSystem().equals(candidate.getSource())
              && mapping.getTargetSystem().equals(candidate.getTarget())) {

            currentGroup = candidate;

            // Workaround for the decoder producing an immutable array by
            // replacing it with a mutable one.
            currentGroup.setElement(new ArrayList<>(currentGroup.getElement()));
            break;
          }
        }

        // No matching group found, so add it.
        if (currentGroup == null) {
          currentGroup = map.addGroup();

          currentGroup.setSource(mapping.getSourceSystem());
          currentGroup.setTarget(mapping.getTargetSystem());

          // Ensure a new element is created for the newly created group.
          element = null;
        }
      }

      // There is an element for each distinct source value in the map,
      // so add one if it does not match the previous.
      if (element == null
          || !mapping.getSourceValue().equals(element.getCode())) {

        element = currentGroup.addElement();
        element.setCode(mapping.getSourceValue());
      }

      element.addTarget().setCode(mapping.getTargetValue());
    }
  }

  /**
   * Given a concept map, returns a list of mapping records it contains.
   *
   * @param map a concept map
   * @return a list of Mapping records.
   */
  public static List<Mapping> expandMappings(ConceptMap map) {

    List<Mapping> mappings = new ArrayList<>();

    expandMappingsIterator(map).forEachRemaining(mappings::add);

    return mappings;
  }

  private static Iterator<Mapping> expandMappingsIterator(ConceptMap map) {

    List<Mapping> mappings = new ArrayList<>();

    for (ConceptMapGroupComponent group : map.getGroup()) {

      for (SourceElementComponent element : group.getElement()) {

        for (TargetElementComponent target : element.getTarget()) {

          Mapping mapping = new Mapping();

          mapping.setConceptMapUri(map.getUrl());
          mapping.setConceptMapVersion(map.getVersion());

          try {
            String sourceValue = map.getSource() instanceof UriType
                ? map.getSourceUriType().getValue()
                : map.getSourceUriType().getValue();

            mapping.setSourceValueSet(sourceValue);

            String targetValue = map.getTarget() instanceof UriType
                ? map.getTargetUriType().getValue()
                : map.getTargetUriType().getValue();

            mapping.setTargetValueSet(targetValue);

          } catch (FHIRException fhirException) {

            // This should not happen because we check the types,
            // but rethrow to avoid any possibility of swallowing
            // an exception.
            throw new RuntimeException(fhirException);
          }

          mapping.setSourceSystem(group.getSource());
          mapping.setSourceValue(element.getCode());

          mapping.setTargetSystem(group.getTarget());
          mapping.setTargetValue(target.getCode());

          if (target.getEquivalence() != null) {
            mapping.setEquivalence(target.getEquivalence().toCode());
          }

          mappings.add(mapping);
        }
      }
    }

    return mappings.iterator();
  }

  @Override
  public ConceptMaps withConceptMaps(Dataset<ConceptMap> conceptMaps) {

    Dataset<UrlAndVersion> newMembers = getUrlAndVersions(conceptMaps);

    if (hasDuplicateUrlAndVersions(newMembers) || conceptMaps.count() != newMembers.count()) {

      throw new IllegalArgumentException(
          "Cannot add concept maps having duplicate conceptMapUri and conceptMapVersion");
    }

    // Remove the concept contents for persistence. This is most easily done in the ConceptMap
    // object by setting the group to an empty list.
    Dataset<ConceptMap> withoutConcepts = conceptMaps
        .map((MapFunction<ConceptMap, ConceptMap>) conceptMap -> {

          // Remove the elements rather than the groups to preserved the
          // "unmapped" structure in a group that can refer to other
          // concept maps.
          ConceptMap withoutElements = conceptMap.copy();

          List<ConceptMapGroupComponent> updatedGroups = new ArrayList<>();

          for (ConceptMapGroupComponent group : withoutElements.getGroup()) {

            group.setElement(new ArrayList<>());
            updatedGroups.add(group);
          }

          withoutElements.setGroup(updatedGroups);

          return withoutElements;
        }, CONCEPT_MAP_ENCODER);

    Dataset<Mapping> newMappings = conceptMaps.flatMap(ConceptMaps::expandMappingsIterator,
        MAPPING_ENCODER);

    return withConceptMaps(withoutConcepts, newMappings);
  }

  @Override
  public Broadcast<BroadcastableMappings> broadcast(Map<String, String> conceptMapUriToVersion) {

    List<ConceptMap> mapsList = getMaps().collectAsList();

    Map<String, ConceptMap> mapsToLoad = mapsList
        .stream()
        .filter(conceptMap ->
            conceptMap.getVersion().equals(conceptMapUriToVersion.get(conceptMap.getUrl())))
        .collect(Collectors.toMap(ConceptMap::getUrl, Function.identity()));

    // Expand the concept maps to load and sort them so dependencies are before
    // their dependents in the list.
    List<String> sortedMapsToLoad = sortMapsToLoad(conceptMapUriToVersion.keySet(), mapsToLoad);

    // Since this is used to map from one system to another, we use only targets
    // that don't introduce inaccurate meanings. (For instance, we can't map
    // general condition code to a more specific type, since that is not
    // representative of the source data.)
    Dataset<Mapping> mappings = getMappings(conceptMapUriToVersion)
        .filter("equivalence in ('equivalent', 'equals', 'wider', 'subsumes')");

    // Group mappings by their concept map URI
    Map<String, List<Mapping>> groupedMappings = mappings
        .collectAsList()
        .stream()
        .collect(Collectors.groupingBy(Mapping::getConceptMapUri));

    Map<String, BroadcastableConceptMap> broadcastableMaps = new HashMap<>();

    for (String conceptMapUri : sortedMapsToLoad) {

      ConceptMap map = mapsToLoad.get(conceptMapUri);

      Set<String> children = getMapChildren(map);

      List<BroadcastableConceptMap> childMaps = children.stream()
          .map(child -> broadcastableMaps.get(child))
          .collect(Collectors.toList());

      BroadcastableConceptMap broadcastableConceptMap = new BroadcastableConceptMap(conceptMapUri,
          groupedMappings.getOrDefault(conceptMapUri, Collections.emptyList()),
          childMaps);

      broadcastableMaps.put(conceptMapUri, broadcastableConceptMap);
    }

    JavaSparkContext ctx = new JavaSparkContext(getMaps()
        .sparkSession()
        .sparkContext());

    return ctx.broadcast(new BroadcastableMappings(broadcastableMaps));
  }

  /**
   * Returns a list of concept maps in the order they need to be loaded.
   */
  private static List<String> sortMapsToLoad(Set<String> mapsToLoad,
      Map<String, ConceptMap> allMaps) {

    Deque<String> pendingMaps = new ArrayDeque<>(mapsToLoad);

    List<String> loadOrder = new ArrayList<>();

    Set<String> loadedMaps = new HashSet<>();

    while (!pendingMaps.isEmpty()) {

      String nextMap = pendingMaps.peek();

      // If the map has already been loaded,
      // remove it and continue.
      if (loadedMaps.contains(nextMap)) {

        pendingMaps.pop();
        continue;
      }

      ConceptMap mapToLoad = allMaps.get(nextMap);

      if (mapToLoad == null) {
        throw new IllegalStateException("Concept map " + nextMap + " "
            + " is referenced but not in the collection of concept maps.");
      }

      // Get the set of children we need to load before the pending map.
      Set<String> childrenToLoad = getMapChildren(mapToLoad);

      childrenToLoad.removeAll(loadedMaps);

      // If the pending map has no children to load, we can
      // add it to our load order.
      if (childrenToLoad.isEmpty()) {

        loadedMaps.add(nextMap);
        loadOrder.add(nextMap);
        pendingMaps.pop();

      } else {

        // The pending map has children, so we need to load them first.
        for (String child : childrenToLoad) {
          pendingMaps.push(child);
        }
      }
    }

    return loadOrder;
  }

  /**
   * Returns the children of a given concept map to which the parent may delegate mapped items.
   */
  private static Set<String> getMapChildren(ConceptMap map) {

    return map.getGroup()
        .stream()
        .filter(group -> group.getUnmapped() != null
            && group.getUnmapped().getMode() == ConceptMapGroupUnmappedMode.OTHERMAP)
        .map(group -> group.getUnmapped().getUrl())
        .collect(Collectors.toSet());
  }
}
