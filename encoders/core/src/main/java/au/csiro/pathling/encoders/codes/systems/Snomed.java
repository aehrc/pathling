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

package au.csiro.pathling.encoders.codes.systems;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

import au.csiro.pathling.encoders.codes.Hierarchies;
import au.csiro.pathling.encoders.codes.Hierarchies.HierarchicalElement;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Support for SNOMED CT.
 */
public class Snomed {

  /**
   * Hierarchy URI used for SNOMED is-a relationships.
   */
  public static final String SNOMED_HIERARCHY_URI = Hierarchies.HIERARCHY_URI_PREFIX + "snomed";

  /**
   * SNOMED code system URI.
   */
  public static final String SNOMED_CODE_SYSTEM_URI = "http://snomed.info/sct";

  /**
   * The relationship ID used to identify "is a" relationships in SNOMED.
   */
  private static final String SNOMED_ISA_RELATIONSHIP_ID = "116680003";

  /**
   * Reads a Snomed relationship file and converts it to a {@link HierarchicalElement} dataset.
   *
   * @param spark the Spark session
   * @param snomedRelationshipPath path to the SNOMED relationship file
   * @return a dataset of{@link HierarchicalElement} representing the hierarchical relationship.
   */
  public static Dataset<HierarchicalElement> readRelationshipFile(SparkSession spark,
      String snomedRelationshipPath) {

    return spark.read()
        .option("header", true)
        .option("delimiter", "\t")
        .csv(snomedRelationshipPath)
        .where(col("typeId").equalTo(lit(SNOMED_ISA_RELATIONSHIP_ID)))
        .where(col("active").equalTo(lit("1")))
        .select(col("destinationId"), col("sourceId"))
        .where(col("destinationId").isNotNull()
            .and(col("destinationId").notEqual(lit(""))))
        .where(col("sourceId").isNotNull()
            .and(col("sourceId").notEqual(lit(""))))
        .map((MapFunction<Row, HierarchicalElement>) row -> {

          HierarchicalElement element = new HierarchicalElement();

          element.setAncestorSystem(SNOMED_CODE_SYSTEM_URI);
          element.setAncestorValue(row.getString(0));

          element.setDescendantSystem(SNOMED_CODE_SYSTEM_URI);
          element.setDescendantValue(row.getString(1));

          return element;
        }, Hierarchies.getHierarchicalElementEncoder());
  }

  /**
   * Returns a ConceptMaps instance with the specified multiaxial hierarchy. This method reads the
   * LOINC multiaxial hierarchy file and convert it to a {@link HierarchicalElement} dataset, and
   * adds it to the given hierarchies.
   *
   * @param spark the Spark session
   * @param hierarchies a {@link Hierarchies} instance to which the hierarchy will be added
   * @param snomedRelationshipPath path to the relationship CSV
   * @param snomedVersion the version of SNOMED being read.
   * @return a {@link Hierarchies} instance that includes the read hierarchy.
   */
  public static Hierarchies withRelationships(SparkSession spark,
      Hierarchies hierarchies,
      String snomedRelationshipPath,
      String snomedVersion) {

    Dataset<HierarchicalElement> elements = readRelationshipFile(spark, snomedRelationshipPath);

    return hierarchies.withHierarchyElements(SNOMED_HIERARCHY_URI, snomedVersion, elements);
  }
}
