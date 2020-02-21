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
 * Support for LOINC.
 */
public class Loinc {

  /**
   * Hierarchy URI used for LOINC is-a relationships.
   */
  public static final String LOINC_HIERARCHY_URI = Hierarchies.HIERARCHY_URI_PREFIX + "loinc";

  /**
   * LOINC code system URI.
   */
  public static final String LOINC_CODE_SYSTEM_URI = "http://loinc.org";

  /**
   * Reads the LOINC mutliaxial hierarchy file and converts it to a {@link HierarchicalElement}
   * dataset.
   *
   * @param spark the Spark session
   * @param loincHierarchyPath path to the multiaxial hierarchy CSV
   * @return a dataset of {@link HierarchicalElement} representing the hierarchical relationship.
   */
  public static Dataset<HierarchicalElement> readMultiaxialHierarchyFile(SparkSession spark,
      String loincHierarchyPath) {

    return spark.read()
        .option("header", true)
        .csv(loincHierarchyPath)
        .select(col("IMMEDIATE_PARENT"), col("CODE"))
        .where(col("IMMEDIATE_PARENT").isNotNull()
            .and(col("IMMEDIATE_PARENT").notEqual(lit(""))))
        .where(col("CODE").isNotNull()
            .and(col("CODE").notEqual(lit(""))))
        .map((MapFunction<Row, HierarchicalElement>) row -> {

          HierarchicalElement element = new HierarchicalElement();

          element.setAncestorSystem(LOINC_CODE_SYSTEM_URI);
          element.setAncestorValue(row.getString(0));

          element.setDescendantSystem(LOINC_CODE_SYSTEM_URI);
          element.setDescendantValue(row.getString(1));

          return element;
        }, Hierarchies.getHierarchicalElementEncoder());
  }

  /**
   * Returns a {@link Hierarchies} instance with the specified multiaxial hierarchy. This method
   * reads the LOINC multiaxial hierarchy file and converts it to a {@link HierarchicalElement}
   * dataset, and adds it to the given hierarchies.
   *
   * @param spark the Spark session
   * @param hierarchies a {@link Hierarchies} instance to which the hierarchy will be added
   * @param loincHierarchyPath path to the multiaxial hierarchy CSV
   * @param loincVersion the version of the LOINC hierarchy being read
   * @return a {@link Hierarchies} instance that includes the read hierarchy.
   */
  public static Hierarchies withLoincHierarchy(SparkSession spark,
      Hierarchies hierarchies,
      String loincHierarchyPath,
      String loincVersion) {

    Dataset<HierarchicalElement> elements = readMultiaxialHierarchyFile(spark, loincHierarchyPath);

    return hierarchies.withHierarchyElements(LOINC_HIERARCHY_URI, loincVersion, elements);
  }
}
