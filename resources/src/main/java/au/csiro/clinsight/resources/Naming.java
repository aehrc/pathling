/*
 * Copyright © Australian e-Health Research Centre, CSIRO. All rights reserved.
 */

package au.csiro.clinsight.resources;

import java.util.Random;

/**
 * Utility class for generating unique dimension and fact table names from Dimension and Metric
 * resources.
 *
 * @author John Grimes
 */
public abstract class Naming {

  public static String tableNameForDimension(Dimension dimension) {
    return "dimension_" + dimension.getName() + "_" + dimension.getKey();
  }

  public static String tableNameForMultiValuedDimension(FactSet factSet, String attributeName,
      Dimension dimension) {
    String tableName = "bridge_" + tableNameForFactSet(factSet) + "_" + attributeName + "_"
        + tableNameForDimension(
        dimension);
    return tableName.replaceAll("(fact_|dimension_|)", "");
  }

  public static String tableNameForFactSet(FactSet factSet) {
    return "fact_" + factSet.getName() + "_" + factSet.getKey();
  }

  public static String fieldNameForDimensionAttribute(DimensionAttribute dimensionAttribute) {
    return dimensionAttribute.getTitle();
  }

  public static String generateRandomKey() {
    return Integer.toUnsignedString(new Random().nextInt(), 36);
  }

  // TODO: Make pluralisation a little bit more sophisticated.
  public static String pluraliseResourceName(String name) {
    return name + "s";
  }

  public static String lowerFirstLetter(String name) {
    return name.substring(0, 1).toLowerCase() + name.substring(1);
  }

}
