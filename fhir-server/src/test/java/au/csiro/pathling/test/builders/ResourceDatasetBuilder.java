/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.test.builders;

import javax.annotation.Nonnull;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

/**
 * @author John Grimes
 */
public class ResourceDatasetBuilder extends DatasetBuilder {

  public ResourceDatasetBuilder(@Nonnull final SparkSession spark) {
    super(spark);
  }

  @Nonnull
  @Override
  public DatasetBuilder withIdColumn() {
    return withColumn("id", DataTypes.StringType);
  }

}
