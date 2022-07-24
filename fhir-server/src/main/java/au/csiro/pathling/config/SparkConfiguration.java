/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.config;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import lombok.Data;

/**
 * Represents configuration that controls the behaviour of Apache Spark.
 */
@Data
public class SparkConfiguration {

  /**
   * The name that Pathling will be identified as within the Spark cluster.
   */
  @NotBlank
  private String appName;

  /**
   * Setting this option to {@code true} will enable additional logging relating to the query plan
   * used to execute queries.
   */
  @NotNull
  private Boolean explainQueries;

  /**
   * This controls whether the built-in caching within Spark is used for resource datasets and
   * search results. It may be useful to turn this off for large datasets in memory-constrained
   * environments.
   */
  @NotNull
  private Boolean cacheDatasets;

  /**
   * When a table is updated, the number of partitions is checked. If the number exceeds this
   * threshold, the table will be repartitioned back to the default number of partitions. This
   * prevents large numbers of small updates causing poor subsequent query performance.
   */
  @NotNull
  @Min(1)
  private int compactionThreshold;

}
