/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.config;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Configuration relating to the storage of data.
 */
@Data
@Builder(toBuilder = true)
@AllArgsConstructor
@NoArgsConstructor
public class StorageConfiguration {

  /**
   * The base URL at which Pathling will look for data files, and where it will save data received
   * within import requests.
   */
  @NotBlank
  @Builder.Default
  private String warehouseUrl = "file:///usr/share/warehouse";

  /**
   * The subdirectory within the warehouse path used to read and write data.
   */
  @NotBlank
  @Pattern(regexp = "[A-Za-z0-9-_]+")
  @Size(min = 1, max = 50)
  @Builder.Default
  private String databaseName = "default";

  /**
   * This controls whether the built-in caching within Spark is used for resource datasets. It may
   * be useful to turn this off for large datasets in memory-constrained environments.
   */
  @NotNull
  @Builder.Default
  private Boolean cacheDatasets = true;

  /**
   * When a table is updated, the number of partitions is checked. If the number exceeds this
   * threshold, the table will be repartitioned back to the default number of partitions. This
   * prevents large numbers of small updates causing poor subsequent query performance.
   */
  @NotNull
  @Min(1)
  @Builder.Default
  private int compactionThreshold = 10;

}
