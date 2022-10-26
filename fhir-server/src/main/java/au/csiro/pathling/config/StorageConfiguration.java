/*
 * Copyright 2022 Commonwealth Scientific and Industrial Research
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

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;
import lombok.Data;

/**
 * Configuration relating to the storage of data.
 */
@Data
public class StorageConfiguration {

  /**
   * The base URL at which Pathling will look for data files, and where it will save data received
   * within import requests. Can be an Amazon S3 ({@code s3://}), HDFS ({@code hdfs://}) or
   * filesystem ({@code file://}) URL.
   */
  @NotBlank
  private String warehouseUrl;

  /**
   * The subdirectory within the warehouse path used to read and write data.
   */
  @NotBlank
  @Pattern(regexp = "[A-Za-z0-9-_]+")
  @Size(min = 1, max = 50)
  private String databaseName;

}
