/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
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
