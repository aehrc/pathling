/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.config;

import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;
import lombok.Data;
import lombok.ToString;

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

  @NotNull
  private Aws aws;

  /**
   * Configuration relating to storage of data using Amazon Web Services (AWS).
   */
  @Data
  public static class Aws {

    /**
     * Public buckets can be accessed by default, set this to false to access protected buckets.
     */
    @NotNull
    private boolean anonymousAccess;

    /**
     * Authentication details for connecting to a protected Amazon S3 bucket.
     */
    @Nullable
    private String accessKeyId;

    /**
     * Authentication details for connecting to a protected Amazon S3 bucket.
     */
    @Nullable
    @ToString.Exclude
    private String secretAccessKey;

    /**
     * The ARN of an IAM role that should be assumed using STS.
     */
    @Nullable
    private String assumedRole;

    @Nonnull
    public Optional<String> getAccessKeyId() {
      return Optional.ofNullable(accessKeyId);
    }

    @Nonnull
    public Optional<String> getSecretAccessKey() {
      return Optional.ofNullable(secretAccessKey);
    }

    @Nonnull
    public Optional<String> getAssumedRole() {
      return Optional.ofNullable(assumedRole);
    }

  }

}
