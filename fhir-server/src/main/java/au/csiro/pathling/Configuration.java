/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling;

import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.constraints.*;
import lombok.Data;
import lombok.ToString;
import org.hibernate.validator.constraints.URL;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Defines all of the configuration options for the Pathling server.
 * <p>
 * See {@code application.yml} for default values.
 *
 * @author John Grimes
 */
@ConfigurationProperties(prefix = "pathling")
@Validated
@Data
@ToString(doNotUseGetters = true)
public class Configuration {

  /**
   * The version number to advertise for this server, e.g. in the capability statement.
   */
  @NotBlank
  private String version;

  /* GENERAL */

  /**
   * The port which the server should bind to and listen for HTTP connections.
   */
  @NotNull
  private Integer httpPort;

  /**
   * A prefix to add to the API endpoint, e.g. a value of {@code /foo} would cause the FHIR endpoint
   * to be changed to {@code /foo/fhir}.
   */
  @NotNull
  private String httpBase;

  /**
   * Setting this option to {@code true} will enable additional logging of the details of requests
   * to the server, and between the server and the terminology service.
   */
  @NotNull
  private Boolean verboseRequestLogging;

  /* STORAGE */

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

  /**
   * Authentication details for connecting to a protected Amazon S3 bucket.
   */
  @Nullable
  private String awsAccessKeyId;

  /**
   * Authentication details for connecting to a protected Amazon S3 bucket.
   */
  @Nullable
  @ToString.Exclude
  private String awsSecretAccessKey;

  @Nonnull
  public Optional<String> getAwsAccessKeyId() {
    return Optional.ofNullable(awsAccessKeyId);
  }

  @Nonnull
  public Optional<String> getAwsSecretAccessKey() {
    return Optional.ofNullable(awsSecretAccessKey);
  }

  /* CROSS-ORIGIN RESOURCE SHARING (CORS) */

  @NotBlank
  private String corsAllowedDomains;

  /* APACHE SPARK */

  @NotNull
  private Spark spark;

  /* TERMINOLOGY SERVICE */

  @NotNull
  private Terminology terminology;

  /* AUTHORISATION */

  @NotNull
  private Authorisation auth;

  /* CACHING */

  @NotNull
  private Caching caching;

  /**
   * Represents configuration that controls the behaviour of Apache Spark.
   */
  @Data
  public static class Spark {

    /**
     * The name that Pathling will be identified as within the Spark cluster.
     */
    @NotBlank
    private String appName;

    /**
     * Address of the master node of an Apache Spark cluster to use for processing data.
     */
    @NotBlank
    private String masterUrl;

    /**
     * Hostname or IP address to use when binding listening sockets within Spark.
     */
    @NotBlank
    private String bindAddress;

    /**
     * The quantity of memory available for each child task to process data within, in the same
     * format as JVM memory strings.
     */
    @NotBlank
    private String executorMemory;

    /**
     * This option controls the number of data partitions used to distribute data between child
     * tasks. This can be tuned to higher numbers for larger data sets. It also controls the
     * granularity of requests made to the configured terminology service.
     */
    @NotNull
    @Min(1)
    private Integer shufflePartitions;

    /**
     * Setting this option to {@code true} will enable additional logging relating to the query plan
     * used to execute queries.
     */
    @NotNull
    private Boolean explainQueries;

  }

  /**
   * Represents configuration specific to the terminology functions of the server.
   */
  @Data
  public static class Terminology {

    /**
     * Enables the use of terminology functions.
     */
    @NotNull
    private boolean enabled;

    /**
     * The endpoint of a FHIR terminology service (R4) that the server can use to resolve
     * terminology queries.
     */
    @NotBlank
    @URL
    private String serverUrl;

    /**
     * The maximum period (in milliseconds) that the server should wait for incoming data from the
     * terminology service.
     */
    @NotNull
    @Min(0)
    private Integer socketTimeout;

  }

  /**
   * Represents configuration specific to SMART authorisation.
   */
  @Data
  @ToString(doNotUseGetters = true)
  public static class Authorisation {

    /**
     * Enables SMART authorisation.
     */
    @NotNull
    private boolean enabled;

    /**
     * Provides the URL of a JSON Web Key Set from which the signing key for incoming bearer tokens
     * can be retrieved.
     */
    @URL
    @Nullable
    private String jwksUrl;

    /**
     * Configures the issuing domain for bearer tokens, which will be checked against the claims
     * within incoming bearer tokens.
     */
    @Nullable
    private String issuer;

    /**
     * Configures the audience for bearer tokens, which is the FHIR endpoint that tokens are
     * intended to be authorised for.
     */
    @Nullable
    private String audience;

    /**
     * Provides the URL which will be advertised as the authorization endpoint.
     */
    @URL
    @Nullable
    private String authorizeUrl;

    /**
     * Provides the URL which will be advertised as the token endpoint.
     */
    @URL
    @Nullable
    private String tokenUrl;

    /**
     * Provides the URL which will be advertised as the token revocation endpoint.
     */
    @URL
    @Nullable
    private String revokeUrl;

    @Nonnull
    public Optional<String> getJwksUrl() {
      return Optional.ofNullable(jwksUrl);
    }

    @Nonnull
    public Optional<String> getIssuer() {
      return Optional.ofNullable(issuer);
    }

    @Nonnull
    public Optional<String> getAudience() {
      return Optional.ofNullable(audience);
    }

    @Nonnull
    public Optional<String> getAuthorizeUrl() {
      return Optional.ofNullable(authorizeUrl);
    }

    @Nonnull
    public Optional<String> getTokenUrl() {
      return Optional.ofNullable(tokenUrl);
    }

    @Nonnull
    public Optional<String> getRevokeUrl() {
      return Optional.ofNullable(revokeUrl);
    }

  }

  /**
   * Represents configuration specific to request caching.
   */
  @Data
  public static class Caching {

    /**
     * Controls whether request caching is enabled.
     */
    @NotNull
    private boolean enabled;

    /**
     * Controls the maximum number of cache entries held in memory.
     */
    @NotNull
    private long maxEntries;

  }

}
