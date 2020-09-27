/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling;

import java.util.List;
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
   * Controls the description of this server displayed within the FHIR CapabilityStatement.
   */
  @NotNull
  private String implementationDescription;

  /**
   * Setting this option to {@code true} will enable additional logging of the details of requests
   * to the server, and between the server and the terminology service.
   */
  @NotNull
  private Boolean verboseRequestLogging;

  @NotNull
  private Spark spark;

  @NotNull
  private Storage storage;

  @NotNull
  private Terminology terminology;

  @NotNull
  private Authorisation auth;

  @NotNull
  private Caching caching;

  @NotNull
  private Cors cors;

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
     * Setting this option to {@code true} will enable additional logging relating to the query plan
     * used to execute queries.
     */
    @NotNull
    private Boolean explainQueries;

  }

  /**
   * Configuration relating to the storage of data.
   */
  @Data
  public static class Storage {

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

      @Nonnull
      public Optional<String> getAccessKeyId() {
        return Optional.ofNullable(accessKeyId);
      }

      @Nonnull
      public Optional<String> getSecretAccessKey() {
        return Optional.ofNullable(secretAccessKey);
      }

    }

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
    @Min(0)
    private long aggregateRequestCacheSize;

    @NotNull
    @Min(0)
    private long searchBundleCacheSize;

    @NotNull
    @Min(0)
    private long searchPageCacheSize;

    @NotNull
    @Min(0)
    private long resourceReaderCacheSize;

  }

  /**
   * Configures the CORS functionality of the server.
   */
  @Data
  public static class Cors {

    @NotNull
    private List<String> allowedOrigins;

    @NotNull
    private List<String> allowedMethods;

    @NotNull
    private List<String> allowedHeaders;

    @Nullable
    private List<String> exposeHeaders;

    @NotNull
    private long maxAge;

    @Nullable
    public Optional<List<String>> getExposeHeaders() {
      return Optional.ofNullable(exposeHeaders);
    }

  }

}
