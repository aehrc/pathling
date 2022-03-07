/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
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

  /**
   * If this variable is set, all errors will be reported to a Sentry service, e.g.
   * `https://abc123@sentry.io/123456`.
   */
  @Nullable
  private String sentryDsn;

  /**
   * Sets the environment that will be sent with Sentry reports.
   */
  @Nullable
  private String sentryEnvironment;

  @Nonnull
  public Optional<String> getSentryDsn() {
    return Optional.ofNullable(sentryDsn);
  }

  @Nonnull
  public Optional<String> getSentryEnvironment() {
    return Optional.ofNullable(sentryEnvironment);
  }

  @NotNull
  private Spark spark;

  @NotNull
  private Storage storage;

  @NotNull
  private Terminology terminology;

  @NotNull
  private Authorization auth;

  @NotNull
  private HttpCaching httpCaching;

  @NotNull
  private Cors cors;

  // Handle the `import` property outside of Lombok, as import is a Java keyword.
  @Getter(AccessLevel.NONE)
  @Setter(AccessLevel.NONE)
  @NotNull
  private Import import_;

  @NotNull
  private Async async;

  @Nonnull
  public Import getImport() {
    return import_;
  }

  public void setImport(@Nonnull final Import import_) {
    this.import_ = import_;
  }

  @NotNull
  private Encoding encoding;

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
   * Represents configuration specific to authorization.
   */
  @Data
  @ToString(doNotUseGetters = true)
  public static class Authorization {

    /**
     * Enables authorization.
     */
    @NotNull
    private boolean enabled;

    /**
     * Configures the issuing domain for bearer tokens, which will be checked against the claims
     * within incoming bearer tokens.
     */
    @Nullable
    private String issuer;

    /**
     * Configures the audience for bearer tokens, which is the FHIR endpoint that tokens are
     * intended to be authorized for.
     */
    @Nullable
    private String audience;

    @NotNull
    private Ga4ghPassports ga4ghPassports;

    @Nonnull
    public Optional<String> getIssuer() {
      return Optional.ofNullable(issuer);
    }

    @Nonnull
    public Optional<String> getAudience() {
      return Optional.ofNullable(audience);
    }

    /**
     * Configuration relating to support for GA4GH Passports as a method of authorization.
     */
    @Data
    public static class Ga4ghPassports {

      @NotNull
      private String patientIdSystem;

      /**
       * A set of allowable visa issuers.
       */
      @NotNull
      private List<String> allowedVisaIssuers;

    }

  }

  @Data
  public static class HttpCaching {

    /**
     * A list of values to return within the Vary header.
     */
    @NotNull
    private List<String> vary;

    /**
     * A list of values to return within the Cache-Control header, for cacheable responses.
     */
    @NotNull
    private List<String> cacheableControl;

    /**
     * A list of values to return within the Cache-Control header, for uncacheable responses.
     */
    @NotNull
    private List<String> uncacheableControl;

  }

  /**
   * Represents configuration relating to Cross-Origin Resource Sharing (CORS).
   */
  @Data
  public static class Cors {

    @NotNull
    private List<String> allowedOrigins;

    @NotNull
    private List<String> allowedOriginPatterns;

    @NotNull
    private List<String> allowedMethods;

    @NotNull
    private List<String> allowedHeaders;

    @NotNull
    private List<String> exposedHeaders;

    @NotNull
    @Min(0)
    private Long maxAge;

  }

  /**
   * Represents configuration specific to import functionality.
   */
  @Data
  public static class Import {

    /**
     * A set of URL prefixes which are allowable for use within the import operation.
     */
    @NotNull
    private List<String> allowableSources;

  }

  /**
   * Represents configuration relating to asynchronous processing.
   */
  @Data
  public static class Async {

    /**
     * Enables asynchronous process of requests.
     */
    @NotNull
    private boolean enabled;

  }


  /**
   * Represents configuration specific to FHIR encoding.
   */
  @Data
  public static class Encoding {

    /**
     * The maximum nesting level for recursive data types.
     */
    @NotNull
    @Min(0)
    private Integer maxNestingLevel;

    /**
     * The list of types that are encoded within open types, such as extensions.
     */
    @NotNull
    private Set<String> openTypes;

    /**
     * Enables support for FHIR extensions.
     */
    @NotNull
    private boolean enableExtensions;

  }

}
