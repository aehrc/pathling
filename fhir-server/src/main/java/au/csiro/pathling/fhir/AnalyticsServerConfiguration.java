/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhir;

import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * @author John Grimes
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class AnalyticsServerConfiguration {

  /**
   * (OPTIONAL) Version of this API, as advertised within the CapabilityStatement.
   */
  @Nullable
  private String version;

  /**
   * (OPTIONAL) URL for the Apache Spark cluster this server should use.
   */
  @Nonnull
  private String sparkMasterUrl;

  /**
   * (OPTIONAL) URL for the location of warehouse tables.
   */
  @Nonnull
  private String warehouseUrl;

  /**
   * (OPTIONAL) Name of the database within the warehouse that this server should make available.
   */
  @Nonnull
  private String databaseName;

  /**
   * (OPTIONAL) Quantity of memory to make available to Spark executors.
   */
  @Nonnull
  private String executorMemory;

  /**
   * (OPTIONAL) URL of a FHIR terminology service to use in satisfying terminology queries.
   */
  @Nullable
  private String terminologyServerUrl;

  /**
   * (OPTIONAL) Socket timeout for connections to the FHIR terminology service.
   */
  private int terminologySocketTimeout;

  /**
   * (OPTIONAL) Whether to run an explain ahead of each Spark SQL query.
   */
  private boolean explainQueries;

  /**
   * (OPTIONAL) Turns on verbose logging of the details of requests to the analytics server, and
   * requests to the terminology server.
   */
  private boolean verboseRequestLogging;

  /**
   * (OPTIONAL) Number of partitions to use when shuffling data for joins or aggregations.
   */
  private int shufflePartitions;

  /**
   * (OPTIONAL) Authentication information for reading and writing data using Amazon S3.
   */
  private String awsAccessKeyId;

  /**
   * (OPTIONAL) Authentication information for reading and writing data using Amazon S3.
   */
  private String awsSecretAccessKey;

  /**
   * (OPTIONAL) Allowed origins for the CORS configuration.
   */
  private List<String> corsAllowedOrigins;

  /**
   * (OPTIONAL) Enables SMART authorisation on all requests.
   */
  private boolean authEnabled;

  /**
   * (REQUIRED if authEnabled) The URL of the JSON Web Key set containing the public key used to
   * verify incoming bearer tokens.
   */
  private String authJwksUrl;

  /**
   * (REQUIRED if authEnabled) The value of the issuer claim expected within incoming bearer
   * tokens.
   */
  private String authIssuer;

  /**
   * (REQUIRED if authEnabled) The value of the audience claim expected within incoming bearer
   * tokens.
   */
  private String authAudience;

  /**
   * (OPTIONAL) The URL which will be advertised as the authorization endpoint.
   */
  private String authorizeUrl;

  /**
   * (OPTIONAL) The URL which will be advertised as the token endpoint.
   */
  private String tokenUrl;

  /**
   * (OPTIONAL) The URL which will be advertised as the token revocation endpoint.
   */
  private String revokeTokenUrl;

  /**
   * (OPTIONAL) A prefix to add to the API endpoint.
   */
  private String httpBase;

  public AnalyticsServerConfiguration() {
    sparkMasterUrl = "local[*]";
    warehouseUrl = "file:///usr/share/warehouse";
    databaseName = "default";
    executorMemory = "1g";
    terminologyServerUrl = "https://r4.ontoserver.csiro.au/fhir";
    terminologySocketTimeout = 60000;
    explainQueries = false;
    verboseRequestLogging = false;
    shufflePartitions = 2;
    corsAllowedOrigins = Collections.singletonList("*");
  }

  @Nullable
  public String getVersion() {
    return version;
  }

  public void setVersion(@Nullable String version) {
    this.version = version;
  }

  @Nullable
  public String getMajorVersion() {
    String version = getVersion();
    return version == null
           ? null
           : version.substring(0, 1);
  }

  @Nonnull
  public String getSparkMasterUrl() {
    return sparkMasterUrl;
  }

  public void setSparkMasterUrl(@Nonnull String sparkMasterUrl) {
    this.sparkMasterUrl = sparkMasterUrl;
  }

  @Nonnull
  public String getWarehouseUrl() {
    return warehouseUrl;
  }

  public void setWarehouseUrl(@Nonnull String warehouseUrl) {
    this.warehouseUrl = warehouseUrl;
  }

  @Nonnull
  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(@Nonnull String databaseName) {
    this.databaseName = databaseName;
  }

  @Nonnull
  public String getExecutorMemory() {
    return executorMemory;
  }

  public void setExecutorMemory(@Nonnull String executorMemory) {
    this.executorMemory = executorMemory;
  }

  @Nonnull
  public String getTerminologyServerUrl() {
    return terminologyServerUrl;
  }

  public void setTerminologyServerUrl(@Nonnull String terminologyServerUrl) {
    this.terminologyServerUrl = terminologyServerUrl;
  }

  public int getTerminologySocketTimeout() {
    return terminologySocketTimeout;
  }

  public void setTerminologySocketTimeout(int terminologySocketTimeout) {
    this.terminologySocketTimeout = terminologySocketTimeout;
  }

  public boolean isExplainQueries() {
    return explainQueries;
  }

  public void setExplainQueries(boolean explainQueries) {
    this.explainQueries = explainQueries;
  }

  public boolean isVerboseRequestLogging() {
    return verboseRequestLogging;
  }

  public void setVerboseRequestLogging(boolean verboseRequestLogging) {
    this.verboseRequestLogging = verboseRequestLogging;
  }

  public int getShufflePartitions() {
    return shufflePartitions;
  }

  public void setShufflePartitions(int shufflePartitions) {
    this.shufflePartitions = shufflePartitions;
  }

  public String getAwsAccessKeyId() {
    return awsAccessKeyId;
  }

  public void setAwsAccessKeyId(String awsAccessKeyId) {
    this.awsAccessKeyId = awsAccessKeyId;
  }

  public String getAwsSecretAccessKey() {
    return awsSecretAccessKey;
  }

  public void setAwsSecretAccessKey(String awsSecretAccessKey) {
    this.awsSecretAccessKey = awsSecretAccessKey;
  }

  public List<String> getCorsAllowedOrigins() {
    return corsAllowedOrigins;
  }

  public void setCorsAllowedOrigins(List<String> corsAllowedOrigins) {
    this.corsAllowedOrigins = corsAllowedOrigins;
  }

  public boolean isAuthEnabled() {
    return authEnabled;
  }

  public void setAuthEnabled(boolean authEnabled) {
    this.authEnabled = authEnabled;
  }

  public String getAuthJwksUrl() {
    return authJwksUrl;
  }

  public void setAuthJwksUrl(String authJwksUrl) {
    this.authJwksUrl = authJwksUrl;
  }

  public String getAuthIssuer() {
    return authIssuer;
  }

  public void setAuthIssuer(String authIssuer) {
    this.authIssuer = authIssuer;
  }

  public String getAuthAudience() {
    return authAudience;
  }

  public void setAuthAudience(String authAudience) {
    this.authAudience = authAudience;
  }

  public String getAuthorizeUrl() {
    return authorizeUrl;
  }

  public void setAuthorizeUrl(String authorizeUrl) {
    this.authorizeUrl = authorizeUrl;
  }

  public String getTokenUrl() {
    return tokenUrl;
  }

  public void setTokenUrl(String tokenUrl) {
    this.tokenUrl = tokenUrl;
  }

  public String getRevokeTokenUrl() {
    return revokeTokenUrl;
  }

  public void setRevokeTokenUrl(String revokeTokenUrl) {
    this.revokeTokenUrl = revokeTokenUrl;
  }

  public String getHttpBase() {
    return httpBase;
  }

  public void setHttpBase(String httpBase) {
    this.httpBase = httpBase;
  }

  @Override
  public String toString() {
    return "AnalyticsServerConfiguration{" +
        "version='" + version + '\'' +
        ", sparkMasterUrl='" + sparkMasterUrl + '\'' +
        ", warehouseUrl='" + warehouseUrl + '\'' +
        ", databaseName='" + databaseName + '\'' +
        ", executorMemory='" + executorMemory + '\'' +
        ", terminologyServerUrl='" + terminologyServerUrl + '\'' +
        ", terminologySocketTimeout=" + terminologySocketTimeout +
        ", explainQueries=" + explainQueries +
        ", verboseRequestLogging=" + verboseRequestLogging +
        ", shufflePartitions=" + shufflePartitions +
        ", awsAccessKeyId='" + awsAccessKeyId + '\'' +
        ", corsAllowedOrigins=" + corsAllowedOrigins +
        ", authEnabled=" + authEnabled +
        ", authJwksUrl='" + authJwksUrl + '\'' +
        ", authIssuer='" + authIssuer + '\'' +
        ", authAudience='" + authAudience + '\'' +
        ", authorizeUrl='" + authorizeUrl + '\'' +
        ", tokenUrl='" + tokenUrl + '\'' +
        ", revokeTokenUrl='" + revokeTokenUrl + '\'' +
        ", httpBase='" + httpBase + '\'' +
        '}';
  }

}
