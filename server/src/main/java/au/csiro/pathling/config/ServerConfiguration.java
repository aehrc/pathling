package au.csiro.pathling.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.annotation.PostConstruct;
import jakarta.validation.constraints.NotNull;
import java.util.Optional;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * @author Felix Naumann
 */
@ConfigurationProperties(prefix = "pathling")
@Validated
@Data
@ToString(doNotUseGetters = true)
@Slf4j
public class ServerConfiguration {

  /**
   * Controls the description of this server displayed within the FHIR CapabilityStatement.
   */
  @NotNull
  private String implementationDescription;

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

  @Value("${pathling.storage.warehouseUrl}/${pathling.storage.databaseName}")
  private String databasePath;

  @Nonnull
  public Optional<String> getSentryDsn() {
    return Optional.ofNullable(sentryDsn);
  }

  @Nonnull
  public Optional<String> getSentryEnvironment() {
    return Optional.ofNullable(sentryEnvironment);
  }

  @NotNull
  private SparkConfiguration spark = SparkConfiguration.builder().build();

  @NotNull
  private StorageConfiguration storage = StorageConfiguration.builder().build();

  @NotNull
  private EncodingConfiguration encoding = EncodingConfiguration.builder().build();

  @NotNull
  private TerminologyConfiguration terminology = TerminologyConfiguration.builder().build();

  @NotNull
  private AuthorizationConfiguration auth;

  @NotNull
  private HttpServerCachingConfiguration httpCaching;

  @NotNull
  @JsonProperty("import")
  private ImportConfiguration import_;

  @Nullable
  public ImportConfiguration getImport() {
    return import_;
  }

  public void setImport(@Nonnull final ImportConfiguration newImport) {
    this.import_ = newImport;
  }

  @NotNull
  @JsonProperty("export")
  private ExportConfiguration export = new ExportConfiguration();

  @NotNull
  private AsyncConfiguration async;

  @NotNull
  private CorsConfiguration cors;

  @Nullable
  private BulkSubmitConfiguration bulkSubmit;

  /**
   * Logs the server configuration on startup.
   */
  @PostConstruct
  public void logConfiguration() {
    log.debug("Server configuration: {}", this);
  }

}
