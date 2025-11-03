package au.csiro.pathling.config;

import au.csiro.pathling.PathlingVersion;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.DeltaSource;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.validation.annotation.Validated;

/**
 * @author Felix Naumann
 */
@ConfigurationProperties(prefix = "pathling")
@Validated
@Data
@ToString(doNotUseGetters = true)
public class ServerConfiguration {

  /**
   * Controls the description of this server displayed within the FHIR CapabilityStatement.
   */
  @NotNull
  private String implementationDescription;

  @NotNull
  private HttpServerCachingConfiguration httpCaching;

  @NotNull
  private AsyncConfiguration async;

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

  @Bean
  public PathlingVersion pathlingVersion() {
    return new PathlingVersion();
  }

  @Bean
  public FhirContext fhirContext() {
    return FhirEncoders.forR4().getOrCreate().getContext();
  }

  @Bean
  public IParser jsonParser() {
    return fhirContext().newJsonParser();
  }

  @Bean
  public PathlingContext pathlingContext() {
    return PathlingContext.create();
  }

  @Bean
  public QueryableDataSource deltaLake(PathlingContext pathlingContext) {
    return new DeltaSource(pathlingContext, databasePath);
  }

  // Handle the `import` property outside of Lombok, as import is a Java keyword.
  @Getter(AccessLevel.NONE)
  @Setter(AccessLevel.NONE)
  @NotNull
  private ImportConfiguration import_;

  @Nonnull
  public ImportConfiguration getImport() {
    return import_;
  }

  public void setImport(@Nonnull final ImportConfiguration import_) {
    this.import_ = import_;
  }

  @NotNull
  private AuthorizationConfiguration auth;

  @NotNull
  private CorsConfiguration cors;
}
