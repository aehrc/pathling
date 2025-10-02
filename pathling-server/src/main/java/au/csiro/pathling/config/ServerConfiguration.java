package au.csiro.pathling.config;

import au.csiro.pathling.PathlingVersion;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.DataSourceBuilder;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import java.util.Optional;
import lombok.Data;
import lombok.ToString;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
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
  public QueryableDataSource deltaLake(SparkSession sparkSession, PathlingContext pathlingContext) {
    return new DataSourceBuilder(pathlingContext).delta(databasePath);
  }

  @NotNull
  private AuthorizationConfiguration auth;

  @NotNull
  private CorsConfiguration cors;
}
