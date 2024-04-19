/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Optional;
import javax.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Defines all the configuration options for the Pathling server.
 * <p>
 * See {@code application.yml} for default values.
 *
 * @author John Grimes
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
  private SparkConfiguration spark = SparkConfiguration.builder().build();

  @NotNull
  private StorageConfiguration storage = StorageConfiguration.builder().build();

  @NotNull
  private QueryConfiguration query = QueryConfiguration.builder().build();

  @NotNull
  private EncodingConfiguration encoding = EncodingConfiguration.builder().build();

  @NotNull
  private TerminologyConfiguration terminology = TerminologyConfiguration.builder().build();

  @NotNull
  private AuthorizationConfiguration auth;

  @NotNull
  private HttpServerCachingConfiguration httpCaching;

  @NotNull
  private CorsConfiguration cors;

  // Handle the `import` property outside of Lombok, as import is a Java keyword.
  @Getter(AccessLevel.NONE)
  @Setter(AccessLevel.NONE)
  @NotNull
  private ImportConfiguration import_;

  @NotNull
  private AsyncConfiguration async;

  @Nonnull
  public ImportConfiguration getImport() {
    return import_;
  }

  public void setImport(@Nonnull final ImportConfiguration import_) {
    this.import_ = import_;
  }

}
