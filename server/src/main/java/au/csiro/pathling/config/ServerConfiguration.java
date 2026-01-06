/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import jakarta.annotation.PostConstruct;
import jakarta.validation.Valid;
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

  /** Controls the description of this server displayed within the FHIR CapabilityStatement. */
  @NotNull private String implementationDescription;

  /**
   * If this variable is set, all errors will be reported to a Sentry service, e.g.
   * `https://abc123@sentry.io/123456`.
   */
  @Nullable private String sentryDsn;

  /** Sets the environment that will be sent with Sentry reports. */
  @Nullable private String sentryEnvironment;

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

  @Valid @NotNull private SparkConfiguration spark = SparkConfiguration.builder().build();

  @Valid @NotNull private StorageConfiguration storage = StorageConfiguration.builder().build();

  @Valid @NotNull private EncodingConfiguration encoding = EncodingConfiguration.builder().build();

  @Valid @NotNull
  private TerminologyConfiguration terminology = TerminologyConfiguration.builder().build();

  @Valid @NotNull private AuthorizationConfiguration auth;

  @Valid @NotNull private HttpServerCachingConfiguration httpCaching;

  @Valid
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

  @Valid
  @NotNull
  @JsonProperty("export")
  private ExportConfiguration export = new ExportConfiguration();

  @Valid @NotNull private AsyncConfiguration async;

  @Valid @NotNull private CorsConfiguration cors;

  @Valid @Nullable private BulkSubmitConfiguration bulkSubmit;

  @Valid @NotNull private QueryConfiguration query = QueryConfiguration.builder().build();

  /** Configuration for enabling/disabling individual server operations. */
  @Valid @NotNull private OperationConfiguration operations = new OperationConfiguration();

  /** Configuration for the admin UI. */
  @Valid @NotNull private AdminUiConfiguration adminUi = new AdminUiConfiguration();

  /** Logs the server configuration on startup. */
  @PostConstruct
  public void logConfiguration() {
    log.debug("Server configuration: {}", this);
  }
}
