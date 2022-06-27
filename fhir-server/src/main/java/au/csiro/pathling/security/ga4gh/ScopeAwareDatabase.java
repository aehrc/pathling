/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.security.ga4gh;

import au.csiro.pathling.config.Configuration;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.io.Database;
import ca.uhn.fhir.context.FhirContext;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

/**
 * A special type of {@link Database} that is capable of limiting the scope of resources based upon
 * the passport scopes that have been applied to this request.
 *
 * @author John Grimes
 */
@Component
@Profile("core & ga4gh")
@Slf4j
public class ScopeAwareDatabase extends Database {

  @Nonnull
  private final Configuration configuration;

  @Nonnull
  private final FhirContext fhirContext;

  @Nonnull
  private final Optional<TerminologyServiceFactory> terminologyServiceFactory;

  @Nonnull
  private final Optional<PassportScope> passportScope;

  /**
   * @param configuration a {@link Configuration} object to control the behaviour of the executor
   * @param fhirContext a {@link FhirContext} for doing FHIR stuff
   * @param spark a {@link SparkSession} for interacting with Spark
   * @param fhirEncoders {@link FhirEncoders} object for creating empty datasets
   * @param terminologyServiceFactory a {@link TerminologyServiceFactory} for resolving terminology
   * queries
   * @param passportScope a {@link PassportScope} that can be used to limit the scope of resources,
   */
  @SuppressWarnings("WeakerAccess")
  public ScopeAwareDatabase(@Nonnull final Configuration configuration,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final SparkSession spark,
      @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final Optional<TerminologyServiceFactory> terminologyServiceFactory,
      @Nonnull final Optional<PassportScope> passportScope,
      @Nonnull final ThreadPoolTaskExecutor executor) {
    super(configuration, spark, fhirEncoders, executor);
    log.debug("Initializing passport scope-aware resource reader");

    this.configuration = configuration;
    this.fhirContext = fhirContext;
    this.terminologyServiceFactory = terminologyServiceFactory;
    this.passportScope = passportScope;
  }

  @Nonnull
  @Override
  @SuppressWarnings("unused")
  public Dataset<Row> read(@Nonnull final ResourceType resourceType) {
    final Dataset<Row> resources = super.read(resourceType);

    // If a passport scope is present, enforce the filters within it before returning the final
    // dataset.
    return passportScope
        .map(scope -> {
          // We need to create a non-scope-aware reader here for the parsing of the filters, so that
          // we don't have recursive application of the filters.
          final Database database = new Database(configuration, spark, fhirEncoders, executor);
          final PassportScopeEnforcer scopeEnforcer = new PassportScopeEnforcer(configuration,
              fhirContext, spark, database, terminologyServiceFactory, scope);
          return scopeEnforcer.enforce(resourceType, resources);
        })
        .orElse(resources);
  }

}
