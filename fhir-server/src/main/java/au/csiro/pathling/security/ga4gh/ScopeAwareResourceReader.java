/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.security.ga4gh;

import au.csiro.pathling.Configuration;
import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.io.ResourceReader;
import ca.uhn.fhir.context.FhirContext;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * A special type of {@link ResourceReader} that is capable of limiting the scope of resources based
 * upon the passport scopes that have been applied to this request.
 *
 * @author John Grimes
 */
@Component
@ConditionalOnProperty(prefix = "pathling", name = {"auth.enabled", "auth.ga4gh-passports.enabled"},
    havingValue = "true")
@Profile("core")
@Slf4j
public class ScopeAwareResourceReader extends ResourceReader {

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
   * @param terminologyServiceFactory a {@link TerminologyServiceFactory} for resolving terminology
   * queries
   * @param passportScope a {@link PassportScope} that can be used to limit the scope of resources,
   */
  @SuppressWarnings("WeakerAccess")
  public ScopeAwareResourceReader(@Nonnull final Configuration configuration,
      @Nonnull final FhirContext fhirContext,
      @Nonnull final SparkSession spark,
      @Nonnull final Optional<TerminologyServiceFactory> terminologyServiceFactory,
      @Nonnull final Optional<PassportScope> passportScope) {
    super(configuration, spark);
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
          final ResourceReader resourceReader = new ResourceReader(configuration, spark);
          final PassportScopeEnforcer scopeEnforcer = new PassportScopeEnforcer(configuration,
              fhirContext, spark, resourceReader, terminologyServiceFactory, scope);
          return scopeEnforcer.enforce(resourceType, resources);
        })
        .orElse(resources);
  }

}
