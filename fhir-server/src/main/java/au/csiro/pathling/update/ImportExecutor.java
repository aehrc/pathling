/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.update;

import au.csiro.pathling.caching.CacheInvalidator;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.UnsupportedResourceError;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.errors.SecurityError;
import au.csiro.pathling.fhir.FhirContextFactory;
import au.csiro.pathling.io.AccessRules;
import au.csiro.pathling.io.PersistenceScheme;
import au.csiro.pathling.io.ResourceReader;
import au.csiro.pathling.io.ResourceWriter;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.OperationOutcome.IssueSeverity;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;
import org.hl7.fhir.r4.model.OperationOutcome.OperationOutcomeIssueComponent;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.hl7.fhir.r4.model.UrlType;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Encapsulates the execution of an import operation.
 *
 * @author John Grimes
 */
@Component
@Profile("core")
@Slf4j
public class ImportExecutor {

  @Nonnull
  private final SparkSession spark;

  @Nonnull
  private final ResourceReader resourceReader;

  @Nonnull
  private final ResourceWriter resourceWriter;

  @Nonnull
  private final FhirEncoders fhirEncoders;

  @Nonnull
  private final FhirContextFactory fhirContextFactory;

  @Nonnull
  private final Optional<CacheInvalidator> cacheInvalidator;

  @Nonnull
  private final AccessRules accessRules;

  /**
   * @param spark A {@link SparkSession} for resolving Spark queries
   * @param resourceReader A {@link ResourceReader} for retrieving resources
   * @param resourceWriter A {@link ResourceWriter} for saving resources
   * @param fhirEncoders A {@link FhirEncoders} object for converting data back into HAPI FHIR
   * @param fhirContextFactory A {@link FhirContextFactory} for constructing FhirContext objects in
   * the context of parallel processing
   * @param cacheInvalidator A {@link CacheInvalidator} for invalidating caches upon import
   * @param accessRules A {@link AccessRules} for validating access to URLs
   */
  public ImportExecutor(@Nonnull final SparkSession spark,
      @Nonnull final ResourceReader resourceReader, @Nonnull final ResourceWriter resourceWriter,
      @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final FhirContextFactory fhirContextFactory,
      @Nonnull final Optional<CacheInvalidator> cacheInvalidator,
      @Nonnull final AccessRules accessRules) {
    this.spark = spark;
    this.resourceReader = resourceReader;
    this.resourceWriter = resourceWriter;
    this.fhirEncoders = fhirEncoders;
    this.fhirContextFactory = fhirContextFactory;
    this.cacheInvalidator = cacheInvalidator;
    this.accessRules = accessRules;
  }

  /**
   * Executes an import request.
   *
   * @param inParams a FHIR {@link Parameters} object describing the import request
   * @return a FHIR {@link OperationOutcome} resource describing the result
   */
  @Nonnull
  public OperationOutcome execute(@Nonnull @ResourceParam final Parameters inParams) {
    // Parse and validate the JSON request.
    final List<ParametersParameterComponent> sourceParams = inParams.getParameter().stream()
        .filter(param -> "source".equals(param.getName())).collect(Collectors.toList());
    if (sourceParams.isEmpty()) {
      throw new InvalidUserInputError("Must provide at least one source parameter");
    }
    log.info("Received $import request");

    // For each input within the request, read the resources of the declared type and create
    // the corresponding table in the warehouse.
    for (final ParametersParameterComponent sourceParam : sourceParams) {
      final ParametersParameterComponent resourceTypeParam = sourceParam.getPart().stream()
          .filter(param -> "resourceType".equals(param.getName()))
          .findFirst()
          .orElseThrow(
              () -> new InvalidUserInputError("Must provide resourceType for each source"));
      final ParametersParameterComponent urlParam = sourceParam.getPart().stream()
          .filter(param -> "url".equals(param.getName()))
          .findFirst()
          .orElseThrow(
              () -> new InvalidUserInputError("Must provide url for each source"));

      final String resourceCode = ((CodeType) resourceTypeParam.getValue()).getCode();
      final ResourceType resourceType = ResourceType.fromCode(resourceCode);
      final FhirContextFactory localFhirContextFactory = this.fhirContextFactory;

      final ExpressionEncoder<IBaseResource> fhirEncoder;
      try {
        fhirEncoder = fhirEncoders.of(resourceType.toCode());
      } catch (final UnsupportedResourceError e) {
        throw new InvalidUserInputError("Unsupported resource type: " + resourceCode);
      }

      String url = ((UrlType) urlParam.getValue()).getValueAsString();
      url = URLDecoder.decode(url, StandardCharsets.UTF_8);
      url = PersistenceScheme.convertS3ToS3aUrl(url);
      final Dataset<String> jsonStrings;
      try {
        accessRules.checkCanImportFrom(url);
        final FilterFunction<String> nonBlanks = s -> !s.isBlank();
        jsonStrings = spark.read().textFile(url).filter(nonBlanks);
      } catch (final SecurityError e) {
        throw new InvalidUserInputError("Not allowed to import from URL: " + url, e);
      } catch (final Exception e) {
        throw new InvalidUserInputError("Error reading from URL: " + url, e);
      }
      final Dataset<IBaseResource> resources = jsonStrings
          .map((MapFunction<String, IBaseResource>) json -> {
            final IBaseResource resource = localFhirContextFactory
                .build().newJsonParser().parseResource(json);
            resource.setId(UUID.randomUUID().toString());
            return resource;
          }, fhirEncoder);

      log.info("Saving resources: {}", resourceType.toCode());
      resourceWriter.write(resourceType, resources.toDF());
    }

    // Update the list of available resources within the resource reader.
    log.info("Updating available resource types");
    resourceReader.updateAvailableResourceTypes();

    // We return 200, as this operation is currently synchronous.
    log.info("Import complete");

    // Invalidate all caches following the import.
    cacheInvalidator.ifPresent(CacheInvalidator::invalidateAll);

    // Construct a response.
    final OperationOutcome opOutcome = new OperationOutcome();
    final OperationOutcomeIssueComponent issue = new OperationOutcomeIssueComponent();
    issue.setSeverity(IssueSeverity.INFORMATION);
    issue.setCode(IssueType.INFORMATIONAL);
    issue.setDiagnostics("Data import completed successfully");
    opOutcome.getIssue().add(issue);
    return opOutcome;
  }
}
