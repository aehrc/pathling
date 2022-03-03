/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.update;

import static au.csiro.pathling.utilities.Preconditions.checkUserInput;

import au.csiro.pathling.caching.CacheInvalidator;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.encoders.UnsupportedResourceError;
import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.errors.SecurityError;
import au.csiro.pathling.fhir.FhirContextFactory;
import au.csiro.pathling.io.AccessRules;
import au.csiro.pathling.io.PersistenceScheme;
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
import org.hl7.fhir.r4.model.BooleanType;
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
  private final ResourceWriter resourceWriter;

  @Nonnull
  private final FhirEncoders fhirEncoders;

  @Nonnull
  private final FhirContextFactory fhirContextFactory;

  @Nonnull
  private final Optional<CacheInvalidator> cacheInvalidator;

  @Nonnull
  private final AccessRules accessRules;

  public static final String GENERATE_IDS_PARAM = "generateIDs";

  /**
   * @param spark A {@link SparkSession} for resolving Spark queries
   * @param resourceWriter A {@link ResourceWriter} for saving resources
   * @param fhirEncoders A {@link FhirEncoders} object for converting data back into HAPI FHIR
   * @param fhirContextFactory A {@link FhirContextFactory} for constructing FhirContext objects in
   * the context of parallel processing
   * @param cacheInvalidator A {@link CacheInvalidator} for invalidating caches upon import
   * @param accessRules A {@link AccessRules} for validating access to URLs
   */
  public ImportExecutor(@Nonnull final SparkSession spark,
      @Nonnull final ResourceWriter resourceWriter,
      @Nonnull final FhirEncoders fhirEncoders,
      @Nonnull final FhirContextFactory fhirContextFactory,
      @Nonnull final Optional<CacheInvalidator> cacheInvalidator,
      @Nonnull final AccessRules accessRules) {
    this.spark = spark;
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
    // The generateIDs parameter defaults to false.
    final boolean generateIds = inParams.getParameter().stream()
        .anyMatch(param -> GENERATE_IDS_PARAM.equals(param.getName()) &&
            param.getValue() instanceof BooleanType &&
            ((BooleanType) param.getValue()).booleanValue());
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

      // Get an encoder based on the declared resource type within the source parameter.
      final ExpressionEncoder<IBaseResource> fhirEncoder;
      try {
        fhirEncoder = fhirEncoders.of(resourceType.toCode());
      } catch (final UnsupportedResourceError e) {
        throw new InvalidUserInputError("Unsupported resource type: " + resourceCode);
      }

      // Check that the user is authorized to execute the operation.
      final Dataset<String> jsonStrings = checkAuthorization(urlParam);

      // Parse each line into a HAPI FHIR object, then encode to a Spark dataset.
      final Dataset<IBaseResource> resources = jsonStrings
          .map(jsonToResourceConverter(generateIds), fhirEncoder);

      log.info("Saving resources: {}", resourceType.toCode());
      resourceWriter.write(resourceType, resources.toDF());
    }

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

  @Nonnull
  private Dataset<String> checkAuthorization(@Nonnull final ParametersParameterComponent urlParam) {
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
    return jsonStrings;
  }

  @Nonnull
  private MapFunction<String, IBaseResource> jsonToResourceConverter(final boolean generateIds) {
    final FhirContextFactory localFhirContextFactory = this.fhirContextFactory;
    return (json) -> {
      final IBaseResource resource = localFhirContextFactory
          .build().newJsonParser().parseResource(json);
      if (generateIds) {
        // If generateIDs is set to true, the ID of each resource will be overwritten with a 
        // new random UUID.
        resource.setId(UUID.randomUUID().toString());
      } else {
        // If generateIDs is not set (which is the default), we check that the ID has been set 
        // on each resource. Query behaviour does not function correctly without an ID 
        // present.
        checkUserInput(resource.getIdElement() != null,
            "All resources must have an ID unless the generateIDs parameter is set");
      }
      return resource;
    };
  }

}
