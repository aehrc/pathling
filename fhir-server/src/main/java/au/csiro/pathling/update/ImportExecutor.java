/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.update;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhir.AnalyticsServerConfiguration;
import au.csiro.pathling.fhir.FhirContextFactory;
import au.csiro.pathling.query.ResourceReader;
import au.csiro.pathling.utilities.PersistenceScheme;
import ca.uhn.fhir.rest.annotation.ResourceParam;
import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encapsulates the execution of an import operation.
 *
 * @author John Grimes
 */
public class ImportExecutor {

  private static final Logger logger = LoggerFactory.getLogger(ImportExecutor.class);
  private final SparkSession spark;
  private final ResourceWriter resourceWriter;
  private final FhirEncoders fhirEncoders;
  private final FhirContextFactory fhirContextFactory;
  private final ResourceReader resourceReader;

  public ImportExecutor(AnalyticsServerConfiguration configuration, SparkSession spark,
      FhirEncoders fhirEncoders, FhirContextFactory fhirContextFactory,
      ResourceReader resourceReader) {
    this.spark = spark;
    this.resourceWriter = new ResourceWriter(configuration.getWarehouseUrl(),
        configuration.getDatabaseName());
    this.fhirEncoders = fhirEncoders;
    this.fhirContextFactory = fhirContextFactory;
    this.resourceReader = resourceReader;
  }

  public OperationOutcome execute(@ResourceParam Parameters inParams)
      throws IOException, URISyntaxException {
    // Parse and validate the JSON request.
    List<ParametersParameterComponent> sourceParams = inParams.getParameter().stream()
        .filter(param -> param.getName().equals("source")).collect(Collectors.toList());
    if (sourceParams.isEmpty()) {
      throw new InvalidRequestException("Must provide at least one source parameter");
    }
    logger.info("Received $import request");

    // For each input within the request, read the resources of the declared type and create
    // the corresponding table in the warehouse.
    for (ParametersParameterComponent sourceParam : sourceParams) {
      ParametersParameterComponent resourceTypeParam = sourceParam.getPart().stream()
          .filter(param -> param.getName().equals("resourceType"))
          .findFirst()
          .orElseThrow(
              () -> new InvalidRequestException("Must provide resourceType for each source"));
      ParametersParameterComponent urlParam = sourceParam.getPart().stream()
          .filter(param -> param.getName().equals("url"))
          .findFirst()
          .orElseThrow(
              () -> new InvalidRequestException("Must provide url for each source"));

      String resourceCode = ((CodeType) resourceTypeParam.getValue()).getCode();
      ResourceType resourceType = ResourceType
          .fromCode(resourceCode);
      ExpressionEncoder<IBaseResource> fhirEncoder = fhirEncoders.of(resourceType.toCode());
      @SuppressWarnings("UnnecessaryLocalVariable") FhirContextFactory localFhirContextFactory = this.fhirContextFactory;

      String url = ((UrlType) urlParam.getValue()).getValueAsString();
      url = PersistenceScheme.convertS3ToS3aUrl(url);
      Dataset<String> jsonStrings;
      try {
        jsonStrings = spark.read().textFile(url);
      } catch (Exception e) {
        throw new InvalidRequestException(e.getMessage());
      }
      Dataset resources = jsonStrings
          .map((MapFunction<String, IBaseResource>) json -> localFhirContextFactory
              .build().newJsonParser().parseResource(json), fhirEncoder);

      logger.info("Saving resources: " + resourceType.toCode());
      resourceWriter.write(resourceType, resources);
    }

    // Update the list of available resources within the resource reader.
    logger.info("Updating available resource types");
    resourceReader.updateAvailableResourceTypes();

    // We return 200, as this operation is currently synchronous.
    logger.info("Import complete");
    OperationOutcome opOutcome = new OperationOutcome();
    OperationOutcomeIssueComponent issue = new OperationOutcomeIssueComponent();
    issue.setSeverity(IssueSeverity.INFORMATION);
    issue.setCode(IssueType.INFORMATIONAL);
    issue.setDiagnostics("Data import completed successfully");
    opOutcome.getIssue().add(issue);
    return opOutcome;
  }
}
